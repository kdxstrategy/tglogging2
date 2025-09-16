import io
import time
import asyncio
import nest_asyncio
import weakref
import threading
from logging import StreamHandler
from typing import Optional, Dict, List
from aiohttp import ClientSession, FormData, ClientTimeout, ClientError

# allow nested loops in interactive/debug environments
nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    """
    Telegram log handler:
      - per-topic buffers, per-topic message_id and last_sent
      - background event loop + shared aiohttp session
      - automatic periodic flusher (sends without external trigger)
      - non-blocking for sync code (emit only enqueues + schedules)
      - requeues on timeout/errors (no logs lost)
    """

    # shared class-level bg loop, thread, session and registry
    _handlers = weakref.WeakSet()
    _bg_loop: Optional[asyncio.AbstractEventLoop] = None
    _bg_thread: Optional[threading.Thread] = None
    _session: Optional[ClientSession] = None
    _session_lock = threading.Lock()

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: Optional[int] = None,
        update_interval: int = 5,
        minimum_lines: int = 1,
        pending_logs: int = 200000,
        request_timeout: int = 10,
    ):
        super().__init__()
        # runtime / config
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.default_topic = int(topic_id) if topic_id is not None else 0
        self.update_interval = float(update_interval)
        self.minimum = int(minimum_lines)
        self.pending = int(pending_logs)
        self.request_timeout = int(request_timeout)

        # per-topic buffers & states
        # keys: topic id (int)
        self._buffers: Dict[int, List[str]] = {}
        self._message_ids: Dict[int, int] = {}
        self._last_sent: Dict[int, str] = {}
        self._lines: Dict[int, int] = {}
        self._last_update: Dict[int, float] = {}

        # other
        self.floodwait = 0
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.initialized = False

        # thread lock for buffers
        self._lock = threading.Lock()

        # register
        TelegramLogHandler._handlers.add(self)

        # ensure bg loop + session + flusher running
        self._ensure_bg_loop_running()
        # schedule session creation & flusher in bg loop
        self._schedule_bg_task(self._bg_init())

    # ---------------------------
    # Background loop & session
    # ---------------------------
    @classmethod
    def _start_bg_loop(cls):
        if cls._bg_loop is not None and cls._bg_loop.is_running():
            return

        def thread_target():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            cls._bg_loop = loop
            loop.run_forever()

        t = threading.Thread(target=thread_target, name="tglog-bg-loop", daemon=True)
        t.start()
        cls._bg_thread = t
        # wait until bg_loop is set
        while cls._bg_loop is None:
            time.sleep(0.01)

    def _ensure_bg_loop_running(self):
        if TelegramLogHandler._bg_loop is None or not TelegramLogHandler._bg_loop.is_running():
            TelegramLogHandler._start_bg_loop()

    @classmethod
    async def _ensure_session(cls, timeout_seconds: int):
        """Ensure shared aiohttp session exists (must be called inside bg loop or app loop)."""
        if cls._session is None or cls._session.closed:
            cls._session = ClientSession(timeout=ClientTimeout(total=timeout_seconds))

    async def _bg_init(self):
        """Create session and start flusher coroutine on bg loop."""
        # ensure session exists on this loop
        await TelegramLogHandler._ensure_session(self.request_timeout)
        # start periodic flusher if not already scheduled
        # We schedule a single flusher task on the bg loop (runs forever)
        # Use attribute on class to avoid duplicate tasks per handler
        if not hasattr(TelegramLogHandler, "_flusher_task"):
            TelegramLogHandler._flusher_task = asyncio.create_task(self._flusher_loop())

    # ---------------------------
    # Scheduling helper
    # ---------------------------
    def _schedule_bg_task(self, coro: asyncio.coroutines):
        """Run a coroutine in the bg loop (non-blocking)."""
        self._ensure_bg_loop_running()
        assert TelegramLogHandler._bg_loop is not None
        return asyncio.run_coroutine_threadsafe(coro, TelegramLogHandler._bg_loop)

    def _maybe_create_task_in_running_loop(self, coro: asyncio.coroutines):
        """
        If current thread has running loop, use create_task there (async apps).
        Otherwise schedule in bg loop via run_coroutine_threadsafe (sync apps).
        """
        try:
            loop = asyncio.get_running_loop()
            # if loop running in this thread, schedule there
            if loop.is_running():
                return loop.create_task(coro)
        except RuntimeError:
            pass
        # otherwise use bg loop
        return self._schedule_bg_task(coro)

    # ---------------------------
    # Emit -> enqueue only (non-blocking)
    # ---------------------------
    def emit(self, record):
        try:
            msg = self.format(record)
        except Exception:
            # avoid raising during emit
            return

        topic = self.default_topic
        now = time.time()

        with self._lock:
            buf = self._buffers.setdefault(topic, [])
            buf.append(msg)
            self._lines[topic] = self._lines.get(topic, 0) + 1
            self._last_update.setdefault(topic, now)
            buffer_text = "\n".join(buf)
            buffer_len = len(buffer_text)

        # If buffer big — schedule immediate send
        if buffer_len >= 3000 and not self.floodwait:
            self._maybe_create_task_in_running_loop(self._handle_topic_send(topic, force_send=True))
            with self._lock:
                self._lines[topic] = 0
                self._last_update[topic] = now
            # also check other handlers
            self._schedule_check_other_handlers()
            return

        # If enough lines — send
        if self._lines.get(topic, 0) >= self.minimum:
            self._maybe_create_task_in_running_loop(self._handle_topic_send(topic))
            with self._lock:
                self._lines[topic] = 0
                self._last_update[topic] = now
            self._schedule_check_other_handlers()
            return

        # else: nothing immediate; flusher will pick it up later
        self._schedule_check_other_handlers()

    def _schedule_check_other_handlers(self):
        # quickly schedule a check that will flush other handlers if needed
        # run in bg loop (non-blocking)
        self._schedule_bg_task(self._check_all_handlers_once())

    async def _check_all_handlers_once(self):
        now = time.time()
        for handler in list(TelegramLogHandler._handlers):
            try:
                with handler._lock:
                    for topic, buf in list(handler._buffers.items()):
                        if not buf:
                            continue
                        text = "\n".join(buf)
                        length = len(text)
                        last = handler._last_update.get(topic, 0)
                        if length >= 3000 or (now - last >= handler.update_interval and handler._lines.get(topic, 0) >= handler.minimum):
                            # schedule send for that handler/topic
                            asyncio.create_task(handler._handle_topic_send(topic))
                            handler._lines[topic] = 0
                            handler._last_update[topic] = now
            except Exception:
                # ignore handler errors during scheduling
                continue

    # ---------------------------
    # Periodic flusher (runs on bg loop)
    # ---------------------------
    async def _flusher_loop(self):
        while True:
            try:
                await asyncio.sleep( max(0.5, self.update_interval/2) )
                now = time.time()
                # iterate handlers and topics
                for handler in list(TelegramLogHandler._handlers):
                    try:
                        with handler._lock:
                            for topic, buf in list(handler._buffers.items()):
                                if not buf:
                                    continue
                                last = handler._last_update.get(topic, 0)
                                # if buffer aged enough or size big, schedule send
                                if len("\n".join(buf)) >= 3000 or (now - last >= handler.update_interval and handler._lines.get(topic, 0) >= handler.minimum):
                                    asyncio.create_task(handler._handle_topic_send(topic))
                                    handler._lines[topic] = 0
                                    handler._last_update[topic] = now
                    except Exception:
                        continue
            except asyncio.CancelledError:
                break
            except Exception:
                # swallow exceptions to keep flusher running
                await asyncio.sleep(1)

    # ---------------------------
    # Core topic send logic
    # ---------------------------
    async def _handle_topic_send(self, topic: int, force_send: bool = False):
        """
        Sends buffered logs for the given topic.
        If sending fails, requeues data to buffer for later retries.
        Runs in an event loop (bg loop or app loop).
        """
        # snapshot and clear buffer atomically
        with self._lock:
            buf = self._buffers.get(topic, [])
            if not buf:
                return
            full_message = "\n".join(buf)
            # clear buffer now; on failure we'll push back
            self._buffers[topic] = []
            self._lines[topic] = 0

        if not full_message.strip():
            return

        # ensure session available in this loop
        try:
            await TelegramLogHandler._ensure_session(self.request_timeout)
        except Exception:
            # session creation failure -> requeue and return
            with self._lock:
                self._buffers.setdefault(topic, []).insert(0, full_message)
                self._lines[topic] = self._lines.get(topic, 0) + 1
            return

        # initialize bot if needed (verify)
        if not self.initialized:
            ok = await self._verify_bot_once()
            if not ok:
                # restore
                with self._lock:
                    self._buffers.setdefault(topic, []).insert(0, full_message)
                    self._lines[topic] = self._lines.get(topic, 0) + 1
                return

        # try to append to existing message if possible
        last_text = self._last_sent.get(topic, "")
        mid = self._message_ids.get(topic)
        try:
            if mid and len(last_text + "\n" + full_message) <= 4096:
                ok = await self._edit_message_request(topic, mid, last_text + "\n" + full_message)
                if ok:
                    self._last_sent[topic] = last_text + "\n" + full_message
                    return
                # else fall through to send new chunks
        except Exception:
            # on unexpected error, requeue and return
            with self._lock:
                self._buffers.setdefault(topic, []).insert(0, full_message)
                self._lines[topic] = self._lines.get(topic, 0) + 1
            return

        # send as new messages split into chunks
        chunks = self._split_into_chunks(full_message)
        for idx, chunk in enumerate(chunks):
            if not chunk.strip():
                continue
            ok = await self._send_message_request(topic, chunk)
            if not ok:
                # requeue remaining (including this chunk)
                with self._lock:
                    # push current chunk and remaining chunks to front
                    self._buffers.setdefault(topic, []).insert(0, chunk)
                    for rem in reversed(chunks[idx+1:]):
                        self._buffers[topic].insert(0, rem)
                    self._lines[topic] = self._lines.get(topic, 0) + 1
                return

    # ---------------------------
    # HTTP helpers (shared session)
    # ---------------------------
    async def _send_request(self, url: str, *, json_payload=None, data=None, params=None):
        sess = TelegramLogHandler._session
        if sess is None:
            # try to ensure session on this loop
            try:
                await TelegramLogHandler._ensure_session(self.request_timeout)
                sess = TelegramLogHandler._session
            except Exception:
                return False, {}

        try:
            if json_payload is not None:
                async with sess.post(url, json=json_payload) as resp:
                    res = await resp.json()
                    return res.get("ok", False), res
            else:
                async with sess.post(url, data=data, params=params) as resp:
                    res = await resp.json()
                    return res.get("ok", False), res
        except asyncio.TimeoutError:
            return False, {"ok": False, "description": "timeout"}
        except ClientError as e:
            return False, {"ok": False, "description": str(e)}
        except Exception as e:
            return False, {"ok": False, "description": str(e)}

    async def _send_message_request(self, topic: int, message: str) -> bool:
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["text"] = f"```k-server\n{message}```"
        if topic:
            payload["message_thread_id"] = topic
        ok, res = await self._send_request(f"{self.base_url}/sendMessage", json_payload=payload)
        if ok:
            mid = res["result"]["message_id"]
            self._message_ids[topic] = mid
            self._last_sent[topic] = message
            return True
        await self._handle_error(topic, res)
        return False

    async def _edit_message_request(self, topic: int, message_id: int, message: str) -> bool:
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["message_id"] = message_id
        payload["text"] = f"```k-server\n{message}```"
        if topic:
            payload["message_thread_id"] = topic
        ok, res = await self._send_request(f"{self.base_url}/editMessageText", json_payload=payload)
        if ok:
            self._last_sent[topic] = message
            return True
        await self._handle_error(topic, res)
        return False

    # ---------------------------
    # Verify / initialize
    # ---------------------------
    async def _verify_bot_once(self) -> bool:
        ok, res = await self._send_request(f"{self.base_url}/getMe", json_payload={})
        if not ok:
            return False
        # mark initialized
        self.initialized = True
        return True

    # ---------------------------
    # Error handling
    # ---------------------------
    async def _handle_error(self, topic: int, resp: dict):
        params = resp.get("parameters", {}) or {}
        code = resp.get("error_code")
        desc = resp.get("description", "") or ""
        if desc == "message thread not found":
            self._message_ids.pop(topic, None)
            self._last_sent.pop(topic, None)
            self.initialized = False
            return
        if code == 429:
            retry_after = int(params.get("retry_after", 30))
            self.floodwait = retry_after
            return
        if "message to edit not found" in desc:
            self._message_ids.pop(topic, None)
            self._last_sent.pop(topic, None)
            self.initialized = False
            return
        # network timeouts etc. will be indicated by description; do nothing else

    # ---------------------------
    # Chunking utility
    # ---------------------------
    def _split_into_chunks(self, message: str) -> List[str]:
        chunks: List[str] = []
        current = ""
        for line in message.split("\n"):
            if line == "":
                line = " "
            while len(line) > 4096:
                split_pos = line[:4096].rfind(" ")
                if split_pos <= 0:
                    split_pos = 4096
                part = line[:split_pos]
                line = line[split_pos:].lstrip()
                if current:
                    current += "\n" + part
                else:
                    current = part
                if len(current) >= 4096:
                    chunks.append(current)
                    current = ""
            if len(current) + len(line) + 1 <= 4096:
                current = (current + "\n" + line) if current else line
            else:
                if current:
                    chunks.append(current)
                current = line
        if current:
            chunks.append(current)
        return chunks

    # ---------------------------
    # Helpers: flush and close
    # ---------------------------
    def flush_all(self):
        """Schedule flush of all handlers (non-blocking)."""
        self._schedule_bg_task(self._check_all_handlers_once())

    @classmethod
    def close(cls):
        """Close bg session and stop flusher (best-effort)."""
        if cls._bg_loop is None:
            return

        async def _close_all():
            # cancel flusher if present
            task = getattr(TelegramLogHandler, "_flusher_task", None)
            if task:
                task.cancel()
                try:
                    await task
                except Exception:
                    pass
                TelegramLogHandler._flusher_task = None
            if cls._session and not cls._session.closed:
                await cls._session.close()
                cls._session = None

        fut = asyncio.run_coroutine_threadsafe(_close_all(), cls._bg_loop)
        try:
            fut.result(timeout=5)
        except Exception:
            pass

# end of class
