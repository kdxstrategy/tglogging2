import io
import time
import asyncio
import nest_asyncio
import weakref
import threading
from logging import StreamHandler
from typing import Optional, Dict, List, Tuple
from aiohttp import ClientSession, FormData, ClientTimeout, ClientError

# allow nested loops in some interactive/debug environments
nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    """
    Handler that sends logs to Telegram. Works in sync code without blocking:
    - per-topic buffers and per-topic message state (message_id, last_sent)
    - background event loop in a thread used when no running loop present
    - retries/queues logs on timeout/errors (logs not lost)
    - when sending for one topic, checks other handlers' queues and schedules them too
    """

    # global registry + background loop/session
    _handlers = weakref.WeakSet()
    _bg_loop: Optional[asyncio.AbstractEventLoop] = None
    _bg_thread: Optional[threading.Thread] = None
    _session: Optional[ClientSession] = None
    _session_lock = threading.Lock()  # protect session creation

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
        # sync/async integration
        self._main_loop = None
        try:
            # may raise if no running loop
            self._main_loop = asyncio.get_running_loop()
        except RuntimeError:
            self._main_loop = None

        # parameters
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.default_topic = int(topic_id) if topic_id else 0
        self.update_interval = float(update_interval)
        self.minimum = int(minimum_lines)
        self.pending = int(pending_logs)
        self.request_timeout = int(request_timeout)

        # buffers and per-topic state (topic -> ...)
        # NOTE: keys are topic ids (int); default topic uses self.default_topic
        self._buffers: Dict[int, List[str]] = {}
        self._message_ids: Dict[int, int] = {}
        self._last_sent: Dict[int, str] = {}
        self._lines: Dict[int, int] = {}
        self._last_update: Dict[int, float] = {}

        # flow control
        self.floodwait = 0

        # api base
        self.base_url = f"https://api.telegram.org/bot{token}"

        # thread-safety for buffer access between threads
        self._lock = threading.Lock()

        # register
        self._handlers.add(self)

        # ensure background loop available for scheduling in sync contexts
        self._ensure_bg_loop()

    # ---------------------------
    # Background loop management
    # ---------------------------
    @classmethod
    def _start_bg_loop(cls):
        if cls._bg_loop is not None:
            return

        def _run():
            loop = asyncio.new_event_loop()
            cls._bg_loop = loop
            asyncio.set_event_loop(loop)
            loop.run_forever()

        t = threading.Thread(target=_run, daemon=True, name="tglog-bg-loop")
        t.start()
        cls._bg_thread = t

        # wait until loop is set
        while cls._bg_loop is None:
            time.sleep(0.01)

    def _ensure_bg_loop(self):
        # start once per class
        if TelegramLogHandler._bg_loop is None:
            TelegramLogHandler._start_bg_loop()

    @classmethod
    async def _ensure_session(cls, timeout_seconds: int):
        # create aiohttp session in the bg loop when first needed
        if cls._session is None or cls._session.closed:
            # protect against races (called inside bg loop)
            cls._session = ClientSession(timeout=ClientTimeout(total=timeout_seconds))

    # ---------------------------
    # Utility for scheduling coroutines
    # ---------------------------
    def _schedule_coro(self, coro: asyncio.coroutines):
        """
        Schedule `coro` to run:
         - if there's a running loop in current thread, use it via create_task (async app)
         - otherwise, submit to background loop via run_coroutine_threadsafe (non-blocking)
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # running in an async app: schedule here
            loop.create_task(coro)
        else:
            # schedule on background loop (non-blocking)
            assert TelegramLogHandler._bg_loop is not None
            asyncio.run_coroutine_threadsafe(coro, TelegramLogHandler._bg_loop)

    # ---------------------------
    # Emit: only enqueues (non-blocking)
    # ---------------------------
    def emit(self, record):
        """
        Called from sync code. Only manipulates buffers and schedules async worker tasks,
        never blocks waiting for network.
        """
        try:
            msg = self.format(record)
        except Exception:
            # be defensive: formatting could fail; avoid raising inside emit
            return

        topic = self.default_topic
        now = time.time()

        # add to buffer thread-safely
        with self._lock:
            buf = self._buffers.setdefault(topic, [])
            buf.append(msg)
            self._lines[topic] = self._lines.get(topic, 0) + 1
            # set last_update if first append
            self._last_update.setdefault(topic, now)
            buffer_text = "\n".join(buf)
            current_length = len(buffer_text)

        # If buffer large, schedule immediate send
        if current_length >= 3000 and not self.floodwait:
            self._schedule_coro(self._handle_topic_send(topic, force_send=True))
            with self._lock:
                self._lines[topic] = 0
                self._last_update[topic] = now
            # also check other handlers' queues and schedule them
            self._schedule_other_handlers_if_needed()

            return

        # If enough lines, schedule immediate send
        if self._lines.get(topic, 0) >= self.minimum:
            self._schedule_coro(self._handle_topic_send(topic))
            with self._lock:
                self._lines[topic] = 0
                self._last_update[topic] = now
            self._schedule_other_handlers_if_needed()
            return

        # Otherwise check timers across handlers (if some other handler has old buffers)
        self._schedule_other_handlers_if_needed()

    def _schedule_other_handlers_if_needed(self):
        """
        Inspect all handlers' buffers; if any buffer is ready (by time, size, or minimum),
        schedule send for them as well. This keeps queues from piling up.
        """
        now = time.time()
        for handler in list(TelegramLogHandler._handlers):
            # skip self: handled by caller
            try:
                with handler._lock:
                    for topic, buf in list(handler._buffers.items()):
                        if not buf:
                            continue
                        buf_text = "\n".join(buf)
                        buffer_len = len(buf_text)
                        last_up = handler._last_update.get(topic, 0)
                        time_diff = now - last_up
                        lines = handler._lines.get(topic, 0)
                        if buffer_len >= 3000 or (time_diff >= handler.update_interval and lines >= handler.minimum):
                            handler._schedule_coro(handler._handle_topic_send(topic))
                            handler._lines[topic] = 0
                            handler._last_update[topic] = now
            except Exception:
                # we don't want to fail emit due to other handler issues
                continue

    # ---------------------------
    # Async send logic
    # ---------------------------
    async def _handle_topic_send(self, topic: int, force_send: bool = False):
        """
        Runs in an event loop (bg-loop or app-loop). Sends buffered logs for specific topic.
        If send fails due to network/timeout, requeue logs into buffer for later retry.
        """
        # take snapshot of buffer atomically
        with self._lock:
            buf = self._buffers.get(topic, [])
            if not buf:
                return
            full_message = "\n".join(buf)
            # clear buffer now; if sending fails we will push back
            self._buffers[topic] = []
            self._lines[topic] = 0

        if not full_message.strip():
            return

        # ensure session exists in this loop
        try:
            # If running in bg loop, ensure session created there.
            await TelegramLogHandler._ensure_session(self.request_timeout)
        except Exception:
            # ensure_session might raise if called from app-loop; ignore and continue
            pass

        # initialize bot if not done
        if not self.initialized:
            ok = await self.initialize_bot()
            if not ok:
                # restore buffer on failure
                with self._lock:
                    # prepend to front to try earlier next time
                    self._buffers.setdefault(topic, []).insert(0, full_message)
                    self._lines[topic] = self._lines.get(topic, 0) + full_message.count("\n") + 1
                return

        # try to append to previous message if possible
        last = self._last_sent.get(topic, "")
        msg_id = self._message_ids.get(topic)
        try:
            if msg_id and len(last + "\n" + full_message) <= 4096:
                combined = last + "\n" + full_message
                ok = await self._edit_message_request(topic, msg_id, combined)
                if ok:
                    self._last_sent[topic] = combined
                    return
                # if editing failed, fallthrough to send as new messages
        except Exception as e:
            # on unexpected errors, requeue and return
            with self._lock:
                self._buffers.setdefault(topic, []).insert(0, full_message)
                self._lines[topic] = self._lines.get(topic, 0) + full_message.count("\n") + 1
            return

        # send as new messages (chunked)
        chunks = self._split_into_chunks(full_message)
        for chunk in chunks:
            if not chunk.strip():
                continue
            ok = await self._send_message_request(topic, chunk)
            if not ok:
                # requeue remainder and exit
                with self._lock:
                    self._buffers.setdefault(topic, []).insert(0, chunk)
                    # also restore any remaining chunks if any
                    remaining = chunks[chunks.index(chunk) + 1 :]
                    for r in reversed(remaining):
                        self._buffers[topic].insert(0, r)
                    self._lines[topic] = self._lines.get(topic, 0) + 1
                return

    # ---------------------------
    # HTTP helpers (use shared session)
    # ---------------------------
    async def _send_request(self, method: str, url: str, *, json_payload=None, data=None, params=None) -> Tuple[bool, dict]:
        """
        Low-level request using shared session. Returns (ok, response-dict-or-empty).
        """
        sess = TelegramLogHandler._session
        if sess is None:
            # try ensure session in-case called in app-loop
            await TelegramLogHandler._ensure_session(self.request_timeout)
            sess = TelegramLogHandler._session
            if sess is None:
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
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            # timeout -> don't block, return false
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

        ok, res = await self._send_request("post", f"{self.base_url}/sendMessage", json_payload=payload)
        if ok:
            # store message id + last_sent
            mid = res["result"]["message_id"]
            self._message_ids[topic] = mid
            self._last_sent[topic] = message
            return True
        else:
            # call handle_error to process floodwait etc
            await self._handle_error(topic, res)
            return False

    async def _edit_message_request(self, topic: int, message_id: int, message: str) -> bool:
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["message_id"] = message_id
        payload["text"] = f"```k-server\n{message}```"
        if topic:
            payload["message_thread_id"] = topic

        ok, res = await self._send_request("post", f"{self.base_url}/editMessageText", json_payload=payload)
        if ok:
            self._last_sent[topic] = message
            return True
        else:
            await self._handle_error(topic, res)
            return False

    # ---------------------------
    # Initialize & verify
    # ---------------------------
    async def initialize_bot(self) -> bool:
        uname, ok = await self.verify_bot()
        return bool(ok)

    async def verify_bot(self) -> Tuple[Optional[str], bool]:
        ok, res = await self._send_request("post", f"{self.base_url}/getMe", json_payload={})
        if not ok:
            # treat as not initialized (caller will requeue)
            return None, False
        return res.get("result", {}).get("username"), True

    # ---------------------------
    # Public send/edit wrappers (used by _handle_topic_send)
    # ---------------------------
    async def _send_message_request_wrapper(self, topic: int, message: str) -> bool:
        return await self._send_message_request(topic, message)

    # ---------------------------
    # Error handling
    # ---------------------------
    async def _handle_error(self, topic: int, resp: dict):
        """
        Handle API errors: floodwait, not found, etc.
        On network/timeouts the resp likely contains description; we requeue earlier.
        """
        params = resp.get("parameters", {}) or {}
        code = resp.get("error_code")
        desc = resp.get("description", "") or ""

        if desc == "message thread not found":
            # topic invalid — reset per-topic state
            self._message_ids.pop(topic, None)
            self._last_sent.pop(topic, None)
            self.initialized = False
            return

        if code == 429:
            retry_after = int(params.get("retry_after", 30))
            self.floodwait = retry_after
            # requeue will be handled by caller by re-inserting chunk into buffer
            return

        if "message to edit not found" in desc:
            self._message_ids.pop(topic, None)
            self._last_sent.pop(topic, None)
            self.initialized = False
            return

        # for timeouts / network errors, desc may indicate timeout; nothing else to do
        return

    # ---------------------------
    # Chunking
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
    # Public helper to flush all handlers (optional)
    # ---------------------------
    @classmethod
    def flush_all(cls):
        """
        Request immediate flush for all handlers: schedule their pending buffers to send.
        Non-blocking.
        """
        for handler in list(cls._handlers):
            # schedule for each topic
            try:
                with handler._lock:
                    for topic, buf in list(handler._buffers.items()):
                        if buf:
                            handler._schedule_coro(handler._handle_topic_send(topic))
            except Exception:
                continue

    # ---------------------------
    # Close (cleanup session) - should be called on program exit if desired
    # ---------------------------
    @classmethod
    def close_bg_session(cls):
        """
        Close aiohttp session running in the background loop.
        This schedules session.close() on bg loop and waits briefly.
        """
        if cls._bg_loop is None:
            return

        async def _close():
            if cls._session and not cls._session.closed:
                await cls._session.close()
                cls._session = None

        fut = asyncio.run_coroutine_threadsafe(_close(), cls._bg_loop)
        try:
            fut.result(timeout=5)
        except Exception:
            pass

# End of class
