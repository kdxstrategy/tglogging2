import io
import time
import asyncio
import nest_asyncio
import weakref
import threading
from logging import StreamHandler
from typing import Optional, Dict, List, Tuple
from aiohttp import ClientSession, ClientTimeout, ClientError, FormData

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    """
    TelegramLogHandler:
      - per-handler per-topic buffers
      - background event loop + shared aiohttp session
      - periodic flusher sending accumulated chunks without external trigger
      - non-blocking for synchronous code
      - requeues on network/timeouts so logs aren't lost
      - edits last message when possible (doesn't overwrite older messages)
    """

    # class-level shared resources
    _handlers = weakref.WeakSet()
    _bg_loop: Optional[asyncio.AbstractEventLoop] = None
    _bg_thread: Optional[threading.Thread] = None
    _session: Optional[ClientSession] = None
    _flusher_task_name = "_tglog_flusher_task"

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: Optional[int] = None,
        update_interval: float = 5.0,
        minimum_lines: int = 1,
        pending_logs: int = 200000,
        request_timeout: int = 10,
    ):
        super().__init__()
        # config
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id is not None else None
        self.update_interval = float(update_interval)
        self.minimum = int(minimum_lines)
        self.pending = int(pending_logs)
        self.request_timeout = int(request_timeout)

        # per-handler state
        # buffer is list of strings representing lines to send for this handler/topic
        self._buffer: List[str] = []
        # last sent message id and content (we only edit the last message)
        self._last_message: Optional[Tuple[int, str]] = None
        # floodwait seconds if set by Telegram
        self.floodwait = 0
        # bookkeeping
        self._lines = 0
        self._last_update = 0.0
        self._initialized = False

        # internal lock for thread-safety between emit() and bg loop
        self._lock = threading.Lock()

        # base url for this bot
        self._base_url = f"https://api.telegram.org/bot{self.token}"

        # register handler globally
        TelegramLogHandler._handlers.add(self)

        # ensure background loop + session + flusher are running
        self._ensure_bg_loop()
        # schedule session creation + flusher start on bg loop
        self._run_in_bg(self._ensure_session_and_flusher())

    # ------------------------
    # background loop helpers
    # ------------------------
    @classmethod
    def _start_bg_thread(cls):
        if cls._bg_loop is not None and cls._bg_loop.is_running():
            return

        def _thread_main():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            cls._bg_loop = loop
            loop.run_forever()

        t = threading.Thread(target=_thread_main, name="tglog-bg-loop", daemon=True)
        t.start()
        cls._bg_thread = t
        # wait until loop is available
        while cls._bg_loop is None:
            time.sleep(0.01)

    def _ensure_bg_loop(self):
        if TelegramLogHandler._bg_loop is None or not TelegramLogHandler._bg_loop.is_running():
            TelegramLogHandler._start_bg_thread()

    def _run_in_bg(self, coro: asyncio.coroutines):
        """
        Schedule coroutine on the background loop (non-blocking).
        If current thread has a running loop, prefer creating a task there (for async apps).
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # Running inside an async app: schedule here
            return loop.create_task(coro)
        else:
            assert TelegramLogHandler._bg_loop is not None
            return asyncio.run_coroutine_threadsafe(coro, TelegramLogHandler._bg_loop)

    # ------------------------
    # session + flusher
    # ------------------------
    @classmethod
    async def _ensure_session(cls, timeout_seconds: int):
        if cls._session is None or cls._session.closed:
            cls._session = ClientSession(timeout=ClientTimeout(total=timeout_seconds))

    async def _ensure_session_and_flusher(self):
        # ensure session exists in bg loop
        await TelegramLogHandler._ensure_session(self.request_timeout)
        # start flusher once (on bg loop)
        if not hasattr(TelegramLogHandler, TelegramLogHandler._flusher_task_name):
            # create flusher task bound to bg loop
            task = asyncio.create_task(self._flusher_loop())
            setattr(TelegramLogHandler, TelegramLogHandler._flusher_task_name, task)

    async def _flusher_loop(self):
        """Periodically check all handlers and flush ready buffers."""
        while True:
            try:
                await asyncio.sleep(max(0.5, self.update_interval / 2.0))
                now = time.time()
                # iterate over copy of handlers
                for handler in list(TelegramLogHandler._handlers):
                    try:
                        # decide per-handler per-topic
                        with handler._lock:
                            if not handler._buffer:
                                continue
                            buf_text = "\n".join(handler._buffer)
                            buf_len = len(buf_text)
                            time_diff = now - handler._last_update
                            if buf_len >= 3000 or (time_diff >= max(handler.update_interval, handler.floodwait) and handler._lines >= handler.minimum):
                                # schedule immediate send for that handler
                                asyncio.create_task(handler._handle_send())  # runs in this loop
                                handler._lines = 0
                                handler._last_update = now
                    except Exception:
                        # swallow individual handler errors to keep flusher alive
                        continue
            except asyncio.CancelledError:
                break
            except Exception:
                # swallow and wait a bit, keep flusher alive
                await asyncio.sleep(1)

    # ------------------------
    # emit (non-blocking)
    # ------------------------
    def emit(self, record):
        """Non-blocking: just append to buffer and schedule send checks."""
        try:
            msg = self.format(record)
        except Exception:
            # avoid raising from emit
            return

        now = time.time()
        with self._lock:
            self._buffer.append(msg)
            self._lines += 1
            self._last_update = self._last_update or now

        # If large buffer or enough lines, schedule send in appropriate loop
        # If running inside an async app, schedule there; otherwise schedule in bg loop.
        # Also schedule checks for other handlers so they don't pile up.
        if len("\n".join(self._buffer)) >= 3000 and not self.floodwait:
            # immediate attempt
            self._maybe_schedule_send(self._handle_send(force_send=True))
            with self._lock:
                self._lines = 0
                self._last_update = now
            # also check other handlers
            self._run_in_bg(self._check_other_handlers_once())
            return

        if self._lines >= self.minimum and (time.time() - self._last_update) >= 0:
            self._maybe_schedule_send(self._handle_send())
            with self._lock:
                self._lines = 0
                self._last_update = now
            self._run_in_bg(self._check_other_handlers_once())
            return

        # schedule a lightweight check for other handlers (non-blocking)
        self._run_in_bg(self._check_other_handlers_once())

    def _maybe_schedule_send(self, coro):
        """Schedule coro on running loop if present, otherwise on bg loop."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            loop.create_task(coro)
        else:
            self._run_in_bg(coro)

    async def _check_other_handlers_once(self):
        """Inspect all handlers and schedule sends where needed (called in bg loop)."""
        now = time.time()
        for handler in list(TelegramLogHandler._handlers):
            try:
                with handler._lock:
                    if not handler._buffer:
                        continue
                    buf_text = "\n".join(handler._buffer)
                    buf_len = len(buf_text)
                    last = handler._last_update or 0
                    if buf_len >= 3000 or (now - last >= handler.update_interval and handler._lines >= handler.minimum):
                        asyncio.create_task(handler._handle_send())
                        handler._lines = 0
                        handler._last_update = now
            except Exception:
                continue

    # ------------------------
    # core send logic
    # ------------------------
    async def _handle_send(self, force_send: bool = False):
        """
        Send buffered data for this handler/topic.
        On failure (timeout, network error), requeue the chunk(s) back to buffer.
        """
        # quick check
        with self._lock:
            if not self._buffer or self.floodwait:
                return
            full_message = "\n".join(self._buffer)
            # snapshot and clear buffer — we'll requeue on failure
            self._buffer = []
            self._lines = 0

        if not full_message.strip():
            return

        # ensure session exists in current loop
        try:
            await TelegramLogHandler._ensure_session(self.request_timeout)
        except Exception:
            # session not ready: requeue and return
            with self._lock:
                self._buffer.insert(0, full_message)
                self._lines = self._lines + full_message.count("\n") + 1
            return

        # initialize bot (verify) if needed
        if not self._initialized:
            ok = await self._verify_bot_once()
            if not ok:
                # requeue and return
                with self._lock:
                    self._buffer.insert(0, full_message)
                    self._lines = self._lines + full_message.count("\n") + 1
                return

        # try to append to last message if possible
        last_msg = self._last_message  # (message_id, content) or None
        if last_msg:
            last_id, last_content = last_msg
            if len(last_content + "\n" + full_message) <= 4096:
                ok = await self._edit_message(last_id, last_content + "\n" + full_message)
                if ok:
                    # update last content
                    self._last_message = (last_id, last_content + "\n" + full_message)
                    # also try to flush other handlers — schedule check
                    await self._schedule_flush_all()
                    return
                # fallthrough to send as new chunks

        # send as new message(s) chunked
        chunks = self._split_into_chunks(full_message)
        for idx, chunk in enumerate(chunks):
            if not chunk.strip():
                continue
            ok, msg_id = await self._send_message(chunk)
            if not ok:
                # requeue this chunk and remaining ones at front (preserve order)
                with self._lock:
                    # insert remaining in reverse so they come out in correct order
                    for r in reversed(chunks[idx:]):
                        self._buffer.insert(0, r)
                    self._lines = self._lines + full_message.count("\n") + 1
                return
            # success: store last message id/content (only of the latest chunk we sent)
            self._last_message = (msg_id, chunk)

        # after finishing sending for this handler, ensure other handlers are flushed too
        await self._schedule_flush_all()

    async def _schedule_flush_all(self):
        """
        When one handler finishes sending, check all other handlers and schedule sends for them
        (ensures when sending to one topic we also flush others).
        """
        # run check in bg loop to avoid blocking
        await self._check_other_handlers_once()

    # ------------------------
    # HTTP helpers
    # ------------------------
    async def _send_request(self, path: str, json_payload=None, data=None, params=None) -> Tuple[bool, dict]:
        """
        Perform POST request using shared session; returns (ok_flag, response_json_or_empty).
        This must be called from a running loop that has access to TelegramLogHandler._session
        (we ensured session is created on bg loop at startup).
        """
        sess = TelegramLogHandler._session
        if sess is None:
            try:
                await TelegramLogHandler._ensure_session(self.request_timeout)
                sess = TelegramLogHandler._session
            except Exception:
                return False, {}

        url = f"{self._base_url}/{path.lstrip('/')}"
        try:
            if json_payload is not None:
                async with sess.post(url, json=json_payload) as resp:
                    try:
                        res = await resp.json()
                        return res.get("ok", False), res
                    except Exception:
                        return False, {}
            else:
                async with sess.post(url, data=data, params=params) as resp:
                    try:
                        res = await resp.json()
                        return res.get("ok", False), res
                    except Exception:
                        return False, {}
        except asyncio.TimeoutError:
            return False, {"ok": False, "description": "timeout"}
        except ClientError as e:
            return False, {"ok": False, "description": str(e)}
        except Exception as e:
            return False, {"ok": False, "description": str(e)}

    async def _send_message(self, text: str) -> Tuple[bool, Optional[int]]:
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["text"] = f"```k-server\n{text}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id
        ok, res = await self._send_request("sendMessage", json_payload=payload)
        if ok:
            try:
                mid = res["result"]["message_id"]
                return True, mid
            except Exception:
                return False, None
        # handle error (may set floodwait)
        await self._handle_api_error(res)
        return False, None

    async def _edit_message(self, message_id: int, new_text: str) -> bool:
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["message_id"] = message_id
        payload["text"] = f"```k-server\n{new_text}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id
        ok, res = await self._send_request("editMessageText", json_payload=payload)
        if ok:
            return True
        await self._handle_api_error(res)
        return False

    async def _verify_bot_once(self) -> bool:
        ok, res = await self._send_request("getMe", json_payload={})
        if not ok:
            return False
        self._initialized = True
        return True

    async def _handle_api_error(self, res: dict):
        params = res.get("parameters", {}) or {}
        code = res.get("error_code")
        desc = res.get("description", "") or ""
        if code == 429:
            retry_after = int(params.get("retry_after", 30))
            self.floodwait = retry_after
        elif desc == "message thread not found":
            # thread/topic invalid: reset last message state so next send creates new message
            self._last_message = None
            self._initialized = False
        elif "message to edit not found" in desc:
            self._last_message = None
            self._initialized = False
        # other errors: nothing special (timeouts are handled in _send_request)

    # ------------------------
    # chunking utility
    # ------------------------
    def _split_into_chunks(self, message: str) -> List[str]:
        chunks: List[str] = []
        current = ""
        for line in message.split("\n"):
            if line == "":
                line = " "
            # split extremely long lines into segments
            while len(line) > 4096:
                part = line[:4096]
                line = line[4096:]
                if current:
                    chunks.append(current)
                    current = part
                else:
                    chunks.append(part)
            if len(current) + len(line) + (1 if current else 0) <= 4096:
                current = (current + "\n" + line) if current else line
            else:
                if current:
                    chunks.append(current)
                current = line
        if current:
            chunks.append(current)
        return chunks

    # ------------------------
    # public helpers
    # ------------------------
    def flush(self):
        """Request immediate flush of this handler's buffer (non-blocking)."""
        self._maybe_schedule_send(self._handle_send())

    @classmethod
    def flush_all(cls):
        """Non-blocking flush request for all handlers."""
        for handler in list(cls._handlers):
            handler.flush()

    @classmethod
    def close(cls):
        """Close background session and stop flusher (best-effort)."""
        # cancel flusher task
        if cls._bg_loop is None:
            return

        async def _shutdown():
            task = getattr(TelegramLogHandler, TelegramLogHandler._flusher_task_name, None)
            if task:
                task.cancel()
                try:
                    await task
                except Exception:
                    pass
                delattr(TelegramLogHandler, TelegramLogHandler._flusher_task_name)
            if cls._session and not cls._session.closed:
                await cls._session.close()
                cls._session = None

        fut = asyncio.run_coroutine_threadsafe(_shutdown(), TelegramLogHandler._bg_loop)
        try:
            fut.result(timeout=5)
        except Exception:
            pass

# End of class
