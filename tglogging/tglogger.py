import io
import time
import asyncio
import nest_asyncio
import threading
import weakref
from collections import deque
from logging import StreamHandler
from typing import Optional, Tuple, Deque, Dict, List
from aiohttp import ClientSession, ClientTimeout, ClientError, FormData

# Если вы запускаете в средах вроде Jupyter/VSCode, это помогает вписаться в уже существующий loop.
nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    """
    Robust Telegram logging handler:
      - per-handler queue (deque) for messages (separate per topic)
      - background event loop + single shared aiohttp.ClientSession
      - flusher periodically sends accumulated logs for all handlers
      - edits last message when possible; otherwise sends new message chunks
      - on timeout/network error re-inserts failed chunk at left (so it's retried first)
      - works in both sync and async apps without blocking caller
    """

    # registry and background resources (class-level)
    _handlers: "weakref.WeakSet[TelegramLogHandler]" = weakref.WeakSet()
    _bg_loop: Optional[asyncio.AbstractEventLoop] = None
    _bg_thread: Optional[threading.Thread] = None
    _session: Optional[ClientSession] = None
    _flusher_task: Optional[asyncio.Task] = None
    _started_lock = threading.Lock()

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: Optional[int] = None,
        update_interval: float = 5.0,
        minimum_lines: int = 1,
        pending_logs: int = 200000,
        request_timeout: float = 10.0,
    ):
        super().__init__()
        self.token = str(token)
        self.chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id is not None else None
        self.update_interval = float(update_interval)
        self.minimum_lines = int(minimum_lines)
        self.pending_logs = int(pending_logs)
        self.request_timeout = float(request_timeout)

        # per-handler state
        self._queue: Deque[str] = deque(maxlen=self.pending_logs)
        # last sent message: (message_id, content)
        self._last_message: Optional[Tuple[int, str]] = None
        # floodwait seconds reported by telegram
        self._floodwait_until: float = 0.0

        # bookkeeping
        self._last_flush_ts: float = 0.0
        self._lines_since_update: int = 0
        self._initialized: bool = False

        # register
        TelegramLogHandler._handlers.add(self)

        # ensure bg loop + session + flusher running
        self._ensure_background()

    # ---------------------------
    # public: emit (non-blocking)
    # ---------------------------
    def emit(self, record):
        """
        Called from logging module; must be non-blocking.
        We append to local deque and schedule a flush check.
        """
        try:
            msg = self.format(record)
        except Exception:
            return

        # push to queue
        self._queue.append(msg)
        self._lines_since_update += 1

        # schedule flush check quickly (non-blocking)
        # If we're inside an async app, schedule there; otherwise schedule on bg loop
        try:
            running = asyncio.get_running_loop().is_running()
        except RuntimeError:
            running = False

        coro = self._schedule_flush_for_all()  # check and flush when appropriate
        if running:
            # schedule in current loop
            try:
                asyncio.get_running_loop().create_task(coro)
            except Exception:
                # fallback: schedule in bg loop
                self._run_in_bg(coro)
        else:
            self._run_in_bg(coro)

    # ---------------------------
    # background loop management
    # ---------------------------
    @classmethod
    def _start_bg_loop(cls):
        """Start a dedicated background loop in a thread (once)."""
        with cls._started_lock:
            if cls._bg_loop and cls._bg_loop.is_running():
                return

            def _run():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                cls._bg_loop = loop
                loop.run_forever()

            t = threading.Thread(target=_run, name="tglog-bg-loop", daemon=True)
            t.start()
            cls._bg_thread = t

            # wait until loop is set
            while cls._bg_loop is None:
                time.sleep(0.01)

    def _run_in_bg(self, coro: asyncio.coroutines):
        """Schedule coroutine in background loop thread (non-blocking)."""
        if TelegramLogHandler._bg_loop is None or not TelegramLogHandler._bg_loop.is_running():
            TelegramLogHandler._start_bg_loop()
        return asyncio.run_coroutine_threadsafe(coro, TelegramLogHandler._bg_loop)

    def _ensure_background(self):
        """Ensure background loop, session and flusher are started."""
        # start bg loop if not present
        if TelegramLogHandler._bg_loop is None or not TelegramLogHandler._bg_loop.is_running():
            TelegramLogHandler._start_bg_loop()

        # schedule session + flusher creation on bg loop
        fut = asyncio.run_coroutine_threadsafe(self._ensure_session_and_flusher(), TelegramLogHandler._bg_loop)
        try:
            # wait very briefly for setup, but don't block long (non-blocking pattern)
            fut.result(timeout=1.0)
        except Exception:
            # ignore — background will finish setup
            pass

    @classmethod
    async def _ensure_session_and_flusher(cls):
        """Create shared aiohttp session and flusher task (runs on bg loop)."""
        if cls._session is None or cls._session.closed:
            cls._session = ClientSession(timeout=ClientTimeout(total=cls._default_timeout()))
        if cls._flusher_task is None or cls._flusher_task.done():
            # start flusher (repeats checking all handlers)
            cls._flusher_task = asyncio.create_task(cls._flusher_loop())

    @classmethod
    def _default_timeout(cls) -> float:
        # reasonable default; handler instances may use different request_timeout but session created once
        return 10.0

    @classmethod
    async def _flusher_loop(cls):
        """Background loop that periodically checks all handlers and flushes ready ones."""
        try:
            while True:
                # small sleep to avoid busy looping
                await asyncio.sleep(0.8)
                now = time.time()
                tasks = []
                for h in list(cls._handlers):
                    # if floodwait active skip
                    if h._floodwait_until > now:
                        continue
                    # if queue empty continue
                    if not h._queue:
                        continue
                    # decide whether to flush: size or time or minimum lines
                    buf_text = "\n".join(h._queue)
                    if len(buf_text) >= 3000:
                        tasks.append(h._handle_send())
                        continue
                    time_diff = now - (h._last_flush_ts or 0)
                    if time_diff >= max(h.update_interval, 0.1) and h._lines_since_update >= h.minimum_lines:
                        tasks.append(h._handle_send())
                        continue
                if tasks:
                    # run them concurrently but swallow exceptions to keep loop alive
                    await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        except Exception:
            # keep going on unexpected errors
            await asyncio.sleep(1)
            return await cls._flusher_loop()

    # ---------------------------
    # sending / retry logic
    # ---------------------------
    async def _schedule_flush_for_all(self):
        """
        Called from emit to prompt an immediate check/flush of all handlers.
        Runs either in current loop or bg loop depending on call site.
        """
        now = time.time()
        tasks = []
        for h in list(TelegramLogHandler._handlers):
            if h._floodwait_until > now:
                continue
            # quick conditions: size or age or min lines
            if not h._queue:
                continue
            buf_text = "\n".join(h._queue)
            if len(buf_text) >= 3000:
                tasks.append(h._handle_send())
            else:
                time_diff = now - (h._last_flush_ts or 0)
                if time_diff >= max(h.update_interval, 0.1) and h._lines_since_update >= h.minimum_lines:
                    tasks.append(h._handle_send())
        if tasks:
            # schedule on the current loop if running, else schedule in bg
            try:
                running = asyncio.get_running_loop().is_running()
            except RuntimeError:
                running = False

            if running:
                # schedule concurrently here
                await asyncio.gather(*tasks, return_exceptions=True)
            else:
                # schedule on bg loop
                await asyncio.gather(*[asyncio.ensure_future(t) for t in tasks], return_exceptions=True)

    async def _handle_send(self):
        """
        Send accumulated logs for *this* handler. On failure (timeout/network), reinsert failed chunk(s)
        at the left of the deque so they are retried first.
        """
        # pop snapshot from queue (we'll requeue on failure)
        if not self._queue:
            return
        now = time.time()
        if self._floodwait_until > now:
            return

        # build as much as possible (but not exceed 3000 for initial combined)
        assembled: List[str] = []
        agg_len = 0
        while self._queue and agg_len < 3000:
            item = self._queue.popleft()
            assembled.append(item)
            agg_len = len("\n".join(assembled))

        text = "\n".join(assembled).strip()
        if not text:
            return

        # make sure session exists
        if TelegramLogHandler._session is None or TelegramLogHandler._session.closed:
            await TelegramLogHandler._ensure_session_and_flusher()

        # ensure bot initialized
        if not self._initialized:
            ok = await self._verify_bot()
            if not ok:
                # requeue everything at front
                for part in reversed(assembled):
                    self._queue.appendleft(part)
                return

        # try to append to last message if possible
        if self._last_message:
            last_id, last_content = self._last_message
            combined = last_content + "\n" + text
            if len(combined) <= 4096:
                ok = await self._edit_message(last_id, combined)
                if ok:
                    self._last_message = (last_id, combined)
                    # after sending, also try flush other handlers
                    await self._flush_other_handlers()
                    self._lines_since_update = 0
                    self._last_flush_ts = time.time()
                    return
                # if edit failed, we'll try to send as new messages (fallthrough)

        # split into chunks and send sequentially
        chunks = self._split_into_telegram_chunks(text)
        for i, chunk in enumerate(chunks):
            ok, mid_or_err = await self._send_message_chunk(chunk)
            if not ok:
                # network or API failure: put this chunk and remaining back to left of queue
                # put remaining in correct order
                remaining = chunks[i:]
                for r in reversed(remaining):
                    self._queue.appendleft(r)
                # If API gave floodwait, _send_message_chunk already set it.
                return
            # successful, store last message id/content (use the latest chunk)
            mid = mid_or_err
            self._last_message = (mid, chunk)

        # done; reset counters and flush other handlers
        self._lines_since_update = 0
        self._last_flush_ts = time.time()
        await self._flush_other_handlers()

    async def _flush_other_handlers(self):
        """When one handler successfully sends, try to flush others immediately (non-blocking)."""
        # schedule flushes for other handlers on bg loop
        tasks = []
        now = time.time()
        for h in list(TelegramLogHandler._handlers):
            if h is self:
                continue
            if not h._queue:
                continue
            if h._floodwait_until > now:
                continue
            tasks.append(h._handle_send())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ---------------------------
    # HTTP helpers
    # ---------------------------
    async def _verify_bot(self) -> bool:
        ok, res = await self._api_post("getMe", {})
        if not ok:
            return False
        self._initialized = True
        return True

    async def _send_message_chunk(self, chunk: str) -> Tuple[bool, Optional[int]]:
        """
        Send single message chunk. Returns (ok, message_id or error_info).
        On timeout/network error returns (False, description) and requeues left at caller.
        """
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.chat_id
        payload["text"] = f"```k-server\n{chunk}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        ok, res = await self._api_post("sendMessage", payload)
        if ok:
            try:
                mid = res["result"]["message_id"]
                return True, mid
            except Exception:
                return False, None
        # handle API-level failure (floodwait, thread not found, etc.)
        await self._handle_api_error(res, failed_chunk=chunk)
        return False, None

    async def _edit_message(self, message_id: int, new_text: str) -> bool:
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.chat_id
        payload["message_id"] = message_id
        payload["text"] = f"```k-server\n{new_text}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        ok, res = await self._api_post("editMessageText", payload)
        if ok:
            return True
        await self._handle_api_error(res, failed_chunk=new_text)
        return False

    async def _api_post(self, method: str, json_payload: dict) -> Tuple[bool, dict]:
        """Perform POST using shared session. Returns (ok_bool, response_dict)."""
        if TelegramLogHandler._session is None or TelegramLogHandler._session.closed:
            # try to (re)create session on bg loop
            await TelegramLogHandler._ensure_session_and_flusher()

        url = f"https://api.telegram.org/bot{self.token}/{method}"
        sess = TelegramLogHandler._session
        try:
            async with sess.post(url, json=json_payload) as resp:
                try:
                    data = await resp.json()
                except Exception:
                    return False, {}
                return data.get("ok", False), data
        except asyncio.TimeoutError:
            return False, {"ok": False, "description": "timeout"}
        except ClientError as e:
            return False, {"ok": False, "description": str(e)}
        except Exception as e:
            return False, {"ok": False, "description": str(e)}

    async def _handle_api_error(self, res: dict, failed_chunk: Optional[str] = None):
        """
        Interpret Telegram API errors:
          - on floodwait: set self._floodwait_until
          - on message/thread not found: reset last message id
          - on timeout/network errors: requeue failed_chunk at left
        """
        desc = (res.get("description") or "").lower()
        params = res.get("parameters") or {}
        code = res.get("error_code")

        # Timeout or network error (handled earlier too)
        if "timeout" in desc or "connection" in desc or res == {}:
            if failed_chunk:
                self._queue.appendleft(failed_chunk)
            return

        if code == 429:
            retry_after = int(params.get("retry_after", 30))
            self._floodwait_until = time.time() + retry_after
            if failed_chunk:
                self._queue.appendleft(failed_chunk)
            return

        if "message thread not found" in desc or "chat not found" in desc:
            # bad thread/topic — reset last message so next send creates new
            self._last_message = None
            self._initialized = False
            if failed_chunk:
                self._queue.appendleft(failed_chunk)
            return

        if "message to edit not found" in desc:
            self._last_message = None
            self._initialized = False
            if failed_chunk:
                self._queue.appendleft(failed_chunk)
            return

        if "message is not modified" in desc:
            # not an error — nothing to requeue
            return

        # other errors: put back and log
        if failed_chunk:
            self._queue.appendleft(failed_chunk)
        # print minimal info for debugging
        print(f"TGLogger API error: {res.get('description')}")

    # ---------------------------
    # chunking utility
    # ---------------------------
    def _split_into_telegram_chunks(self, text: str) -> List[str]:
        """Split by lines preserving line boundaries and <=4096 size per chunk."""
        chunks: List[str] = []
        current = ""
        for line in text.split("\n"):
            if line == "":
                line = " "
            # break very long single lines
            while len(line) > 4096:
                part = line[:4096]
                line = line[4096:]
                if current:
                    chunks.append(current)
                    current = part
                else:
                    chunks.append(part)
            if (len(current) + len(line) + (1 if current else 0)) <= 4096:
                current = (current + "\n" + line) if current else line
            else:
                if current:
                    chunks.append(current)
                current = line
        if current:
            chunks.append(current)
        return chunks

    # ---------------------------
    # cleanup helpers
    # ---------------------------
    @classmethod
    def close_all(cls):
        """Stop flusher and close shared session (best-effort)."""
        if cls._bg_loop and cls._bg_loop.is_running():
            async def _shutdown():
                if cls._flusher_task:
                    cls._flusher_task.cancel()
                    try:
                        await cls._flusher_task
                    except Exception:
                        pass
                if cls._session and not cls._session.closed:
                    await cls._session.close()
                    cls._session = None

            fut = asyncio.run_coroutine_threadsafe(_shutdown(), cls._bg_loop)
            try:
                fut.result(timeout=5)
            except Exception:
                pass

# End of class
