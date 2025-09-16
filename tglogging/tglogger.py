import io
import time
import asyncio
import threading
import weakref
import logging
from typing import Dict, List, Tuple, Optional
from logging import StreamHandler
from aiohttp import ClientSession, ClientError, ClientConnectorError, ServerTimeoutError, FormData

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    """
    Telegram log handler:
     - buffers messages per handler instance (per topic_id)
     - background async loop in thread flushes buffers periodically
     - appends to last message via editMessageText when possible
     - on network error messages are returned to the front of buffer
     - safe for sync code (emit is non-blocking)
    """

    # Background loop + registry shared across all handlers
    _bg_loop: Optional[asyncio.AbstractEventLoop] = None
    _bg_thread: Optional[threading.Thread] = None
    _bg_started = False
    _registered_handlers = weakref.WeakSet()

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: Optional[int] = None,
        update_interval: int = 5,
        minimum_lines: int = 1,
        max_batch_chars: int = 3000,
        request_timeout: int = 10,
    ):
        super().__init__()
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id is not None else None
        self.update_interval = float(update_interval)
        self.minimum_lines = int(minimum_lines)
        self.max_batch_chars = int(max_batch_chars)
        self.request_timeout = int(request_timeout)

        self.base_url = f"https://api.telegram.org/bot{self.token}"
        # Buffer for incoming log lines (FIFO)
        self._buffer_lock = threading.Lock()
        self._buffer: List[str] = []  # lines waiting to be sent, oldest at index 0

        # Last sent message tracking (so we can edit the latest message)
        # (message_id, content)
        self._last_message_id: Optional[int] = None
        self._last_content: str = ""

        # floodwait/time handling
        self._floodwait_until = 0.0  # epoch time while we should delay sends

        # register
        TelegramLogHandler._registered_handlers.add(self)

        # start background loop once
        if not TelegramLogHandler._bg_started:
            TelegramLogHandler._start_background_loop()

    # ---------------------------
    # Public handler API
    # ---------------------------
    def emit(self, record: logging.LogRecord) -> None:
        """
        Synchronous. Just append formatted message to buffer.
        Not blocking. Background loop will process.
        """
        try:
            msg = self.format(record)
        except Exception:
            # formatting error shouldn't break program
            msg = f"<log-format-error: {record!r}>"
        with self._buffer_lock:
            self._buffer.append(msg)

        # If buffer large enough, wake background to process ASAP:
        # we signal by scheduling immediate run in bg loop (non-blocking)
        if self._buffer_length_chars() >= self.max_batch_chars:
            # schedule immediate flush in background loop
            loop = TelegramLogHandler._bg_loop
            if loop and loop.is_running():
                # schedule coroutine to flush all handlers now
                asyncio.run_coroutine_threadsafe(
                    TelegramLogHandler._flush_all_handlers_once(), loop
                )

    # ---------------------------
    # Buffer utilities
    # ---------------------------
    def _pop_all_buffer(self) -> List[str]:
        """Atomically take all buffer and return it (oldest first)."""
        with self._buffer_lock:
            items = self._buffer[:]
            self._buffer.clear()
        return items

    def _prepend_to_buffer(self, lines: List[str]) -> None:
        """Put lines back to front (preserve order)."""
        if not lines:
            return
        with self._buffer_lock:
            # put returned lines in front
            self._buffer = lines + self._buffer

    def _buffer_length_chars(self) -> int:
        with self._buffer_lock:
            return sum(len(s) + 1 for s in self._buffer)

    def _buffer_lines_count(self) -> int:
        with self._buffer_lock:
            return len(self._buffer)

    # ---------------------------
    # Background loop management
    # ---------------------------
    @classmethod
    def _start_background_loop(cls):
        """Start a dedicated event loop in a background thread."""
        if cls._bg_started:
            return

        # Create a fresh event loop in new thread
        loop = asyncio.new_event_loop()
        cls._bg_loop = loop

        def _run_loop():
            asyncio.set_event_loop(loop)
            loop.create_task(cls._background_worker())
            loop.run_forever()

        t = threading.Thread(target=_run_loop, daemon=True, name="tg-logger-bg")
        cls._bg_thread = t
        t.start()
        cls._bg_started = True

    @classmethod
    async def _background_worker(cls):
        """
        Periodic background worker: every instance's update_interval seconds
        attempts to flush that handler's buffer.
        Also provides a helper to run an immediate flush on demand (_flush_all_handlers_once).
        """
        # Running in bg loop
        while True:
            # For each registered handler try to flush if eligible.
            handlers = list(cls._registered_handlers)
            # iterate a copy to avoid mutation issues
            for h in handlers:
                try:
                    # If floodwait is active, skip
                    now = time.time()
                    if getattr(h, "_floodwait_until", 0) > now:
                        continue
                    # only flush if enough lines or non-empty
                    if h._buffer_lines_count() >= h.minimum_lines or h._buffer_length_chars() >= h.max_batch_chars:
                        # flush this handler
                        await h._flush_once()
                except Exception as e:
                    # swallow errors to keep worker alive
                    print("[TGLogger] background handler error:", e)
            # sleep for shortest update_interval among handlers to be responsive
            # compute minimal update_interval among active handlers (or default 1s)
            intervals = [getattr(h, "update_interval", 1.0) for h in cls._registered_handlers]
            sleep_t = min(intervals) if intervals else 1.0
            await asyncio.sleep(max(0.5, sleep_t))

    @classmethod
    async def _flush_all_handlers_once(cls):
        """Immediate one-time flush of all handlers (called from sync thread via run_coroutine_threadsafe)."""
        handlers = list(cls._registered_handlers)
        for h in handlers:
            try:
                now = time.time()
                if getattr(h, "_floodwait_until", 0) > now:
                    continue
                if h._buffer_lines_count() > 0:
                    await h._flush_once()
            except Exception as e:
                print("[TGLogger] immediate flush error:", e)

    # ---------------------------
    # Sending logic
    # ---------------------------
    async def _flush_once(self):
        """
        Take the buffer atomically and attempt to send it:
         - try to append to last message via editMessageText if possible
         - otherwise send new message(s)
         - on failure (network), restore the chunk(s) back to front of buffer
        """
        # Take buffer snapshot
        with self._buffer_lock:
            if not self._buffer:
                return
            lines = self._buffer[:]
            self._buffer.clear()

        full_text = "\n".join(lines).strip()
        if not full_text:
            return

        # If we are under floodwait (from API response), skip and restore
        now = time.time()
        if self._floodwait_until > now:
            # Put lines back and return
            with self._buffer_lock:
                self._buffer = lines + self._buffer
            return

        # Ensure bot alive
        ok = await self._ensure_initialized()
        if not ok:
            # restore
            with self._buffer_lock:
                self._buffer = lines + self._buffer
            return

        # Try to append to last message if exists and fits
        try:
            if self._last_message_id is not None:
                combined = (self._last_content + "\n" + full_text) if self._last_content else full_text
                if len(combined) <= 4096:
                    edited = await self._api_edit_message(self._last_message_id, combined)
                    if edited:
                        self._last_content = combined
                        # success, done
                        return
                    # if edit failed with "message is not modified" or other, continue to sending new
            # Send as new message(s) split into chunks
            chunks = list(self._split_into_chunks(full_text))
            # Send sequentially; if any chunk fails due to network, restore remaining chunks + earlier ones
            for idx, chunk in enumerate(chunks):
                sent_ok, msg_id, resp = await self._api_send_message(chunk)
                if sent_ok:
                    # update last sent
                    self._last_message_id = msg_id
                    self._last_content = chunk
                    # continue to next chunk
                else:
                    # network or API error: restore unsent and previously unsaved lines to front
                    # We must reconstruct remaining text: unsent chunk + chunks after + original buffer (which was cleared).
                    remaining_chunks = chunks[idx:]  # chunk that failed and those after
                    # Put them back as lines (split by newline to maintain order)
                    restored_lines = []
                    for c in remaining_chunks:
                        restored_lines.extend(c.splitlines())
                    # prepend restored_lines to buffer
                    with self._buffer_lock:
                        self._buffer = restored_lines + self._buffer
                    # if API returned floodwait, honor it (set in handle_error)
                    return
        except Exception as e:
            # On unexpected exception, restore everything
            with self._buffer_lock:
                self._buffer = lines + self._buffer
            print("[TGLogger] Unexpected error during flush:", e)
            return

    async def _ensure_initialized(self) -> bool:
        """Verify bot works (simple getMe). Avoid repeated calls if already initialized."""
        if getattr(self, "initialized", False):
            return True
        try:
            res = await self._api_request(f"{self.base_url}/getMe", {})
            ok = bool(res.get("ok", False))
            if ok:
                self.initialized = True
            return ok
        except Exception:
            return False

    # ---------------------------
    # Telegram API wrappers
    # ---------------------------
    async def _api_request(self, url: str, payload: dict, form: Optional[FormData] = None) -> dict:
        """Low-level request with error handling and timeout"""
        timeout = self.request_timeout
        try:
            async with ClientSession(timeout=asyncio.ClientTimeout(total=timeout)) as session:
                if form is not None:
                    async with session.post(url, data=form, params=payload) as resp:
                        return await resp.json()
                else:
                    async with session.post(url, json=payload) as resp:
                        return await resp.json()
        except asyncio.TimeoutError as e:
            # mark floodwait small period to avoid immediate retries
            print("[TGLogger] Request timeout:", e)
            return {"ok": False, "description": "timeout"}
        except (ClientConnectorError, ClientError, ServerTimeoutError) as e:
            print("[TGLogger] Client error:", e)
            return {"ok": False, "description": str(e)}
        except Exception as e:
            print("[TGLogger] Unexpected request error:", e)
            return {"ok": False, "description": str(e)}

    async def _api_send_message(self, text: str) -> Tuple[bool, Optional[int], dict]:
        """Send message and handle common errors. On failure, set floodwait if present."""
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["text"] = f"```k-server\n{text}```"
        if self.topic_id is not None:
            payload["message_thread_id"] = self.topic_id

        res = await self._api_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            return True, res["result"]["message_id"], res
        # handle error: floodwait etc.
        await self._handle_api_error(res)
        return False, None, res

    async def _api_edit_message(self, message_id: int, text: str) -> bool:
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["message_id"] = message_id
        payload["text"] = f"```k-server\n{text}```"
        if self.topic_id is not None:
            payload["message_thread_id"] = self.topic_id

        res = await self._api_request(f"{self.base_url}/editMessageText", payload)
        if res.get("ok"):
            return True
        await self._handle_api_error(res)
        return False

    async def _handle_api_error(self, res: dict):
        """
        Inspect API error response. If floodwait, set _floodwait_until.
        If message/thread not found, reset last_message tracking so next send will create new.
        """
        description = (res.get("description") or "").lower()
        params = res.get("parameters") or {}
        err_code = res.get("error_code")
        if err_code == 429 or "flood" in description or "too many requests" in description:
            retry_after = params.get("retry_after") or 30
            # set floodwait window
            self._floodwait_until = time.time() + float(retry_after)
            print(f"[TGLogger] floodwait: {retry_after}s for chat {self.log_chat_id} topic {self.topic_id}")
        if "message thread not found" in description or "chat not found" in description:
            # reset message id so we create a new message next time
            self._last_message_id = None
            self._last_content = ""
        if "message to edit not found" in description:
            self._last_message_id = None
            self._last_content = ""

    # ---------------------------
    # Chunk splitting logic
    # ---------------------------
    def _split_into_chunks(self, message: str):
        """
        Generator yielding chunks <= 4096 chars, preserving line order.
        Yields strings.
        """
        maxc = 4096
        lines = message.split("\n")
        cur = ""
        for line in lines:
            if line == "":
                line = " "
            # if single line longer than maxc -> split hard
            while len(line) > maxc:
                part = line[:maxc]
                line = line[maxc:]
                if cur:
                    yield cur
                    cur = ""
                yield part
            if not cur:
                if len(line) <= maxc:
                    cur = line
                else:
                    # (shouldn't reach here because of above)
                    yield line
                    cur = ""
            else:
                if len(cur) + 1 + len(line) <= maxc:
                    cur = cur + "\n" + line
                else:
                    yield cur
                    cur = line
        if cur:
            yield cur

# ---------------------------
# Example usage
# ---------------------------
if __name__ == "__main__":
    # quick demo - replace token/chat with real values to actually send
    TEST_TOKEN = "YOUR_BOT_TOKEN"
    TEST_CHAT = 123456789

    logger = logging.getLogger("tg_demo")
    logger.setLevel(logging.DEBUG)

    handler = TelegramLogHandler(
        token=TEST_TOKEN,
        log_chat_id=TEST_CHAT,
        topic_id=None,  # or integer thread id
        update_interval=3,
        minimum_lines=1,
        max_batch_chars=1500,
        request_timeout=8,
    )
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Emulate sync code generating logs
    for i in range(20):
        logger.info("Test message %d" % i)
        time.sleep(0.6)

    # Keep main thread alive briefly so background can send
    time.sleep(10)
