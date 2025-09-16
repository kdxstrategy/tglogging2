import io
import time
import asyncio
import nest_asyncio
import weakref
import threading
from logging import StreamHandler
from aiohttp import ClientSession, FormData, ClientConnectorError, ServerTimeoutError, ClientError, ClientTimeout

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    """
    Improved handler to send logs to Telegram chats with thread/topic support.

    Key behavior:
    - Works from synchronous code: emit() is non-blocking and pushes to a background queue.
    - Per-topic buffers & per-topic pending chunk queues (FIFO order).
    - When worker sends for one topic it also attempts to flush pending chunks for other topics.
    - Network operations have maximum 1s timeout. On timeout/connection error, chunks are put back
      to the front of the pending queue to be retried later.
    - Avoids "coroutine was never awaited" by scheduling tasks into the worker loop or awaiting within it.
    """

    # Global worker shared by all handlers
    _handlers = weakref.WeakSet()
    _worker_started = False
    _loop: asyncio.AbstractEventLoop | None = None
    _thread: threading.Thread | None = None
    _queue: "asyncio.Queue[TelegramLogHandler]" | None = None

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: int = None,
        update_interval: int = 5,
        minimum_lines: int = 1,
        pending_logs: int = 200000,
    ):
        super().__init__()
        # synchronous-facing attributes
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.wait_time = update_interval
        self.minimum = minimum_lines
        self.pending = pending_logs

        # per-handler state
        self._buffer_lines: list[str] = []  # accumulating lines (not yet split into chunks)
        self._pending_chunks: list[str] = []  # chunks ready to be sent (FIFO)
        self.floodwait = 0
        self.message_id = 0
        self.last_sent_content = ""  # text of last successfully sent/edited message
        self.last_update = 0
        self.initialized = False

        self.base_url = f"https://api.telegram.org/bot{token}"
        # store chat_id into default payload copy to avoid writing DEFAULT_PAYLOAD globally each time
        self._default_payload = DEFAULT_PAYLOAD.copy()
        self._default_payload["chat_id"] = self.log_chat_id

        # register
        TelegramLogHandler._handlers.add(self)

        # ensure worker running
        if TelegramLogHandler._queue is None:
            TelegramLogHandler._queue = asyncio.Queue()

        if not TelegramLogHandler._worker_started:
            self._start_worker()
            TelegramLogHandler._worker_started = True

    # --------------------------
    # Background worker management
    # --------------------------
    def _start_worker(self):
        """Start a background event loop and worker thread."""
        try:
            # if there's already a running loop in this thread then attach worker to it
            loop = asyncio.get_running_loop()
            TelegramLogHandler._loop = loop
            # schedule the worker coroutine
            loop.create_task(self._worker())
            print("[TGLogger] Worker attached to existing loop")
        except RuntimeError:
            # create a new event loop in a background thread
            loop = asyncio.new_event_loop()
            TelegramLogHandler._loop = loop

            def run_loop(loop: asyncio.AbstractEventLoop):
                asyncio.set_event_loop(loop)
                loop.create_task(self._worker())
                loop.run_forever()

            t = threading.Thread(target=run_loop, args=(loop,), daemon=True)
            TelegramLogHandler._thread = t
            t.start()
            print("[TGLogger] Worker running in background thread")

    # --------------------------
    # Public emit (synchronous)
    # --------------------------
    def emit(self, record):
        """
        Called in synchronous code. Should be non-blocking.
        We append the formatted record to the per-handler buffer, and if thresholds hit we
        move buffer -> pending_chunks and enqueue handler to the global queue.
        """
        try:
            msg = self.format(record)
        except Exception:
            # formatting error should not break application
            return

        # append to local buffer
        self._buffer_lines.append(msg)

        now = time.time()
        self.last_update = self.last_update or now

        # if accumulated text too large or enough lines/time passed — flush buffer to pending chunks
        joined = "\n".join(self._buffer_lines)
        should_flush_by_size = len(joined) >= 3000
        should_flush_by_time = (now - self.last_update) >= self.wait_time and len(self._buffer_lines) >= self.minimum

        if should_flush_by_size or should_flush_by_time:
            # split into chunks and append to pending_chunks
            chunks = self._split_into_chunks(joined)
            # ensure FIFO order: extend to end
            self._pending_chunks.extend(chunks)
            # clear buffer
            self._buffer_lines.clear()
            self.last_update = now

            # enqueue this handler to worker so it will be processed
            self._enqueue_handler()

        # Even if we didn't flush this handler, if other handlers have pending chunks, we should enqueue them too
        # to satisfy requirement "При вызове логера отправки в один топик, должны отправляться накопленные ... в других топиках"
        current_time = time.time()
        for h in TelegramLogHandler._handlers:
            if h is self:
                continue
            if not h._pending_chunks:
                continue
            # if other handler has waiting chunks and they've waited enough (or are big) — ensure worker is aware
            buffer_len = sum(len(c) for c in h._pending_chunks)
            waited = current_time - (h.last_update or current_time)
            if buffer_len >= 1 or waited >= max(h.wait_time, h.floodwait):
                h._enqueue_handler()

    def _enqueue_handler(self):
        """Thread-safe enqueue of this handler into the global queue for the worker to pick up."""
        loop = TelegramLogHandler._loop
        if loop is None or TelegramLogHandler._queue is None:
            return
        # Use run_coroutine_threadsafe to put into queue to avoid coroutine warnings
        fut = asyncio.run_coroutine_threadsafe(TelegramLogHandler._queue.put(self), loop)
        try:
            fut.result(timeout=0.1)
        except Exception:
            # ignore timing issues; the worker will eventually pick it
            pass

    # --------------------------
    # Worker & processing
    # --------------------------
    async def _worker(self):
        """Continuously processes handlers from the queue."""
        q = TelegramLogHandler._queue
        assert q is not None
        while True:
            handler: "TelegramLogHandler" = await q.get()
            try:
                # When processing a handler, also attempt to flush other handlers' pending chunks
                await self._process_all_handlers_once(starting_handler=handler)
            except Exception as e:
                # on unexpected error, re-queue the handler's pending chunks back
                print(f"[TGLogger] Worker exception: {e}")
                # if handler still has pending chunks they remain; ensure it will be retried later
                # small backoff so we don't tight-loop on exception
                await asyncio.sleep(0.5)

    async def _process_all_handlers_once(self, starting_handler: "TelegramLogHandler"):
        """
        Process the starting handler first, then try to flush pending chunks in other handlers.
        On connection errors / timeout, chunks are put back to the front of their pending queue.
        """
        # Build list snapshot to avoid mutation issues
        handlers = list(TelegramLogHandler._handlers)

        # Ensure starting handler is processed first
        if starting_handler in handlers:
            handlers.remove(starting_handler)
            handlers.insert(0, starting_handler)

        # For each handler, try to send as many pending chunks as possible
        for h in handlers:
            # If no pending chunks for this handler, skip
            if not h._pending_chunks:
                continue

            # If this handler is in floodwait, skip for now
            if h.floodwait:
                continue

            # Try to initialize if needed
            if not h.initialized:
                ok = await h.initialize_bot()
                if not ok:
                    # cannot initialize — push back chunks and skip
                    continue

            # Attempt to send its pending chunks sequentially (respecting order)
            # We'll try to append to last message when possible (edit), otherwise send new messages.
            # For each attempt, network timeout is bounded.
            # If a network error occurs, put the failed chunk and the rest back to the front of queue.
            while h._pending_chunks:
                chunk = h._pending_chunks.pop(0)
                try:
                    # If there's an existing message and combined length fits, try to edit first
                    can_edit = bool(h.message_id) and len((h.last_sent_content or "") + "\n" + chunk) <= 4096
                    if can_edit:
                        edit_ok = await h._safe_edit_message(chunk)
                        if edit_ok:
                            # we edited: last_sent_content was updated inside _safe_edit_message
                            continue
                        else:
                            # edit failed (e.g., message not found) - try to send as new message
                            send_ok = await h._safe_send_message(chunk)
                            if send_ok:
                                continue
                            else:
                                # failed to send — requeue the chunk at front and abort this handler processing
                                h._pending_chunks.insert(0, chunk)
                                break
                    else:
                        # send as new message(s)
                        send_ok = await h._safe_send_message(chunk)
                        if send_ok:
                            continue
                        else:
                            # failed to send — requeue chunk and abort processing this handler
                            h._pending_chunks.insert(0, chunk)
                            break
                except Exception as e:
                    # On any unexpected exception, requeue chunk at front and abort processing this handler
                    print(f"[TGLogger] Exception sending chunk for topic {h.topic_id}: {e}")
                    h._pending_chunks.insert(0, chunk)
                    break

            # after finishing h, small sleep to be polite (not necessary; optional)
            await asyncio.sleep(0)  # yield to loop

    # --------------------------
    # Network helpers with 1s timeout and error handling
    # --------------------------
    async def initialize_bot(self):
        uname, is_alive = await self.verify_bot()
        if not is_alive:
            print("TGLogger: [ERROR] - Invalid bot token")
            return False
        self.initialized = True
        return True

    async def send_request(self, url, payload, *, timeout_seconds: float = 1.0):
        """
        Sends a POST request with a strict timeout. Returns parsed JSON on success or a dict with 'ok': False.
        """
        try:
            timeout = ClientTimeout(total=timeout_seconds)
            async with ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as resp:
                    try:
                        return await resp.json()
                    except Exception:
                        return {"ok": False, "description": f"Invalid json response, status={resp.status}"}
        except (asyncio.TimeoutError, ServerTimeoutError) as e:
            return {"ok": False, "description": "timeout", "error": str(e)}
        except ClientConnectorError as e:
            return {"ok": False, "description": "connection_error", "error": str(e)}
        except ClientError as e:
            return {"ok": False, "description": "client_error", "error": str(e)}
        except Exception as e:
            return {"ok": False, "description": "unexpected", "error": str(e)}

    async def verify_bot(self):
        res = await self.send_request(f"{self.base_url}/getMe", {}, timeout_seconds=1.0)
        if res.get("error_code") == 401 or not res.get("ok"):
            return None, False
        return res.get("result", {}).get("username"), True

    # Safe wrappers that return True/False, requeue on network errors
    async def _safe_send_message(self, message: str) -> bool:
        """Send a message with timeout and error handling. Returns True on success, False on failure.
           On connection/timeout errors, the caller should requeue the message."""
        if not message.strip():
            return True

        payload = self._default_payload.copy()
        payload["text"] = f"```k-server\n{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/sendMessage", payload, timeout_seconds=1.0)
        if res.get("ok"):
            # success
            self.message_id = res["result"]["message_id"]
            self.last_sent_content = message
            return True

        # handle specific errors
        await self.handle_error(res)
        # If description indicates connectivity/timeouts, return False so caller will requeue
        desc = (res.get("description") or "").lower()
        if "timeout" in desc or "connection" in desc or res.get("description") in ("timeout", "connection_error"):
            return False
        # otherwise, consider as handled (do not requeue)
        return False

    async def _safe_edit_message(self, new_suffix: str) -> bool:
        """
        Attempts to append new_suffix to last_sent_content via editMessageText.
        If edit fails for recoverable network reasons, returns False (caller will requeue).
        If edit fails because message doesn't exist, resets message_id and returns False (caller may send new).
        """
        if not self.message_id:
            return False

        combined = (self.last_sent_content + "\n" + new_suffix) if self.last_sent_content else new_suffix
        if combined == self.last_sent_content:
            return True

        payload = self._default_payload.copy()
        payload["message_id"] = self.message_id
        payload["text"] = f"```k-server\n{combined}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/editMessageText", payload, timeout_seconds=1.0)
        if res.get("ok"):
            self.last_sent_content = combined
            return True

        # handle errors similarly to send
        await self.handle_error(res)
        desc = (res.get("description") or "").lower()
        if "timeout" in desc or "connection" in desc or res.get("description") in ("timeout", "connection_error"):
            return False
        # if edit failed because message not found, reset message_id so subsequent send creates new message
        if "not found" in desc or "message to edit not found" in desc:
            self.message_id = 0
            self.initialized = False
            return False
        return False

    async def send_as_file(self, logs: str):
        if not logs:
            return
        file = io.BytesIO(f"k-server\n{logs}".encode())
        file.name = "logs.txt"
        payload = self._default_payload.copy()
        payload["caption"] = "```k-server\nLogs (too large for message)```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            timeout = ClientTimeout(total=1.0)
            async with ClientSession(timeout=timeout) as session:
                data = FormData()
                data.add_field("document", file, filename="logs.txt")
                async with session.post(f"{self.base_url}/sendDocument", data=data, params=payload) as resp:
                    await resp.json()
        except Exception:
            # on error, we don't lose logs — caller should ensure they are retried by putting them back into _pending_chunks
            raise

    async def handle_error(self, resp: dict):
        """Interpret API error responses and set handler state appropriately."""
        error = resp.get("parameters", {}) or {}
        error_code = resp.get("error_code")
        description = resp.get("description", "") or ""

        description_low = description.lower() if isinstance(description, str) else ""

        if "thread not found" in description_low or "message thread not found" in description_low:
            print(f"[TGLogger] Thread {self.topic_id} not found - resetting")
            self.message_id = 0
            self.initialized = False
        elif error_code == 401:
            print("[TGLogger] Invalid bot token (401)")
            self.initialized = False
        elif error_code == 429 or "too many requests" in description_low:
            retry_after = error.get("retry_after", 30)
            print(f"[TGLogger] Floodwait: {retry_after}s")
            self.floodwait = retry_after
        elif "message to edit not found" in description_low or "message not found" in description_low:
            print("[TGLogger] Message to edit not found - resetting")
            self.message_id = 0
            self.initialized = False
        elif "message is not modified" in description_low:
            # nothing to do
            pass
        else:
            # other errors - just log
            # Keep description short to avoid huge prints
            print(f"[TGLogger] API error: {description_low[:200]}")

    # --------------------------
    # Utilities
    # --------------------------
    def _split_into_chunks(self, message: str):
        """
        Split message into chunks respecting line boundaries and Telegram limits (4096 chars).
        Preserves order.
        """
        chunks: list[str] = []
        current = ""
        for line in message.split("\n"):
            if line == "":
                line = " "
            # if line exceeds limit, break it
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
            if len(current) + len(line) + (1 if current else 0) <= 4096:
                current = (current + "\n" + line) if current else line
            else:
                if current:
                    chunks.append(current)
                current = line
        if current:
            chunks.append(current)
        return chunks
