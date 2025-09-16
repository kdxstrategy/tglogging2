import io
import time
import asyncio
import nest_asyncio
import weakref
import threading
from collections import deque
from logging import StreamHandler
from typing import Optional, Deque, Dict, Tuple, Any
import aiohttp
from aiohttp import ClientTimeout, ClientConnectorError, ClientError

# если в окружении уже есть работающий loop (например в Jupyter), - это поможет
nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    """
    Telegram logger handler with per-topic FIFO queues, background sender,
    non-blocking for sync code, retry-on-timeout (requeue front), edit-last-if-fit logic.
    """

    # глобальные структуры/фоновое окружение (один на процесс)
    _instances = weakref.WeakSet()
    _bg_loop: Optional[asyncio.AbstractEventLoop] = None
    _bg_thread: Optional[threading.Thread] = None
    _bg_task_started = False
    _bg_wakeup: Optional[asyncio.Event] = None
    _session: Optional[aiohttp.ClientSession] = None

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: Optional[int] = None,
        update_interval: int = 5,
        minimum_lines: int = 1,
        chunk_threshold: int = 3000,
        request_timeout: int = 10,
    ):
        super().__init__()
        # basic config
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.default_topic = int(topic_id) if topic_id else None
        self.update_interval = float(update_interval)
        self.minimum_lines = int(minimum_lines)
        self.chunk_threshold = int(chunk_threshold)
        self.request_timeout = int(request_timeout)

        # per-instance state (this handler may represent one chat/topic, but we keep per-topic queue map anyway)
        # queues: key = (chat_id, topic_id)  topic_id can be None
        self._buffers: Dict[Tuple[int, Optional[int]], Deque[str]] = {}
        # last sent message content per topic (for edit)
        self._last_content: Dict[Tuple[int, Optional[int]], str] = {}
        # last sent message_id per topic (list of message_ids; we edit only last)
        self._last_message_id: Dict[Tuple[int, Optional[int]], int] = {}
        # floodwait per topic (seconds)
        self._floodwait_until: Dict[Tuple[int, Optional[int]], float] = {}

        # register instance
        TelegramLogHandler._instances.add(self)

        # start background loop/task & session if needed
        self._ensure_background_loop_and_worker()

    # ----------------------------
    # Public logging interface
    # ----------------------------
    def emit(self, record):
        """
        Non-blocking: append formatted record to the buffer for this handler's default topic.
        The background worker will pick it up. If background loop isn't running in this thread,
        we still safely append to deque and wake worker.
        """
        try:
            msg = self.format(record)
        except Exception:
            # formatting error -> fallback to str(record)
            msg = str(record)

        key = (self.log_chat_id, self.default_topic)
        q = self._buffers.setdefault(key, deque())
        q.append(msg)

        # Wake background worker: use call_soon_threadsafe to set event
        loop = TelegramLogHandler._bg_loop
        if loop and TelegramLogHandler._bg_wakeup:
            # call_soon_threadsafe to set the event (wake)
            loop.call_soon_threadsafe(TelegramLogHandler._bg_wakeup.set)

    # ----------------------------
    # Background loop + worker
    # ----------------------------
    @classmethod
    def _ensure_background_loop_and_worker(cls):
        """Ensure there's a running background asyncio event loop and the worker task."""
        if cls._bg_task_started:
            return

        # Try to get a running loop in current thread (most apps won't have one)
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        if running:
            # attach worker to the existing running loop
            cls._bg_loop = running
            # ensure session and event
            if cls._session is None:
                cls._session = aiohttp.ClientSession(timeout=ClientTimeout(total=cls._default_request_timeout()))
            if cls._bg_wakeup is None:
                cls._bg_wakeup = asyncio.Event()
            # schedule background worker
            asyncio.ensure_future(cls._background_worker(), loop=cls._bg_loop)
            cls._bg_task_started = True
            return

        # else: create a dedicated loop in a background thread
        loop = asyncio.new_event_loop()
        cls._bg_loop = loop
        cls._bg_wakeup = asyncio.Event(loop=loop)
        # create session inside that loop later in thread
        def run_loop():
            asyncio.set_event_loop(loop)
            # create aiohttp session inside the loop
            loop.create_task(cls._ensure_session())
            loop.create_task(cls._background_worker())
            loop.run_forever()

        t = threading.Thread(target=run_loop, daemon=True)
        cls._bg_thread = t
        t.start()
        cls._bg_task_started = True

    @classmethod
    def _default_request_timeout(cls):
        # default timeout for session if not set earlier: 10s
        return 10

    @classmethod
    async def _ensure_session(cls):
        if cls._session is None:
            cls._session = aiohttp.ClientSession(timeout=ClientTimeout(total=cls._default_request_timeout()))

    @classmethod
    async def _background_worker(cls):
        """
        Periodic worker. Iterates over all handler instances and their queues.
        Sends batches that satisfy thresholds; respects floodwaits; requeues on errors.
        """
        # ensure session exists
        await cls._ensure_session()
        wake = cls._bg_wakeup
        # main loop
        while True:
            now = time.time()
            # iterate over instances snapshot
            instances = list(cls._instances)
            # For fairness, we try to flush all topic queues across instances.
            for inst in instances:
                try:
                    await inst._maybe_flush_all_topics()
                except Exception as e:
                    # do not crash worker
                    print(f"[TGLogger] Worker error in instance flush: {e}")

            # Wait either update_interval (minimum across instances) or until wake event is set
            # Compute min update_interval across instances (seconds)
            min_interval = None
            for inst in instances:
                try:
                    iv = getattr(inst, "update_interval", None)
                    if iv is not None:
                        if min_interval is None or iv < min_interval:
                            min_interval = iv
                except Exception:
                    pass
            if min_interval is None:
                min_interval = 1.0

            # Wait with timeout, but wake earlier if something calls set()
            try:
                # clear event then wait
                wake.clear()
                await asyncio.wait_for(wake.wait(), timeout=min_interval)
            except asyncio.TimeoutError:
                continue
            except Exception:
                # on cancellation or other error, loop and continue
                continue

    # ----------------------------
    # Topic-level operations
    # ----------------------------
    async def _maybe_flush_all_topics(self):
        """
        For this handler instance: iterate over its topic queues and flush those ready.
        Sending for one topic will not prevent sending for others (we iterate all).
        """
        # clone keys to avoid mutation during iteration
        keys = list(self._buffers.keys())
        for key in keys:
            # key = (chat_id, topic_id)
            chat_id, topic_id = key
            # check floodwait for this topic
            if self._is_in_floodwait(key):
                continue

            buf = self._buffers.get(key)
            if not buf or len(buf) == 0:
                continue

            # decide whether to flush:
            total_len = sum(len(s) + 1 for s in buf)  # approximate bytes
            if total_len >= getattr(self, "chunk_threshold", 3000) or len(buf) >= getattr(self, "minimum_lines", 1):
                # flush this topic
                await self._flush_topic(key)

    def _is_in_floodwait(self, key: Tuple[int, Optional[int]]) -> bool:
        t = self._floodwait_until.get(key)
        if not t:
            return False
        return time.time() < t

    async def _flush_topic(self, key: Tuple[int, Optional[int]]):
        """
        Build batch from the queue for the topic (preserving FIFO order),
        attempt to edit last message if fits, otherwise send new messages in chunks.
        On network errors, requeue failed chunk at front.
        """
        chat_id, topic_id = key
        # Build full message from the queue contents (all currently buffered messages)
        buf = self._buffers.get(key)
        if not buf or len(buf) == 0:
            return

        # Pop all current items to a local list to try sending them as batch.
        # If send fails, we'll requeue (prepend) remaining/failed parts.
        msgs = []
        while buf:
            msgs.append(buf.popleft())

        full_message = "\n".join(msgs).strip()
        if not full_message:
            return

        # ensure bot initialization (verify token): handled lazily by verify call
        # Try to edit last message if exists and fits
        last_content = self._last_content.get(key, "")
        last_msg_id = self._last_message_id.get(key, 0)

        try:
            # verify bot presence once
            ok = await self._verify_bot_once()
            if not ok:
                # requeue entire batch to front and return
                self._requeue_front(key, [full_message])
                return
        except Exception:
            # on verify error, requeue and return
            self._requeue_front(key, [full_message])
            return

        # If there is a last message id and editing combined won't exceed limit
        if last_msg_id and len((last_content + "\n" + full_message).strip()) <= 4096:
            combined = (last_content + "\n" + full_message).strip()
            edited = await self._try_edit_message(key, last_msg_id, combined)
            if edited:
                # success
                self._last_content[key] = combined
                return
            # if edit failed (e.g., message deleted) - fallthrough to send as new
        # else send as new message(s) splitting into chunks
        chunks = self._split_into_chunks(full_message)
        for chunk in chunks:
            # attempt to send chunk; on failure requeue front and stop further sends
            ok = await self._try_send_chunk(key, chunk)
            if not ok:
                # remaining chunks + rest should be requeued front in order
                # we should requeue current chunk and the rest of chunks (if any)
                remaining = [chunk] + chunks[chunks.index(chunk) + 1:]
                self._requeue_front(key, remaining)
                return

    # ----------------------------
    # low-level send / edit routines
    # ----------------------------
    async def _verify_bot_once(self) -> bool:
        """Ensure remote bot is reachable; create session if needed."""
        # ensure global session is present
        await TelegramLogHandler._ensure_session()
        # a simple getMe
        url = self._base_url("/getMe")
        try:
            async with TelegramLogHandler._session.post(url, json={}) as resp:
                j = await resp.json()
                return bool(j.get("ok"))
        except Exception as e:
            # network error etc.
            # print once
            # print(f"[TGLogger] verify bot error: {e}")
            return False

    def _base_url(self, method: str) -> str:
        # method should already start with '/'; if passed full, handle.
        if method.startswith("http"):
            return method
        return f"https://api.telegram.org/bot{self.token}{method}"

    async def _try_send_chunk(self, key: Tuple[int, Optional[int]], chunk: str) -> bool:
        """Try to send a single chunk. Return True on success; on failure requeue done by caller."""
        chat_id, topic_id = key
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = chat_id
        payload["text"] = f"```k-server\n{chunk}```"
        if topic_id:
            payload["message_thread_id"] = topic_id

        url = self._base_url("/sendMessage")
        # send
        try:
            async with TelegramLogHandler._session.post(url, json=payload) as resp:
                j = await resp.json()
        except asyncio.TimeoutError:
            # requeue handled by caller
            return False
        except ClientConnectorError:
            return False
        except ClientError:
            return False
        except Exception:
            return False

        # handle response
        if j.get("ok"):
            msg_id = j["result"]["message_id"]
            self._last_message_id[key] = msg_id
            self._last_content[key] = chunk
            return True

        # handle API error cases
        await self._handle_api_error_response(key, j)
        return False

    async def _try_edit_message(self, key: Tuple[int, Optional[int]], message_id: int, new_content: str) -> bool:
        """Try edit; return True on success. On failure, set appropriate state."""
        chat_id, topic_id = key
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = chat_id
        payload["message_id"] = message_id
        payload["text"] = f"```k-server\n{new_content}```"
        if topic_id:
            payload["message_thread_id"] = topic_id

        url = self._base_url("/editMessageText")
        try:
            async with TelegramLogHandler._session.post(url, json=payload) as resp:
                j = await resp.json()
        except asyncio.TimeoutError:
            return False
        except ClientConnectorError:
            return False
        except ClientError:
            return False
        except Exception:
            return False

        if j.get("ok"):
            return True

        # If error says message to edit not found -> reset last_message_id so next send will create new message
        await self._handle_api_error_response(key, j)
        return False

    async def _handle_api_error_response(self, key: Tuple[int, Optional[int]], j: Dict[str, Any]):
        """
        Interpret Telegram API errors: floodwait (429), message/thread not found, message not modified, etc.
        For floodwait, set floodwait_until for that topic.
        For deleted message/thread -> reset last_message_id to allow new message.
        """
        code = j.get("error_code")
        desc = j.get("description", "") or ""
        params = j.get("parameters", {}) or {}

        if code == 429:
            retry_after = params.get("retry_after", 30)
            self._floodwait_until[key] = time.time() + float(retry_after)
            # nothing else: messages already popped; they should be requeued by caller
        elif "message thread not found" in desc.lower():
            # reset message id so next send creates a new message
            self._last_message_id.pop(key, None)
            self._last_content.pop(key, None)
        elif "message to edit not found" in desc.lower():
            self._last_message_id.pop(key, None)
            self._last_content.pop(key, None)
        elif "message is not modified" in desc.lower():
            # nothing to do
            pass
        else:
            # other API errors: reset message id if likely a deletion
            if "message to edit not found" in desc.lower() or "chat not found" in desc.lower():
                self._last_message_id.pop(key, None)
                self._last_content.pop(key, None)

    def _requeue_front(self, key: Tuple[int, Optional[int]], items: Any):
        """
        Put items at front of the deque in order (items is sequence).
        If worker was trying to send many messages, we place failed ones back.
        """
        if not items:
            return
        q = self._buffers.setdefault(key, deque())
        # push items in reverse so the first in items becomes the first popped next time
        for it in reversed(list(items)):
            q.appendleft(it)
        # wake worker
        loop = TelegramLogHandler._bg_loop
        if loop and TelegramLogHandler._bg_wakeup:
            loop.call_soon_threadsafe(TelegramLogHandler._bg_wakeup.set)

    # ----------------------------
    # util: split into safe chunks by lines and size
    # ----------------------------
    def _split_into_chunks(self, message: str):
        """
        Split message into chunks <= 4096 (Telegram limit) preferring line boundaries.
        Returns list of chunks preserving order.
        """
        max_len = 4096
        lines = message.split("\n")
        chunks = []
        current = ""
        for line in lines:
            if not line:
                # preserve empty lines as a single space (so lines remain)
                line = " "
            # if single line is bigger than max_len, cut it
            while len(line) > max_len:
                part = line[:max_len]
                line = line[max_len:]
                if current:
                    current = current + "\n" + part
                else:
                    current = part
                chunks.append(current)
                current = ""
            # now line <= max_len
            if not current:
                if len(line) <= max_len:
                    current = line
                else:
                    # shouldn't happen but fallback
                    chunks.append(line)
                    current = ""
            else:
                if len(current) + 1 + len(line) <= max_len:
                    current = current + "\n" + line
                else:
                    chunks.append(current)
                    current = line
        if current:
            chunks.append(current)
        return chunks

    # ----------------------------
    # shutdown helpers
    # ----------------------------
    @classmethod
    async def _close_session(cls):
        if cls._session is not None:
            await cls._session.close()
            cls._session = None

    @classmethod
    def close_background(cls):
        """Stop background loop & thread (if created). Call from program shutdown if desired."""
        loop = cls._bg_loop
        if loop:
            def stopper():
                for task in asyncio.all_tasks(loop):
                    task.cancel()
                loop.stop()
            loop.call_soon_threadsafe(stopper)
        if cls._bg_thread and cls._bg_thread.is_alive():
            # thread will exit when loop stops
            pass

    # ----------------------------
    # context manager / destructor convenience
    # ----------------------------
    def __del__(self):
        # nothing heavy here; closing session handled explicitly
        pass
