import time
import asyncio
import weakref
import aiohttp
import threading
from logging import StreamHandler
from aiohttp import ClientSession, ClientError, ClientConnectorError, ServerTimeoutError

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    _handlers = weakref.WeakSet()
    _worker_started = False
    _queue = None
    _loop = None
    _thread = None
    _session: ClientSession | None = None

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: int = None,
        update_interval: float = 5.0,
        minimum_lines: int = 1,
        pending_logs: int = 200000,
    ):
        super().__init__()
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.default_topic_id = int(topic_id) if topic_id else None
        self.update_interval = update_interval
        self.minimum = minimum_lines
        self.pending = pending_logs

        # буферы по каждому topic
        self.buffers: dict[int | None, dict] = {}
        self.message_ids: dict[int | None, int] = {}
        self.last_sent: dict[int | None, str] = {}
        self.floodwait = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.initialized = False

        self._handlers.add(self)

        if TelegramLogHandler._queue is None:
            TelegramLogHandler._queue = asyncio.Queue()

        if not TelegramLogHandler._worker_started:
            self._start_worker()
            TelegramLogHandler._worker_started = True

    # -----------------------
    # Logging
    # -----------------------
    def emit(self, record):
        msg = self.format(record)
        topic = self.default_topic_id
        buf = self.buffers.setdefault(
            topic, {"messages": [], "lines": 0, "last_update": time.time()}
        )

        buf["messages"].append(msg)
        buf["lines"] += 1
        now = time.time()

        # если накопилось достаточно строк — сразу в очередь
        if buf["lines"] >= self.minimum:
            self._enqueue((self, topic, "\n".join(buf["messages"])))
            buf["messages"] = []
            buf["lines"] = 0
            buf["last_update"] = now

    def _enqueue(self, item):
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(TelegramLogHandler._queue.put(item))
        except RuntimeError:
            if TelegramLogHandler._loop:
                TelegramLogHandler._loop.call_soon_threadsafe(
                    asyncio.create_task, TelegramLogHandler._queue.put(item)
                )

    # -----------------------
    # Background loop
    # -----------------------
    def _start_worker(self):
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._worker())
            loop.create_task(self._flusher())
            print("[TGLogger] Worker attached to existing loop")
        except RuntimeError:
            TelegramLogHandler._loop = asyncio.new_event_loop()

            def run_loop(loop):
                asyncio.set_event_loop(loop)
                loop.create_task(self._worker())
                loop.create_task(self._flusher())
                loop.run_forever()

            TelegramLogHandler._thread = threading.Thread(
                target=run_loop, args=(TelegramLogHandler._loop,), daemon=True
            )
            TelegramLogHandler._thread.start()
            print("[TGLogger] Worker running in background thread")

    async def _flusher(self):
        while True:
            await asyncio.sleep(0.5)  # проверяем чаще, чем update_interval
            now = time.time()
            for topic, buf in list(self.buffers.items()):
                if buf["messages"] and (now - buf["last_update"] >= self.update_interval):
                    buffer_text = "\n".join(buf["messages"])
                    self._enqueue((self, topic, buffer_text))
                    buf["messages"] = []
                    buf["lines"] = 0
                    buf["last_update"] = now

    async def _worker(self):
        if not TelegramLogHandler._session:
            TelegramLogHandler._session = ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        while True:
            handler, topic, message = await TelegramLogHandler._queue.get()
            try:
                await handler.handle_logs(topic, message)
            except Exception as e:
                print(f"[TGLogger] Worker error: {e}")
                await asyncio.sleep(5)
                handler._enqueue((handler, topic, message))

    # -----------------------
    # Telegram API
    # -----------------------
    async def handle_logs(self, topic_id: int | None, full_message: str):
        if not full_message.strip() or self.floodwait:
            await asyncio.sleep(self.floodwait or 1)
            self._enqueue((self, topic_id, full_message))
            return

        if not self.initialized:
            success = await self.initialize_bot()
            if not success:
                self._enqueue((self, topic_id, full_message))
                return

        last_sent = self.last_sent.get(topic_id, "")
        message_id = self.message_ids.get(topic_id, 0)

        if message_id and len(last_sent + "\n" + full_message) <= 4096:
            combined_message = last_sent + "\n" + full_message
            success = await self.edit_message(topic_id, message_id, combined_message)
            if success:
                self.last_sent[topic_id] = combined_message
                return

        for chunk in self._split_into_chunks(full_message):
            if chunk.strip():
                ok = await self.send_message(topic_id, chunk)
                if not ok:
                    self._enqueue((self, topic_id, chunk))

    def _split_into_chunks(self, message: str):
        chunks, current_chunk = [], ""
        for line in message.split("\n"):
            if not line:
                line = " "
            while len(line) > 4096:
                split_pos = line[:4096].rfind(" ")
                if split_pos <= 0:
                    split_pos = 4096
                part = line[:split_pos]
                line = line[split_pos:].lstrip()
                if current_chunk:
                    current_chunk += "\n" + part
                else:
                    current_chunk = part
                if len(current_chunk) >= 4096:
                    chunks.append(current_chunk)
                    current_chunk = ""
            if len(current_chunk) + len(line) + 1 <= 4096:
                current_chunk += ("\n" if current_chunk else "") + line
            else:
                chunks.append(current_chunk)
                current_chunk = line
        if current_chunk:
            chunks.append(current_chunk)
        return chunks

    async def initialize_bot(self):
        uname, is_alive = await self.verify_bot()
        if not is_alive:
            print("[TGLogger] Invalid bot token")
            return False
        self.initialized = True
        return True

    async def send_request(self, url, payload):
        try:
            async with TelegramLogHandler._session.post(url, json=payload) as response:
                return await response.json()
        except (asyncio.TimeoutError, ClientConnectorError, ServerTimeoutError) as e:
            print(f"[TGLogger] Timeout: {e}")
            return {"ok": False, "description": str(e)}
        except ClientError as e:
            print(f"[TGLogger] Aiohttp error: {e}")
            return {"ok": False, "description": str(e)}
        except Exception as e:
            print(f"[TGLogger] Unexpected error: {e}")
            return {"ok": False, "description": str(e)}

    async def verify_bot(self):
        res = await self.send_request(f"{self.base_url}/getMe", {})
        if res.get("error_code") == 401:
            return None, False
        return res.get("result", {}).get("username"), True

    async def send_message(self, topic_id, message):
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"```k-server\n{message}```"
        payload["chat_id"] = self.log_chat_id
        if topic_id:
            payload["message_thread_id"] = topic_id

        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            self.message_ids[topic_id] = res["result"]["message_id"]
            self.last_sent[topic_id] = message
            return True
        await self.handle_error(res, topic_id, message)
        return False

    async def edit_message(self, topic_id, message_id, message):
        payload = DEFAULT_PAYLOAD.copy()
        payload["message_id"] = message_id
        payload["text"] = f"```k-server\n{message}```"
        payload["chat_id"] = self.log_chat_id
        if topic_id:
            payload["message_thread_id"] = topic_id

        res = await self.send_request(f"{self.base_url}/editMessageText", payload)
        if res.get("ok"):
            self.last_sent[topic_id] = message
            return True
        await self.handle_error(res, topic_id, message)
        return False

    async def handle_error(self, resp: dict, topic_id, failed_message: str = None):
        error = resp.get("parameters", {})
        error_code = resp.get("error_code")
        description = resp.get("description", "")

        if error_code == 429:
            retry_after = error.get("retry_after", 30)
            print(f"[TGLogger] Floodwait {retry_after}s")
            self.floodwait = retry_after
            if failed_message:
                await asyncio.sleep(retry_after)
                self._enqueue((self, topic_id, failed_message))
        elif "not found" in description.lower():
            print("[TGLogger] Message/thread not found — resetting")
            self.message_ids[topic_id] = 0
            self.initialized = False
            if failed_message:
                self._enqueue((self, topic_id, failed_message))
        elif "not modified" in description.lower():
            print("[TGLogger] Message not modified, skipping")
        else:
            print(f"[TGLogger] API error: {description}")
            if failed_message:
                self._enqueue((self, topic_id, failed_message))
