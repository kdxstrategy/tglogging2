import io
import time
import asyncio
import weakref
import aiohttp
import threading
from logging import StreamHandler
from aiohttp import ClientSession, FormData, ClientError, ClientConnectorError, ServerTimeoutError

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    _handlers = weakref.WeakSet()
    _worker_started = False
    _queue = None
    _loop = None  # event loop для фонового режима

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
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.wait_time = update_interval
        self.minimum = minimum_lines
        self.pending = pending_logs

        self.message_buffer = []
        self.floodwait = 0
        self.message_id = 0
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.initialized = False
        self.last_sent_content = ""

        self._handlers.add(self)

        if TelegramLogHandler._queue is None:
            TelegramLogHandler._queue = asyncio.Queue()

    def emit(self, record):
        # запуск воркера лениво — при первом логе
        if not TelegramLogHandler._worker_started:
            self._ensure_worker()
            TelegramLogHandler._worker_started = True

        msg = self.format(record)
        self.message_buffer.append(msg)
        self.lines += 1
        now = time.time()

        if (
            len("\n".join(self.message_buffer)) >= 3000
            or (now - self.last_update >= self.wait_time and self.lines >= self.minimum)
        ):
            payload = "\n".join(self.message_buffer)
            self.message_buffer = []
            self.lines = 0
            self.last_update = now
            asyncio.run_coroutine_threadsafe(
                TelegramLogHandler._queue.put((self, payload)), TelegramLogHandler._loop
            )

    def _ensure_worker(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # нет активного event loop → создаём свой
            loop = asyncio.new_event_loop()
            TelegramLogHandler._loop = loop

            def run_loop():
                asyncio.set_event_loop(loop)
                loop.run_forever()

            threading.Thread(target=run_loop, daemon=True).start()
        else:
            TelegramLogHandler._loop = loop

        # запускаем воркер
        asyncio.run_coroutine_threadsafe(self._worker(), TelegramLogHandler._loop)

    async def _worker(self):
        """Фоновая задача, которая обрабатывает очередь"""
        while True:
            handler, message = await TelegramLogHandler._queue.get()
            try:
                await handler.handle_logs(message)
            except Exception as e:
                print(f"[TGLogger] Worker error: {e}")
                await TelegramLogHandler._queue.put((handler, message))
                await asyncio.sleep(5)

    async def handle_logs(self, full_message: str):
        if not full_message.strip() or self.floodwait:
            await asyncio.sleep(self.floodwait or 1)
            await TelegramLogHandler._queue.put((self, full_message))
            return

        if not self.initialized:
            success = await self.initialize_bot()
            if not success:
                await TelegramLogHandler._queue.put((self, full_message))
                return

        if self.message_id and len(self.last_sent_content + "\n" + full_message) <= 4096:
            combined_message = self.last_sent_content + "\n" + full_message
            success = await self.edit_message(combined_message)
            if success:
                self.last_sent_content = combined_message
                return
        for chunk in self._split_into_chunks(full_message):
            if chunk.strip():
                ok = await self.send_message(chunk)
                if not ok:
                    await TelegramLogHandler._queue.put((self, chunk))

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
            timeout = aiohttp.ClientTimeout(total=3)
            async with ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as response:
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

    async def send_message(self, message):
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"```k-server\n{message}```"
        payload["chat_id"] = self.log_chat_id
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            self.message_id = res["result"]["message_id"]
            self.last_sent_content = message
            return True
        await self.handle_error(res, message)
        return False

    async def edit_message(self, message):
        if not self.message_id:
            return False
        payload = DEFAULT_PAYLOAD.copy()
        payload["message_id"] = self.message_id
        payload["text"] = f"```k-server\n{message}```"
        payload["chat_id"] = self.log_chat_id
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/editMessageText", payload)
        if res.get("ok"):
            self.last_sent_content = message
            return True
        await self.handle_error(res, message)
        return False

    async def handle_error(self, resp: dict, failed_message: str = None):
        error = resp.get("parameters", {})
        error_code = resp.get("error_code")
        description = resp.get("description", "")

        if error_code == 429:  # floodwait
            retry_after = error.get("retry_after", 30)
            print(f"[TGLogger] Floodwait {retry_after}s")
            self.floodwait = retry_after
            if failed_message:
                await asyncio.sleep(retry_after)
                await TelegramLogHandler._queue.put((self, failed_message))
        elif "not found" in description.lower():
            print("[TGLogger] Message/thread not found — resetting")
            self.message_id = 0
            self.initialized = False
            if failed_message:
                await TelegramLogHandler._queue.put((self, failed_message))
        elif "not modified" in description.lower():
            print("[TGLogger] Message not modified, skipping")
        else:
            print(f"[TGLogger] API error: {description}")
            if failed_message:
                await TelegramLogHandler._queue.put((self, failed_message))
