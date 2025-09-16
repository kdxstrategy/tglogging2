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

    def __init__(
        self,
        token: str,
        chats: list[tuple[int, int | None]],  # [(chat_id, topic_id), ...]
        flush_interval: float = 1.0,
        minimum_lines: int = 1,
    ):
        """
        :param token: Telegram bot token
        :param chats: список [(chat_id, topic_id), ...]
        :param flush_interval: как часто проверять буферы (сек)
        :param minimum_lines: минимальное число строк для отправки
        """
        super().__init__()
        self.token = token
        self.chats = [(int(cid), int(tid) if tid else None) for cid, tid in chats]
        self.flush_interval = flush_interval
        self.minimum = minimum_lines
        self.base_url = f"https://api.telegram.org/bot{token}"

        # отдельные буферы для каждого (chat, topic)
        self.buffers = {(cid, tid): [] for cid, tid in self.chats}

        self._handlers.add(self)

        if TelegramLogHandler._queue is None:
            TelegramLogHandler._queue = asyncio.Queue()

        if not TelegramLogHandler._worker_started:
            self._start_worker()
            TelegramLogHandler._worker_started = True

    # ---------------- Worker init ----------------
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

    def _enqueue(self, item):
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(TelegramLogHandler._queue.put(item))
        except RuntimeError:
            if TelegramLogHandler._loop:
                TelegramLogHandler._loop.call_soon_threadsafe(
                    asyncio.create_task, TelegramLogHandler._queue.put(item)
                )

    # ---------------- Logging ----------------
    def emit(self, record):
        msg = self.format(record)
        for key in self.buffers:
            self.buffers[key].append(msg)

    async def _flusher(self):
        """Периодически сбрасывает буферы"""
        while True:
            await asyncio.sleep(self.flush_interval)
            for key, buf in self.buffers.items():
                if len(buf) >= self.minimum:
                    payload = "\n".join(buf)
                    self.buffers[key] = []
                    self._enqueue((self, key, payload))

    async def _worker(self):
        while True:
            handler, key, message = await TelegramLogHandler._queue.get()
            try:
                await handler.handle_logs(key, message)
            except Exception as e:
                print(f"[TGLogger] Worker error: {e}")
                await asyncio.sleep(2)
                handler._enqueue((handler, key, message))

    # ---------------- Send to Telegram ----------------
    async def handle_logs(self, key, full_message: str):
        chat_id, topic_id = key
        for chunk in self._split_into_chunks(full_message):
            if chunk.strip():
                await self._send_message(chat_id, topic_id, chunk)

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

    async def _send_message(self, chat_id, topic_id, message):
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = chat_id
        payload["text"] = f"```logs\n{message}```"
        if topic_id:
            payload["message_thread_id"] = topic_id
        res = await self._send_request(f"{self.base_url}/sendMessage", payload)
        if not res.get("ok"):
            print(f"[TGLogger] Error sending to {chat_id}/{topic_id}: {res}")

    async def _send_request(self, url, payload):
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as response:
                    return await response.json()
        except (asyncio.TimeoutError, ClientConnectorError, ServerTimeoutError) as e:
            return {"ok": False, "description": f"Timeout {e}"}
        except ClientError as e:
            return {"ok": False, "description": f"ClientError {e}"}
        except Exception as e:
            return {"ok": False, "description": f"Unexpected {e}"}
