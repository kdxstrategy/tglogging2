import io
import time
import asyncio
import weakref
import nest_asyncio
from logging import StreamHandler
from aiohttp import ClientSession, ClientTimeout, FormData

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    """Логгер с поддержкой топиков и очередей, не теряет сообщения при таймаутах."""

    _handlers = weakref.WeakSet()
    _bg_task_started = False

    def __init__(self, token: str, log_chat_id: int, topic_id: int = None,
                 update_interval: int = 5, minimum_lines: int = 1):
        super().__init__()
        self.loop = asyncio.get_event_loop()
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.update_interval = update_interval
        self.minimum_lines = minimum_lines
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.session = ClientSession(timeout=ClientTimeout(total=10))

        # Буферы сообщений для этого топика
        self.queue = []
        self.sent_messages = []  # список message_id для редактирования
        self.last_content = ""   # содержимое последнего сообщения
        self.floodwait = 0
        self.initialized = False
        self.last_update = 0

        self._handlers.add(self)
        if not TelegramLogHandler._bg_task_started:
            self._start_background_task()

    def emit(self, record):
        msg = self.format(record)
        self.queue.append(msg)

    @classmethod
    def _start_background_task(cls):
        """Запускает фоновую задачу, общую для всех хэндлеров."""
        loop = asyncio.get_event_loop()
        loop.create_task(cls._background_worker())
        cls._bg_task_started = True

    @classmethod
    async def _background_worker(cls):
        """Фоновая задача: каждые update_interval проверяет все буферы."""
        while True:
            now = time.time()
            for handler in list(cls._handlers):
                try:
                    if handler.queue and (now - handler.last_update >= handler.update_interval):
                        await handler._flush()
                        handler.last_update = now
                except Exception as e:
                    print(f"[TGLogger] Background error: {e}")
            await asyncio.sleep(1)

    async def _flush(self):
        """Отправка накопленных сообщений."""
        if not self.queue or self.floodwait:
            return

        if not self.initialized:
            ok = await self._verify_bot()
            if not ok:
                return
            self.initialized = True

        batch = "\n".join(self.queue)
        self.queue.clear()

        # можно ли редактировать последнее сообщение?
        if self.sent_messages and len(self.last_content + "\n" + batch) <= 4096:
            combined = self.last_content + "\n" + batch
            success = await self._edit_message(combined)
            if success:
                self.last_content = combined
                return
            # иначе упадём в отправку нового сообщения

        # отправляем новые чанки
        for chunk in self._split_into_chunks(batch):
            ok = await self._send_message(chunk)
            if not ok:
                # вернуть chunk обратно в начало очереди
                self.queue.insert(0, chunk)
                break

    async def _send_request(self, url, payload):
        try:
            async with self.session.post(url, json=payload) as r:
                return await r.json()
        except asyncio.TimeoutError:
            print("[TGLogger] Timeout, message will be retried")
            raise
        except Exception as e:
            print(f"[TGLogger] Request error: {e}")
            raise

    async def _verify_bot(self):
        try:
            res = await self._send_request(f"{self.base_url}/getMe", {})
            return res.get("ok", False)
        except Exception:
            return False

    async def _send_message(self, message: str):
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["text"] = f"```logs\n{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            res = await self._send_request(f"{self.base_url}/sendMessage", payload)
            if res.get("ok"):
                msg_id = res["result"]["message_id"]
                self.sent_messages.append(msg_id)
                self.last_content = message
                return True
            return False
        except asyncio.TimeoutError:
            self.queue.insert(0, message)
            return False

    async def _edit_message(self, new_content: str):
        if not self.sent_messages:
            return False
        msg_id = self.sent_messages[-1]

        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["message_id"] = msg_id
        payload["text"] = f"```logs\n{new_content}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            res = await self._send_request(f"{self.base_url}/editMessageText", payload)
            if res.get("ok"):
                return True
            return False
        except asyncio.TimeoutError:
            self.queue.insert(0, new_content)
            return False

    def _split_into_chunks(self, text: str):
        lines = text.split("\n")
        chunks, current = [], ""
        for line in lines:
            if len(current) + len(line) + 1 > 4000:
                chunks.append(current)
                current = line
            else:
                current = current + "\n" + line if current else line
        if current:
            chunks.append(current)
        return chunks

    async def close(self):
        await self.session.close()
