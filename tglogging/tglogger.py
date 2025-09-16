import io
import time
import asyncio
import weakref
import logging
from aiohttp import ClientSession, ClientError, FormData

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(logging.StreamHandler):
    """
    Log handler for sending logs to Telegram with thread/topic support.
    - Buffers logs separately for each (chat_id, topic_id).
    - Appends to last message if possible, otherwise creates new.
    - Retries on timeout without losing logs.
    - Works in sync and async code.
    """

    _handlers = weakref.WeakSet()
    _background_started = False

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: int = None,
        update_interval: int = 5,
        minimum_lines: int = 1,
    ):
        super().__init__()
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.update_interval = update_interval
        self.minimum_lines = minimum_lines

        self.base_url = f"https://api.telegram.org/bot{token}"
        self.initialized = False

        # Буфер для накопления строк
        self.message_buffer: list[str] = []

        # Список сообщений [(id, content)] для текущего топика
        self.sent_messages: list[tuple[int, str]] = []

        # Регистрируем хэндлер
        self._handlers.add(self)

        # Запускаем фоновую задачу один раз
        if not TelegramLogHandler._background_started:
            loop = self._get_loop()
            loop.create_task(self._background_task())
            TelegramLogHandler._background_started = True

    # -------------------- Основное API --------------------

    def emit(self, record):
        """Получение лог-сообщения (синхронно)"""
        msg = self.format(record)
        self.message_buffer.append(msg)

    async def handle_logs(self):
        """Отправка накопленных логов"""
        if not self.message_buffer:
            return

        full_message = "\n".join(self.message_buffer)
        self.message_buffer.clear()

        # Инициализация бота
        if not self.initialized:
            ok = await self._verify_bot()
            if not ok:
                self.message_buffer.insert(0, full_message)  # вернуть назад
                return
            self.initialized = True

        # Попробовать добавить в последнее сообщение
        if self.sent_messages:
            last_id, last_content = self.sent_messages[-1]
            new_content = last_content + "\n" + full_message
            if len(new_content) <= 4096:
                ok = await self._edit_message(last_id, new_content)
                if ok:
                    self.sent_messages[-1] = (last_id, new_content)
                    return

        # Иначе — шлём новое
        for chunk in self._split_into_chunks(full_message):
            ok, msg_id = await self._send_message(chunk)
            if ok and msg_id:
                self.sent_messages.append((msg_id, chunk))

    # -------------------- Вспомогательные методы --------------------

    async def _background_task(self):
        """Фоновая задача, отправляющая логи раз в N секунд"""
        while True:
            await asyncio.sleep(self.update_interval)
            for handler in list(self._handlers):
                try:
                    await handler.handle_logs()
                except Exception as e:
                    print("Logger background error:", e)

    def _get_loop(self):
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    def _split_into_chunks(self, message: str):
        """Разделить текст на чанки ≤ 4096 символов"""
        lines = message.split("\n")
        chunk = ""
        for line in lines:
            if not line:
                line = " "
            if len(chunk) + len(line) + 1 > 4096:
                yield chunk
                chunk = line
            else:
                chunk = (chunk + "\n" + line) if chunk else line
        if chunk:
            yield chunk

    async def _send_request(self, url, payload, form_data=None):
        try:
            async with ClientSession() as session:
                if form_data:
                    async with session.post(url, data=form_data, params=payload) as r:
                        return await r.json()
                else:
                    async with session.post(url, json=payload) as r:
                        return await r.json()
        except (ClientError, asyncio.TimeoutError) as e:
            print("Telegram request error:", e)
            return {"ok": False, "description": str(e)}

    async def _verify_bot(self):
        res = await self._send_request(f"{self.base_url}/getMe", {})
        return res.get("ok", False)

    async def _send_message(self, message: str):
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["text"] = f"```logs\n{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self._send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            return True, res["result"]["message_id"]

        # Ошибка: вернуть текст обратно
        self.message_buffer.insert(0, message)
        return False, None

    async def _edit_message(self, message_id: int, message: str):
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["message_id"] = message_id
        payload["text"] = f"```logs\n{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self._send_request(f"{self.base_url}/editMessageText", payload)
        return res.get("ok", False)

    async def _send_as_file(self, logs: str):
        file = io.BytesIO(logs.encode())
        file.name = "logs.txt"
        payload = {"chat_id": self.log_chat_id}
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        form = FormData()
        form.add_field("document", file, filename="logs.txt")

        await self._send_request(f"{self.base_url}/sendDocument", payload, form)


# -------------------- Пример использования --------------------

if __name__ == "__main__":
    import logging

    logger = logging.getLogger("tg")
    logger.setLevel(logging.DEBUG)

    handler = TelegramLogHandler(
        token="YOUR_BOT_TOKEN",
        log_chat_id=123456789,
        topic_id=None,
        update_interval=5,
    )
    logger.addHandler(handler)

    for i in range(20):
        logger.info(f"Test log {i}")
        time.sleep(1)
