import io
import time
import asyncio
import nest_asyncio
import weakref
from logging import StreamHandler
from aiohttp import ClientSession, FormData, ClientTimeout

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    """
    Improved handler to send logs to Telegram chats with thread/topic support.
    Preserves all messages during floodwait, queues on timeout,
    and appends new logs without overwriting old ones.
    """
    _handlers = weakref.WeakSet()  # Registry of all active handlers

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
        self.loop = asyncio.get_event_loop()
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

    def emit(self, record):
        msg = self.format(record)
        self.lines += 1
        self.message_buffer.append(msg)

        now = time.time()
        current_length = len("\n".join(self.message_buffer))

        # Срабатывание по размеру
        if current_length >= 3000 and not self.floodwait:
            asyncio.create_task(self.handle_logs(force_send=True))
            self.lines = 0
            self.last_update = now

        # Срабатывание по таймеру
        elif (now - self.last_update >= max(self.wait_time, self.floodwait)
              and self.lines >= self.minimum):
            if self.floodwait:
                self.floodwait = 0
            asyncio.create_task(self.handle_logs())
            self.lines = 0
            self.last_update = now

        # Проверка очередей у других хендлеров
        for handler in TelegramLogHandler._handlers:
            if handler is self or not handler.message_buffer:
                continue

            time_diff = now - handler.last_update
            buffer_length = len("\n".join(handler.message_buffer))
            if ((time_diff >= max(handler.wait_time, handler.floodwait)
                 and handler.lines >= handler.minimum)
                    or buffer_length >= 3000):
                if handler.floodwait:
                    handler.floodwait = 0
                asyncio.create_task(handler.handle_logs())
                handler.lines = 0
                handler.last_update = now

    async def handle_logs(self, force_send=False):
        if not self.message_buffer or self.floodwait:
            return

        full_message = "\n".join(self.message_buffer)
        self.message_buffer = []  # очищаем только если точно отправляем

        if not full_message.strip():
            return

        if not self.initialized:
            success = await self.initialize_bot()
            if not success:
                # возвращаем в очередь при ошибке
                self.message_buffer.insert(0, full_message)
                return

        # если можем редактировать предыдущее сообщение
        if (self.message_id
                and len(self.last_sent_content + "\n" + full_message) <= 4096):
            combined = self.last_sent_content + "\n" + full_message
            success = await self.edit_message(combined)
            if success:
                self.last_sent_content = combined
            else:
                await self._send_chunks(full_message)
        else:
            await self._send_chunks(full_message)

    async def _send_chunks(self, full_message: str):
        chunks = self._split_into_chunks(full_message)
        for chunk in chunks:
            if chunk.strip():
                ok = await self.send_message(chunk)
                if not ok:
                    # если не отправилось — вернём в очередь
                    self.message_buffer.insert(0, chunk)
                    break

    def _split_into_chunks(self, message):
        chunks, current = [], ""
        for line in message.split("\n"):
            if line == "":
                line = " "
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
            if len(current) + len(line) + 1 <= 4096:
                current = f"{current}\n{line}" if current else line
            else:
                chunks.append(current)
                current = line
        if current:
            chunks.append(current)
        return chunks

    async def initialize_bot(self):
        uname, is_alive = await self.verify_bot()
        if not is_alive:
            print("TGLogger: [ERROR] - Invalid bot token")
            return False
        self.initialized = True
        return True

    async def send_request(self, url, payload):
        try:
            timeout = ClientTimeout(total=1)
            async with ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as resp:
                    return await resp.json()
        except Exception as e:
            print(f"TGLogger: [WARN] send_request failed: {e}")
            return {"ok": False, "description": str(e)}

    async def verify_bot(self):
        res = await self.send_request(f"{self.base_url}/getMe", {})
        if res.get("error_code") == 401:
            return None, False
        return res.get("result", {}).get("username"), True

    async def send_message(self, message):
        if not message.strip():
            return False
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["text"] = f"```k-server\n{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id
        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            self.message_id = res["result"]["message_id"]
            self.last_sent_content = message
            return True
        await self.handle_error(res)
        return False

    async def edit_message(self, message):
        if not message.strip() or not self.message_id:
            return False
        if message == self.last_sent_content:
            return True
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["message_id"] = self.message_id
        payload["text"] = f"```k-server\n{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id
        res = await self.send_request(f"{self.base_url}/editMessageText", payload)
        if res.get("ok"):
            self.last_sent_content = message
            return True
        await self.handle_error(res)
        return False

    async def send_as_file(self, logs):
        if not logs:
            return
        file = io.BytesIO(f"k-server\n{logs}".encode())
        file.name = "logs.txt"
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["caption"] = "```k-server\nLogs (too large for message)```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id
        try:
            timeout = ClientTimeout(total=1)
            async with ClientSession(timeout=timeout) as session:
                data = FormData()
                data.add_field("document", file, filename="logs.txt")
                async with session.post(
                    f"{self.base_url}/sendDocument", data=data, params=payload
                ) as resp:
                    await resp.json()
        except Exception as e:
            print(f"TGLogger: [WARN] send_as_file failed: {e}")

    async def handle_error(self, resp: dict):
        error = resp.get("parameters", {})
        code = resp.get("error_code")
        desc = resp.get("description", "")
        if desc == "message thread not found":
            print(f"Thread {self.topic_id} not found - resetting")
            self.message_id = 0
            self.initialized = False
        elif code == 429:
            retry_after = error.get("retry_after", 30)
            print(f"Floodwait: {retry_after}s")
            self.floodwait = retry_after
        elif "message to edit not found" in desc:
            self.message_id = 0
            self.initialized = False
        elif "message is not modified" in desc:
            pass
        else:
            print(f"Telegram API error: {desc}")
