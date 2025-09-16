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
    Preserves all messages during floodwait and appends new logs to previous message when possible.
    Works in both sync and async code.
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
        timeout: int = 10,
    ):
        StreamHandler.__init__(self)
        self.loop = asyncio.get_event_loop()
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.wait_time = update_interval
        self.minimum = minimum_lines
        self.pending = pending_logs
        self.timeout = timeout
        self.message_buffer = []
        self.floodwait = 0
        self.message_id = 0
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        DEFAULT_PAYLOAD.update({"chat_id": self.log_chat_id})
        self.initialized = False
        self.last_sent_content = ""  # Track last successfully sent content
        self._handlers.add(self)

    def emit(self, record):
        msg = self.format(record)
        self.lines += 1
        self.message_buffer.append(msg)

        now = time.time()
        current_length = len("\n".join(self.message_buffer))

        def run_async(coro):
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(coro)
            except RuntimeError:
                # нет активного цикла → синхронный код
                asyncio.run(coro)

        # Срабатывание по размеру
        if current_length >= 3000 and not self.floodwait:
            run_async(self.handle_logs(force_send=True))
            self.lines = 0
            self.last_update = now

        # Срабатывание по таймеру
        elif (now - self.last_update >= max(self.wait_time, self.floodwait)
              and self.lines >= self.minimum):
            if self.floodwait:
                self.floodwait = 0
            run_async(self.handle_logs())
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
                run_async(handler.handle_logs())
                handler.lines = 0
                handler.last_update = now

    async def handle_logs(self, force_send=False):
        if not self.message_buffer or self.floodwait:
            return

        full_message = "\n".join(self.message_buffer)
        self.message_buffer = []  # Clear buffer only after checking floodwait

        if not full_message.strip():
            return

        # Initialize if needed
        if not self.initialized:
            success = await self.initialize_bot()
            if not success:
                self.message_buffer = [full_message]  # Restore buffer if init fails
                return

        # Check if we can append to the existing message
        if self.message_id and len(self.last_sent_content + "\n" + full_message) <= 4096:
            combined_message = self.last_sent_content + "\n" + full_message
            success = await self.edit_message(combined_message)
            if success:
                self.last_sent_content = combined_message
            else:
                await self.send_message(full_message)
        else:
            # Send as new message(s)
            chunks = self._split_into_chunks(full_message)
            for chunk in chunks:
                if chunk.strip():
                    await self.send_message(chunk)

    def _split_into_chunks(self, message):
        """Split message into chunks respecting line boundaries and size limits"""
        chunks = []
        current_chunk = ""
        lines = message.split("\n")

        for line in lines:
            if line == "":
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
                if current_chunk:
                    current_chunk += "\n" + line
                else:
                    current_chunk = line
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = line

        if current_chunk:
            chunks.append(current_chunk)

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
            timeout = ClientTimeout(total=self.timeout)
            async with ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as response:
                    return await response.json()
        except Exception as e:
            print(f"TGLogger: [WARNING] request failed: {e}")
            return {}

    async def verify_bot(self):
        res = await self.send_request(f"{self.base_url}/getMe", {})
        if res.get("error_code") == 401:
            return None, False
        return res.get("result", {}).get("username"), True

    async def send_message(self, message):
        if not message.strip():
            return False

        payload = DEFAULT_PAYLOAD.copy()
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
        payload["caption"] = "```k-server\nLogs (too large for message)```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            timeout = ClientTimeout(total=self.timeout)
            async with ClientSession(timeout=timeout) as session:
                data = FormData()
                data.add_field("document", file, filename="logs.txt")
                async with session.post(
                    f"{self.base_url}/sendDocument",
                    data=data,
                    params=payload,
                ) as response:
                    await response.json()
        except Exception as e:
            print(f"TGLogger: [WARNING] send_as_file failed: {e}")

    async def handle_error(self, resp: dict):
        error = resp.get("parameters", {})
        error_code = resp.get("error_code")
        description = resp.get("description", "")

        if description == "message thread not found":
            print(f"Thread {self.topic_id} not found - resetting")
            self.message_id = 0
            self.initialized = False
        elif error_code == 429:  # Too Many Requests
            retry_after = error.get("retry_after", 30)
            print(f"Floodwait: {retry_after} seconds")
            self.floodwait = retry_after
        elif "message to edit not found" in description:
            print("Message to edit not found - resetting")
            self.message_id = 0
            self.initialized = False
        elif "message is not modified" in description:
            print("Message not modified (no changes), skipping")
        elif description:
            print(f"Telegram API error: {description}")
