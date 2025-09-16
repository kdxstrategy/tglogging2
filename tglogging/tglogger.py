import io
import time
import asyncio
import nest_asyncio
import weakref
from logging import StreamHandler
from aiohttp import ClientSession, FormData, ClientError

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    """
    Improved handler to send logs to Telegram chats with thread/topic support.
    Preserves all messages during floodwait and appends new logs to previous message when possible.
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
        StreamHandler.__init__(self)
        self.loop = asyncio.get_event_loop()
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.wait_time = update_interval
        self.minimum = minimum_lines
        self.pending = pending_logs
        self.message_buffer = []
        self.floodwait = 0
        self.sent_messages = []  # хранение списка (id, content)
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        DEFAULT_PAYLOAD.update({"chat_id": self.log_chat_id})
        self.initialized = False
        self._handlers.add(self)

    def _safe_ensure(self, coro):
        """Безопасный запуск корутин из sync-кода"""
        try:
            if self.loop.is_running():
                fut = asyncio.run_coroutine_threadsafe(coro, self.loop)
                return fut.result()
            else:
                return self.loop.run_until_complete(coro)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)

    def emit(self, record):
        msg = self.format(record)
        self.lines += 1
        self.message_buffer.append(msg)

        current_length = len('\n'.join(self.message_buffer))
        if current_length >= 3000:
            if not self.floodwait:
                self._safe_ensure(self.handle_logs(force_send=True))
                self.lines = 0
                self.last_update = time.time()

        diff = time.time() - self.last_update
        if diff >= max(self.wait_time, self.floodwait) and self.lines >= self.minimum:
            if self.floodwait:
                self.floodwait = 0
            self._safe_ensure(self.handle_logs())
            self.lines = 0
            self.last_update = time.time()

        current_time = time.time()
        for handler in TelegramLogHandler._handlers:
            if handler is self:
                continue
            if not handler.message_buffer:
                continue

            time_diff = current_time - handler.last_update
            buffer_length = len('\n'.join(handler.message_buffer))
            if (time_diff >= max(handler.wait_time, handler.floodwait)
                and handler.lines >= handler.minimum) or buffer_length >= 3000:
                if handler.floodwait:
                    handler.floodwait = 0
                handler._safe_ensure(handler.handle_logs())
                handler.lines = 0
                handler.last_update = current_time

    async def handle_logs(self, force_send=False):
        if not self.message_buffer or self.floodwait:
            return

        full_message = '\n'.join(self.message_buffer)
        self.message_buffer = []

        if not full_message.strip():
            return

        if not self.initialized:
            success = await self.initialize_bot()
            if not success:
                self.message_buffer.insert(0, full_message)
                return

        # редактируем последнее сообщение, если не переполнено
        if self.sent_messages:
            last_id, last_text = self.sent_messages[-1]
            if len(last_text + '\n' + full_message) <= 4096:
                combined = last_text + '\n' + full_message
                success = await self.edit_message(last_id, combined)
                if success:
                    self.sent_messages[-1] = (last_id, combined)
                    return
        # иначе создаём новое сообщение
        chunks = self._split_into_chunks(full_message)
        for chunk in chunks:
            if chunk.strip():
                success, mid = await self.send_message(chunk)
                if success:
                    self.sent_messages.append((mid, chunk))
                else:
                    self.message_buffer.insert(0, chunk)  # вернуть в очередь

    def _split_into_chunks(self, message):
        chunks = []
        current_chunk = ""
        lines = message.split('\n')

        for line in lines:
            if line == "":
                line = " "

            while len(line) > 4096:
                split_pos = line[:4096].rfind(' ')
                if split_pos <= 0:
                    split_pos = 4096
                part = line[:split_pos]
                line = line[split_pos:].lstrip()

                if current_chunk:
                    current_chunk += '\n' + part
                else:
                    current_chunk = part

                if len(current_chunk) >= 4096:
                    chunks.append(current_chunk)
                    current_chunk = ""

            if len(current_chunk) + len(line) + 1 <= 4096:
                if current_chunk:
                    current_chunk += '\n' + line
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
            async with ClientSession() as session:
                async with session.post(url, json=payload, timeout=15) as response:
                    return await response.json()
        except (asyncio.TimeoutError, ClientError) as e:
            print(f"TGLogger: [ERROR] Request failed: {e}")
            return {"ok": False, "description": str(e)}

    async def verify_bot(self):
        res = await self.send_request(f"{self.base_url}/getMe", {})
        if res.get("error_code") == 401:
            return None, False
        return res.get("result", {}).get("username"), True

    async def send_message(self, message):
        if not message.strip():
            return False, None

        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"```k-server\n{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            mid = res["result"]["message_id"]
            return True, mid

        await self.handle_error(res)
        return False, None

    async def edit_message(self, message_id, message):
        if not message.strip() or not message_id:
            return False

        payload = DEFAULT_PAYLOAD.copy()
        payload["message_id"] = message_id
        payload["text"] = f"```k-server\n{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/editMessageText", payload)
        if res.get("ok"):
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
            async with ClientSession() as session:
                data = FormData()
                data.add_field('document', file, filename='logs.txt')
                async with session.post(
                    f"{self.base_url}/sendDocument",
                    data=data,
                    params=payload,
                    timeout=20,
                ) as response:
                    await response.json()
        except (asyncio.TimeoutError, ClientError) as e:
            print(f"TGLogger: [ERROR] File upload failed: {e}")
            self.message_buffer.insert(0, logs)

    async def handle_error(self, resp: dict):
        error = resp.get("parameters", {})
        error_code = resp.get("error_code")
        description = resp.get("description", "")

        if description == "message thread not found":
            print(f"Thread {self.topic_id} not found - resetting")
            self.sent_messages = []
            self.initialized = False
        elif error_code == 429:
            retry_after = error.get("retry_after", 30)
            print(f"Floodwait: {retry_after} seconds")
            self.floodwait = retry_after
        elif "message to edit not found" in description:
            print("Message to edit not found - resetting")
            self.sent_messages = []
            self.initialized = False
        elif "message is not modified" in description:
            print("Message not modified, skipping")
        else:
            print(f"Telegram API error: {description}")
