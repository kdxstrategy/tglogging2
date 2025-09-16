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
    Each topic has its own buffer and message state.
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

        # Buffers and states per topic
        self.message_buffers = {}        # topic_id -> list of log lines
        self.message_ids = {}            # topic_id -> last message_id
        self.last_sent_contents = {}     # topic_id -> last content sent
        self.lines = {}                  # topic_id -> line counter
        self.last_update = {}            # topic_id -> last send time

        self.floodwait = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        DEFAULT_PAYLOAD.update({"chat_id": self.log_chat_id})
        self.initialized = False

        self._handlers.add(self)

    def _get_topic(self):
        return self.topic_id or 0  # Use 0 as default key if no topic_id

    def emit(self, record):
        msg = self.format(record)
        topic = self._get_topic()

        buf = self.message_buffers.setdefault(topic, [])
        buf.append(msg)
        self.lines[topic] = self.lines.get(topic, 0) + 1

        # Check if need to send due to size
        current_length = len("\n".join(buf))
        if current_length >= 3000 and not self.floodwait:
            self.loop.run_until_complete(self.handle_logs(topic, force_send=True))
            self.lines[topic] = 0
            self.last_update[topic] = time.time()

        # Check if need to send due to interval
        diff = time.time() - self.last_update.get(topic, 0)
        if diff >= max(self.wait_time, self.floodwait) and self.lines[topic] >= self.minimum:
            if self.floodwait:
                self.floodwait = 0
            self.loop.run_until_complete(self.handle_logs(topic))
            self.lines[topic] = 0
            self.last_update[topic] = time.time()

        # Also check other handlers' topics
        current_time = time.time()
        for handler in TelegramLogHandler._handlers:
            for t, b in list(handler.message_buffers.items()):
                if not b:
                    continue
                time_diff = current_time - handler.last_update.get(t, 0)
                buffer_length = len("\n".join(b))
                if (time_diff >= max(handler.wait_time, handler.floodwait)
                        and handler.lines.get(t, 0) >= handler.minimum) or buffer_length >= 3000:
                    if handler.floodwait:
                        handler.floodwait = 0
                    handler.loop.run_until_complete(handler.handle_logs(t))
                    handler.lines[t] = 0
                    handler.last_update[t] = current_time

    async def handle_logs(self, topic, force_send=False):
        buf = self.message_buffers.get(topic, [])
        if not buf or self.floodwait:
            return

        full_message = "\n".join(buf)
        self.message_buffers[topic] = []  # Clear after copy

        if not full_message.strip():
            return

        if not self.initialized:
            success = await self.initialize_bot()
            if not success:
                self.message_buffers[topic] = [full_message]  # restore
                return

        last_content = self.last_sent_contents.get(topic, "")
        msg_id = self.message_ids.get(topic)

        # Try edit message
        if msg_id and len(last_content + "\n" + full_message) <= 4096:
            combined_message = last_content + "\n" + full_message
            success = await self.edit_message(topic, combined_message)
            if success:
                self.last_sent_contents[topic] = combined_message
            else:
                await self.send_message(topic, full_message)
        else:
            # Send new message(s)
            chunks = self._split_into_chunks(full_message)
            for chunk in chunks:
                if chunk.strip():
                    await self.send_message(topic, chunk)

    def _split_into_chunks(self, message):
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
            timeout = ClientTimeout(total=15)
            async with ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as response:
                    return await response.json()
        except asyncio.TimeoutError:
            print("TGLogger: Connection timeout")
            return {"ok": False, "description": "Connection timeout"}

    async def verify_bot(self):
        res = await self.send_request(f"{self.base_url}/getMe", {})
        if res.get("error_code") == 401:
            return None, False
        return res.get("result", {}).get("username"), True

    async def send_message(self, topic, message):
        if not message.strip():
            return False

        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"```k-server\n{message}```"
        if topic:
            payload["message_thread_id"] = topic

        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            self.message_ids[topic] = res["result"]["message_id"]
            self.last_sent_contents[topic] = message
            return True

        await self.handle_error(topic, res)
        return False

    async def edit_message(self, topic, message):
        if not message.strip():
            return False
        msg_id = self.message_ids.get(topic)
        if not msg_id:
            return False
        if message == self.last_sent_contents.get(topic, ""):
            return True

        payload = DEFAULT_PAYLOAD.copy()
        payload["message_id"] = msg_id
        payload["text"] = f"```k-server\n{message}```"
        if topic:
            payload["message_thread_id"] = topic

        res = await self.send_request(f"{self.base_url}/editMessageText", payload)
        if res.get("ok"):
            self.last_sent_contents[topic] = message
            return True

        await self.handle_error(topic, res)
        return False

    async def send_as_file(self, topic, logs):
        if not logs:
            return

        file = io.BytesIO(f"k-server\n{logs}".encode())
        file.name = "logs.txt"
        payload = DEFAULT_PAYLOAD.copy()
        payload["caption"] = "```k-server\nLogs (too large for message)```"
        if topic:
            payload["message_thread_id"] = topic

        timeout = ClientTimeout(total=30)
        async with ClientSession(timeout=timeout) as session:
            data = FormData()
            data.add_field("document", file, filename="logs.txt")
            async with session.post(
                f"{self.base_url}/sendDocument", data=data, params=payload
            ) as response:
                await response.json()

    async def handle_error(self, topic, resp: dict):
        error = resp.get("parameters", {})
        error_code = resp.get("error_code")
        description = resp.get("description", "")

        if description == "message thread not found":
            print(f"Thread {topic} not found - resetting")
            self.message_ids[topic] = 0
            self.initialized = False
        elif error_code == 429:  # Too Many Requests
            retry_after = error.get("retry_after", 30)
            print(f"Floodwait: {retry_after} seconds")
            self.floodwait = retry_after
        elif "message to edit not found" in description:
            print("Message to edit not found - resetting")
            self.message_ids[topic] = 0
            self.initialized = False
        elif "message is not modified" in description:
            print("Message not modified (no changes), skipping")
        elif description:
            print(f"Telegram API error: {description}")
