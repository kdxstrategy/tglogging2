import contextlib
import io
import time
import asyncio
import nest_asyncio
from logging import StreamHandler
from aiohttp import ClientSession, FormData

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}

class TelegramLogHandler(StreamHandler):
    """
    Improved handler to send logs to Telegram chats with thread/topic support.
    Preserves all messages without deletion and handles both time and size-based triggers.
    """

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
        self.current_messages = {}  # {message_id: current_content}
        self.floodwait = 0
        self.last_message_id = 0  # ID of the most recent message
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        DEFAULT_PAYLOAD.update({"chat_id": self.log_chat_id})
        self.initialized = False

    def emit(self, record):
        msg = self.format(record)
        self.lines += 1
        self.message_buffer.append(msg)
        
        # Check if we should send immediately due to size
        if len('\n'.join(self.message_buffer)) >= 3000:
            self.loop.run_until_complete(self.handle_logs(force_send=True))
            return
            
        # Check if we should send due to interval
        diff = time.time() - self.last_update
        if diff >= max(self.wait_time, self.floodwait) and self.lines >= self.minimum:
            if self.floodwait:
                self.floodwait = 0
            self.loop.run_until_complete(self.handle_logs())
            self.lines = 0
            self.last_update = time.time()

    async def handle_logs(self, force_send=False):
        if not self.message_buffer:
            return

        # Combine all pending messages
        new_content = '\n'.join(self.message_buffer)
        self.message_buffer = []  # Clear buffer immediately
        
        if not new_content.strip():
            return

        # Initialize if needed
        if not self.initialized:
            success = await self.initialize_bot()
            if not success:
                return

        # If we have no messages yet, send the first one
        if not self.current_messages:
            new_msg_id = await self.send_message(new_content)
            if new_msg_id:
                self.current_messages[new_msg_id] = new_content
                self.last_message_id = new_msg_id
            return

        # Get the last message to append to
        last_msg_id = self.last_message_id
        last_content = self.current_messages.get(last_msg_id, "")
        
        # Check if we can append to last message without exceeding size limit
        if len(last_content) + len(new_content) + 1 <= 3000:  # +1 for newline
            updated_content = f"{last_content}\n{new_content}"
            success = await self.edit_message(last_msg_id, updated_content)
            if success:
                self.current_messages[last_msg_id] = updated_content
                return
        
        # If we can't append to last message, send a new one
        new_msg_id = await self.send_message(new_content)
        if new_msg_id:
            self.current_messages[new_msg_id] = new_content
            self.last_message_id = new_msg_id

    async def initialize_bot(self):
        """Initialize bot connection and message thread"""
        uname, is_alive = await self.verify_bot()
        if not is_alive:
            print("TGLogger: [ERROR] - Invalid bot token")
            return False
            
        # Send initial message
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = "```Logging initialized```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            msg_id = res["result"]["message_id"]
            self.current_messages[msg_id] = payload["text"]
            self.last_message_id = msg_id
            self.initialized = True
            return True
        return False

    async def send_request(self, url, payload):
        async with ClientSession() as session:
            async with session.post(url, json=payload) as response:
                return await response.json()

    async def verify_bot(self):
        res = await self.send_request(f"{self.base_url}/getMe", {})
        if res.get("error_code") == 401:
            return None, False
        return res.get("result", {}).get("username"), True

    async def send_message(self, message):
        if not message:
            return None
            
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"```{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            msg_id = res["result"]["message_id"]
            return msg_id
            
        await self.handle_error(res)
        return None

    async def edit_message(self, message_id, message):
        if not message or not message_id:
            return False
            
        payload = DEFAULT_PAYLOAD.copy()
        payload["message_id"] = message_id
        payload["text"] = f"```{message}```"
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
            
        file = io.BytesIO(logs.encode())
        file.name = "logs.txt"
        payload = DEFAULT_PAYLOAD.copy()
        payload["caption"] = "Logs (too large for message)"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        async with ClientSession() as session:
            data = FormData()
            data.add_field('document', file, filename='logs.txt')
            async with session.post(
                f"{self.base_url}/sendDocument",
                data=data,
                params=payload
            ) as response:
                await response.json()

    async def handle_error(self, resp: dict):
        error = resp.get("parameters", {})
        error_code = resp.get("error_code")
        description = resp.get("description", "")
        
        if description == "message thread not found":
            print(f"Thread {self.topic_id} not found - resetting")
            self.current_messages = {}
            self.last_message_id = 0
            self.initialized = False
        elif error_code == 429:  # Too Many Requests
            retry_after = error.get("retry_after", 30)
            print(f'Floodwait: {retry_after} seconds')
            self.floodwait = retry_after
        elif "message to edit not found" in description:
            print("Message to edit not found - resetting")
            msg_id = resp.get("parameters", {}).get("message_id")
            if msg_id in self.current_messages:
                del self.current_messages[msg_id]
            if msg_id == self.last_message_id:
                self.last_message_id = max(self.current_messages.keys(), default=0)
        else:
            print(f"Telegram API error: {description}")
