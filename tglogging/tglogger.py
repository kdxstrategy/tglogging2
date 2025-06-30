import contextlib
import io
import time
import asyncio
import nest_asyncio
from logging import StreamHandler
from aiohttp import ClientSession, FormData
from collections import deque

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}

class TelegramLogHandler(StreamHandler):
    """
    Improved handler to send logs to telegram chats with thread/topic support.
    Handles both interval-based and size-based triggering.
    """

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: int = None,
        update_interval: int = 5,
        minimum_lines: int = 1,
        pending_logs: int = 200000,
        max_message_length: int = 3000,
    ):
        StreamHandler.__init__(self)
        self.loop = asyncio.get_event_loop()
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.wait_time = update_interval
        self.minimum = minimum_lines
        self.pending = pending_logs
        self.max_msg_len = max_message_length
        self.message_queue = deque()
        self.current_msg = ""
        self.floodwait = 0
        self.message_id = 0
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        DEFAULT_PAYLOAD.update({"chat_id": self.log_chat_id})
        self.initialized = False
        self.lock = asyncio.Lock()

    def emit(self, record):
        msg = self.format(record)
        self.lines += 1
        self.message_queue.append(msg)
        
        # Check immediate send conditions
        if (len(self.message_queue) >= self.minimum and 
            (self.should_send_by_size() or self.should_send_by_time())):
            self.loop.run_until_complete(self.process_queue())

    def should_send_by_time(self):
        return (time.time() - self.last_update) >= max(self.wait_time, self.floodwait)

    def should_send_by_size(self):
        return len('\n'.join(self.message_queue)) > self.max_msg_len

    async def process_queue(self):
        async with self.lock:
            if not self.message_queue:
                return
                
            # Combine all pending messages
            combined = '\n'.join(self.message_queue)
            self.message_queue.clear()
            
            try:
                # Handle very large messages
                if len(combined) > self.pending:
                    await self.send_as_file(combined)
                    return
                
                # Initialize if needed
                if not self.initialized:
                    await self.initialize_bot()
                    
                # Handle message sending
                await self.send_logs(combined)
                
            except Exception as e:
                print(f"Error processing logs: {e}")
            finally:
                self.lines = 0
                self.last_update = time.time()

    async def initialize_bot(self):
        """Initialize bot connection and message thread"""
        uname, is_alive = await self.verify_bot()
        if not is_alive:
            print("TGLogger: [ERROR] - Invalid bot token")
            return False
            
        await self.initialise()
        self.initialized = True
        return True

    async def send_logs(self, logs):
        """Smart log sending with chunk management"""
        if not self.message_id:
            # First message after initialization
            await self.send_message(logs[:self.max_msg_len])
            remaining = logs[self.max_msg_len:]
            if remaining:
                await self.send_logs(remaining)
            return
            
        # Calculate how much we can append to current message
        available_space = self.max_msg_len - len(self.current_msg)
        appendable = logs[:available_space]
        
        if appendable:
            new_content = self.current_msg + appendable
            await self.edit_message(new_content)
            self.current_msg = new_content
            logs = logs[available_space:]
        
        # Send remaining logs
        while logs:
            chunk = logs[:self.max_msg_len]
            await self.send_message(chunk)
            self.current_msg = chunk
            logs = logs[self.max_msg_len:]

    async def send_request(self, url, payload):
        async with ClientSession() as session:
            async with session.post(url, json=payload) as response:
                return await response.json()

    async def verify_bot(self):
        res = await self.send_request(f"{self.base_url}/getMe", {})
        if res.get("error_code") == 401:
            return None, False
        return res.get("result", {}).get("username"), True

    async def initialise(self):
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = "```Logging initialized```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            self.message_id = res["result"]["message_id"]
            self.current_msg = payload["text"]
            return True
        return False

    async def send_message(self, message):
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"```{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            self.message_id = res["result"]["message_id"]
            return True
        await self.handle_error(res)
        return False

    async def edit_message(self, message):
        payload = DEFAULT_PAYLOAD.copy()
        payload["message_id"] = self.message_id
        payload["text"] = f"```{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/editMessageText", payload)
        if res.get("ok"):
            return True
        await self.handle_error(res)
        return False

    async def send_as_file(self, logs):
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
                return await response.json()

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
            print(f'Floodwait: {retry_after} seconds')
            self.floodwait = retry_after
        elif "message to edit not found" in description:
            print("Message to edit not found - resetting")
            self.message_id = 0
            self.initialized = False
        else:
            print(f"Telegram API error: {description}")

    def __del__(self):
        """Ensure all logs are sent before destruction"""
        if self.message_queue:
            self.loop.run_until_complete(self.process_queue())
