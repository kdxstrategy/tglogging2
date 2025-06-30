import contextlib
import io
import time
import asyncio
import nest_asyncio
from logging import StreamHandler, getLogger
from aiohttp import ClientSession, FormData

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}

class TelegramLogHandler(StreamHandler):
    """
    Enhanced handler to send logs to Telegram with:
    - Thread/topic support
    - Floodwait recovery
    - Message verification
    - Persistent message tracking
    - Automatic retries
    """

    _last_global_update = 0  # Class variable to sync time across all handlers
    _active_handlers = []     # Track all active handler instances

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: int = None,
        update_interval: int = 5,
        minimum_lines: int = 1,
        pending_logs: int = 200000,
        max_retries: int = 3,
    ):
        StreamHandler.__init__(self)
        self.loop = asyncio.get_event_loop()
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.wait_time = update_interval
        self.minimum = minimum_lines
        self.pending = pending_logs
        self.max_retries = max_retries
        self.message_buffer = []
        self.current_msg = ""
        self.floodwait = 0
        self.message_id = 0
        self.lines = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.initialized = False
        self.last_edit_time = 0
        self.sent_messages = {}      # {message_id: message_content}
        self.pending_messages = {}   # {timestamp: message_content}
        self.retry_count = 0
        
        DEFAULT_PAYLOAD.update({"chat_id": self.log_chat_id})
        self._register_handler()

    def _register_handler(self):
        """Register this handler for global time synchronization"""
        TelegramLogHandler._active_handlers.append(self)

    def emit(self, record):
        msg = self.format(record)
        
        self.lines += msg.count('\n') + 1
        self.message_buffer.append(msg)
        
        # Immediate send if buffer too large
        if len('\n'.join(self.message_buffer)) >= 3000:
            self.loop.run_until_complete(self.handle_logs(force_send=True))
            return
            
        # Global time sync check
        current_time = time.time()
        if (current_time - TelegramLogHandler._last_global_update >= self.wait_time and 
            self.lines >= self.minimum):
            if self.floodwait:
                self.floodwait = 0
            self.loop.run_until_complete(self.handle_logs())
            self.lines = 0
            TelegramLogHandler._last_global_update = current_time

    async def handle_logs(self, force_send=False):
        if not self.message_buffer:
            return

        new_messages = '\n'.join(self.message_buffer)
        self.message_buffer = []
        
        if not new_messages.strip():
            return

        if not self.initialized:
            success = await self.initialize_bot()
            if not success:
                return

        chunks = self._split_into_chunks(new_messages)
        
        for i, chunk in enumerate(chunks):
            if i == 0 and self.message_id:
                if chunk != self.current_msg:
                    success = await self.edit_message(chunk)
                    if not success:
                        await self.send_message_with_retry(chunk)
            else:
                await self.send_message_with_retry(chunk)
        
        self.current_msg = chunks[-1] if chunks else ""

    async def send_message_with_retry(self, message, retry_count=0):
        """Send message with automatic retry logic"""
        if retry_count >= self.max_retries:
            print(f"Max retries reached for message: {message[:50]}...")
            return False
            
        success = await self.send_message(message)
        if not success:
            await asyncio.sleep(2 ** retry_count)  # Exponential backoff
            return await self.send_message_with_retry(message, retry_count + 1)
        return True

    def _split_into_chunks(self, message):
        """Split message into chunks while preserving newlines"""
        chunks = []
        current_chunk = self.current_msg
        
        if current_chunk:
            test_chunk = f"{current_chunk}\n{message}"
            if len(test_chunk) <= 3000:
                return [test_chunk]
        
        lines = message.split('\n')
        current_chunk = ""
        
        for line in lines:
            if not line:
                continue
                
            if len(current_chunk) + len(line) + 1 <= 3000:
                current_chunk = f"{current_chunk}\n{line}" if current_chunk else line
            else:
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
            
        success = await self.initialise()
        if success:
            self.initialized = True
        return success

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
            self.sent_messages[self.message_id] = self.current_msg
            return True
        return False

    async def send_message(self, message):
        if not message:
            return False
            
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"```{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            message_id = res["result"]["message_id"]
            self.message_id = message_id
            self.current_msg = message
            self.sent_messages[message_id] = message
            if message_id in self.pending_messages:
                del self.pending_messages[message_id]
            return True
            
        # If failed, add to pending messages with timestamp
        timestamp = int(time.time() * 1000)
        self.pending_messages[timestamp] = message
        await self.handle_error(res)
        return False

    async def edit_message(self, message):
        if not message or not self.message_id:
            return False
            
        if time.time() - self.last_edit_time < 1.0:
            return False
            
        if self.current_msg == message:
            return True
            
        payload = DEFAULT_PAYLOAD.copy()
        payload["message_id"] = self.message_id
        payload["text"] = f"```{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/editMessageText", payload)
        if res.get("ok"):
            self.current_msg = message
            self.last_edit_time = time.time()
            self.sent_messages[self.message_id] = message
            return True
            
        await self.handle_error(res)
        return False

    async def verify_message_delivery(self, message_id=None):
        """Verify if messages were actually delivered"""
        if message_id:
            payload = DEFAULT_PAYLOAD.copy()
            payload["message_id"] = message_id
            res = await self.send_request(f"{self.base_url}/getMessage", payload)
            if res.get("ok"):
                return True
            return await self.resend_message(message_id)
        else:
            success = True
            for msg_id, msg_content in list(self.sent_messages.items()):
                if not await self.verify_message_delivery(msg_id):
                    success = False
            for timestamp, msg_content in list(self.pending_messages.items()):
                if await self.send_message_with_retry(msg_content):
                    success = success and True
                else:
                    success = False
            return success

    async def resend_message(self, message_id):
        """Resend a message if verification fails"""
        if message_id in self.sent_messages:
            content = self.sent_messages[message_id]
            del self.sent_messages[message_id]
            return await self.send_message_with_retry(content)
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
                res = await response.json()
                if res.get("ok"):
                    self.message_id = res["result"]["message_id"]
                return res

    async def handle_error(self, resp: dict):
        error = resp.get("parameters", {})
        error_code = resp.get("error_code")
        description = resp.get("description", "")
        
        if description == "message thread not found":
            print(f"Thread {self.topic_id} not found - resetting")
            self.message_id = 0
            self.initialized = False
        elif error_code == 429:
            retry_after = error.get("retry_after", 30)
            print(f'Floodwait: {retry_after} seconds')
            self.floodwait = retry_after
            await asyncio.sleep(retry_after)
            await self.verify_message_delivery()
        elif "message to edit not found" in description:
            print("Message to edit not found - resetting")
            self.message_id = 0
            self.initialized = False
        else:
            print(f"Telegram API error: {description}")

    @classmethod
    def update_all_handlers(cls):
        """Force update all active handlers"""
        current_time = time.time()
        for handler in cls._active_handlers:
            if (current_time - cls._last_global_update >= handler.wait_time and 
                handler.lines >= handler.minimum):
                handler.loop.run_until_complete(handler.handle_logs())
                handler.lines = 0
        cls._last_global_update = current_time

    @classmethod
    async def verify_all_handlers(cls):
        """Verify messages for all active handlers"""
        for handler in cls._active_handlers:
            await handler.verify_message_delivery()
