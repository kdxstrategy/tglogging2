import io
import time
import asyncio
import nest_asyncio
from logging import StreamHandler
from aiohttp import ClientSession, FormData

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Default payload settings for Telegram API
DEFAULT_PAYLOAD = {
    "disable_web_page_preview": True,
    "parse_mode": "Markdown"
}

class TelegramLogHandler(StreamHandler):
    """
    Complete Telegram logging handler with:
    - Thread/topic support
    - Floodwait handling without blocking
    - Message queuing during rate limits
    - Automatic retries
    - Message editing to reduce spam
    - File fallback for large messages
    """

    # Class variables for synchronization
    _last_global_update = 0
    _active_handlers = []

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: int = None,
        update_interval: int = 5,
        minimum_lines: int = 1,
        max_retries: int = 3,
        max_message_length: int = 3000
    ):
        """
        Initialize the Telegram log handler.
        
        Args:
            token: Telegram bot token
            log_chat_id: ID of chat/channel to send logs to
            topic_id: ID of thread/topic (optional)
            update_interval: Seconds between updates (default: 5)
            minimum_lines: Minimum lines before sending (default: 1)
            max_retries: Maximum send attempts (default: 3)
            max_message_length: Max characters per message (default: 3000)
        """
        StreamHandler.__init__(self)
        self.loop = asyncio.get_event_loop()
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.wait_time = update_interval
        self.minimum = minimum_lines
        self.max_retries = max_retries
        self.max_length = max_message_length
        
        # Message handling state
        self.message_buffer = []
        self.current_msg = ""
        self.floodwait_until = 0
        self.message_id = 0
        self.lines = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.initialized = False
        self.last_edit_time = 0
        self.sent_messages = {}
        self.pending_messages = []
        self.retry_count = 0
        
        # Configure default payload
        DEFAULT_PAYLOAD["chat_id"] = self.log_chat_id
        self._register_handler()

    def _register_handler(self):
        """Register this handler in the class's active handlers list"""
        TelegramLogHandler._active_handlers.append(self)

    def emit(self, record):
        """Handle log record emission"""
        try:
            msg = self.format(record)
            self.lines += msg.count('\n') + 1
            self.message_buffer.append(msg)
            
            # Immediate send if buffer too large
            if len('\n'.join(self.message_buffer)) >= self.max_length:
                self.loop.run_until_complete(self.handle_logs(force_send=True))
                return
                
            # Regular interval check
            current_time = time.time()
            if (current_time - TelegramLogHandler._last_global_update >= self.wait_time and 
                self.lines >= self.minimum):
                self.loop.run_until_complete(self.handle_logs())
                self.lines = 0
                TelegramLogHandler._last_global_update = current_time
        except Exception as e:
            print(f"Log emit error: {str(e)}")

    async def handle_logs(self, force_send=False):
        """Process and send all pending logs"""
        if not self.message_buffer and not self.pending_messages:
            return

        # Check if we're in floodwait period
        if time.time() < self.floodwait_until:
            if self.message_buffer:
                self.pending_messages.extend(self.message_buffer)
                self.message_buffer = []
            return

        # Combine new and pending messages
        all_messages = self.message_buffer + self.pending_messages
        self.message_buffer = []
        self.pending_messages = []
        
        full_message = '\n'.join(all_messages)
        if not full_message.strip():
            return

        # Initialize if needed
        if not self.initialized:
            success = await self.initialize_bot()
            if not success:
                return

        # Split into chunks and process
        chunks = self._split_into_chunks(full_message)
        
        for i, chunk in enumerate(chunks):
            if i == 0 and self.message_id:
                if chunk != self.current_msg:
                    success = await self.edit_message(chunk)
                    if not success:
                        await self.send_message_with_retry(chunk)
            else:
                await self.send_message_with_retry(chunk)
        
        self.current_msg = chunks[-1] if chunks else ""

    def _split_into_chunks(self, message):
        """Split message into properly sized chunks"""
        chunks = []
        current_chunk = self.current_msg
        
        # First try combining with existing message
        if current_chunk:
            test_chunk = f"{current_chunk}\n{message}"
            if len(test_chunk) <= self.max_length:
                return [test_chunk]
        
        # Split into new chunks
        lines = message.split('\n')
        current_chunk = ""
        
        for line in lines:
            if not line:
                continue
                
            if len(current_chunk) + len(line) + 1 <= self.max_length:
                current_chunk = f"{current_chunk}\n{line}" if current_chunk else line
            else:
                chunks.append(current_chunk)
                current_chunk = line
        
        if current_chunk:
            chunks.append(current_chunk)
            
        return chunks

    async def send_message_with_retry(self, message, retry_count=0):
        """Send message with automatic retry logic"""
        if retry_count >= self.max_retries:
            print(f"Max retries reached for message: {message[:50]}...")
            # Fallback to file if text sending fails
            if len(message) > 500:
                await self.send_as_file(message)
                return True
            return False
            
        success = await self.send_message(message)
        if not success:
            await asyncio.sleep(2 ** retry_count)  # Exponential backoff
            return await self.send_message_with_retry(message, retry_count + 1)
        return True

    async def initialize_bot(self):
        """Verify bot token and initialize logging"""
        uname, is_alive = await self.verify_bot()
        if not is_alive:
            print("TGLogger: [ERROR] - Invalid bot token")
            return False
            
        success = await self.initialise()
        if success:
            self.initialized = True
        return success

    async def send_request(self, url, payload):
        """Send API request to Telegram"""
        async with ClientSession() as session:
            async with session.post(url, json=payload) as response:
                return await response.json()

    async def verify_bot(self):
        """Check if bot token is valid"""
        res = await self.send_request(f"{self.base_url}/getMe", {})
        if res.get("error_code") == 401:
            return None, False
        return res.get("result", {}).get("username"), True

    async def initialise(self):
        """Send initialization message"""
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
        """Send message to Telegram"""
        if not message:
            return False
            
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"```{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            res = await self.send_request(f"{self.base_url}/sendMessage", payload)
            if res.get("ok"):
                message_id = res["result"]["message_id"]
                self.message_id = message_id
                self.current_msg = message
                self.sent_messages[message_id] = message
                return True
                
            await self.handle_error(res, message)
            return False
        except Exception as e:
            print(f"Send error: {str(e)}")
            self.pending_messages.append(message)
            return False

    async def edit_message(self, message):
        """Edit existing message"""
        if not message or not self.message_id:
            return False
            
        # Rate limit edits to once per second
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

    async def handle_error(self, resp: dict, original_message=None):
        """Handle Telegram API errors"""
        error = resp.get("parameters", {})
        error_code = resp.get("error_code")
        description = resp.get("description", "")
        
        if description == "message thread not found":
            print(f"Thread {self.topic_id} not found - resetting")
            self.message_id = 0
            self.initialized = False
        elif error_code == 429:  # Floodwait
            retry_after = error.get("retry_after", 30)
            print(f'Floodwait: {retry_after} seconds')
            self.floodwait_until = time.time() + retry_after
            if original_message:
                self.pending_messages.insert(0, original_message)  # Requeue at front
        elif "message to edit not found" in description:
            print("Message to edit not found - resetting")
            self.message_id = 0
            self.initialized = False
        else:
            print(f"Telegram API error: {description}")
            if original_message:
                self.pending_messages.append(original_message)

    async def send_as_file(self, logs):
        """Send logs as file when too large"""
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
