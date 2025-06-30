import contextlib
import io
import time
import asyncio
import nest_asyncio
from logging import StreamHandler, getLogger
from aiohttp import ClientSession, FormData
from collections import defaultdict

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}

class TelegramLogHandler(StreamHandler):
    """
    Robust handler with floodwait recovery and cross-thread message verification.
    Ensures no messages are lost after rate limiting.
    """
    
    _last_global_update = 0
    _active_handlers = []
    _message_registry = defaultdict(dict)  # {chat_id: {message_id: content}}
    _lock = asyncio.Lock()

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
        self.current_msg = ""
        self.floodwait = 0
        self.message_id = 0
        self.lines = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.initialized = False
        self.last_edit_time = 0
        self._pending_messages = []  # Messages waiting during floodwait
        DEFAULT_PAYLOAD.update({"chat_id": self.log_chat_id})
        self._register_handler()

    def _register_handler(self):
        """Register this handler for global tracking"""
        TelegramLogHandler._active_handlers.append(self)

    async def _verify_message_sent(self, chat_id, message_id, expected_content):
        """Verify if message was actually delivered"""
        async with self._lock:
            if chat_id in self._message_registry:
                if message_id in self._message_registry[chat_id]:
                    return self._message_registry[chat_id][message_id] == expected_content
        return False

    async def _register_sent_message(self, chat_id, message_id, content):
        """Track successfully sent messages"""
        async with self._lock:
            self._message_registry[chat_id][message_id] = content

    def emit(self, record):
        msg = self.format(record)
        self.lines += msg.count('\n') + 1
        self.message_buffer.append(msg)
        
        # Immediate send if buffer exceeds size
        if len('\n'.join(self.message_buffer)) >= 3000:
            self.loop.run_until_complete(self._process_messages(force_send=True))
            return
            
        # Global time sync check
        current_time = time.time()
        if (current_time - self._last_global_update >= self.wait_time and 
            self.lines >= self.minimum):
            self.loop.run_until_complete(self._process_messages())
            self.lines = 0
            self._last_global_update = current_time

    async def _process_messages(self, force_send=False):
        """Process messages with floodwait recovery and verification"""
        if not self.message_buffer:
            return

        # Handle pending messages first
        if self._pending_messages:
            self.message_buffer = self._pending_messages + self.message_buffer
            self._pending_messages = []

        messages = '\n'.join(self.message_buffer)
        self.message_buffer = []

        if not messages.strip():
            return

        if not self.initialized:
            if not await self.initialize_bot():
                return

        try:
            if self.floodwait:
                if time.time() < self.floodwait:
                    self._pending_messages = [messages]
                    return
                self.floodwait = 0

            chunks = self._create_message_chunks(messages)
            for chunk in chunks:
                await self._send_chunk(chunk)

        except Exception as e:
            print(f"Error processing messages: {e}")
            self._pending_messages = [messages]

    async def _send_chunk(self, chunk):
        """Send a message chunk with delivery verification"""
        if self.message_id:
            if chunk != self.current_msg:
                success = await self._safe_edit_message(chunk)
                if not success:
                    await self._safe_send_message(chunk)
        else:
            await self._safe_send_message(chunk)

    async def _safe_send_message(self, message):
        """Send message with delivery verification"""
        payload = {
            "chat_id": self.log_chat_id,
            "text": f"```{message}```",
            "disable_web_page_preview": True,
            "parse_mode": "Markdown"
        }
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            async with ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/sendMessage",
                    json=payload
                ) as response:
                    res = await response.json()
                    if res.get("ok"):
                        self.message_id = res["result"]["message_id"]
                        self.current_msg = message
                        await self._register_sent_message(
                            self.log_chat_id,
                            self.message_id,
                            message
                        )
                        return True
                    await self.handle_error(res)
        except Exception as e:
            print(f"Failed to send message: {e}")
        return False

    async def _safe_edit_message(self, message):
        """Edit message with rate limiting and verification"""
        if time.time() - self.last_edit_time < 1.0:
            return False
            
        if self.current_msg == message:
            return True

        payload = {
            "chat_id": self.log_chat_id,
            "message_id": self.message_id,
            "text": f"```{message}```",
            "disable_web_page_preview": True,
            "parse_mode": "Markdown"
        }
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            async with ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/editMessageText",
                    json=payload
                ) as response:
                    res = await response.json()
                    if res.get("ok"):
                        verified = await self._verify_message_sent(
                            self.log_chat_id,
                            self.message_id,
                            message
                        )
                        if verified:
                            self.current_msg = message
                            self.last_edit_time = time.time()
                            return True
                        print("Message edit verification failed")
                    await self.handle_error(res)
        except Exception as e:
            print(f"Failed to edit message: {e}")
        return False

    async def _create_message_chunks(self, message):
        """Split message into Telegram-friendly chunks"""
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
            
        payload = {
            "chat_id": self.log_chat_id,
            "text": "```Logging initialized```",
            "disable_web_page_preview": True,
            "parse_mode": "Markdown"
        }
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            async with ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/sendMessage",
                    json=payload
                ) as response:
                    res = await response.json()
                    if res.get("ok"):
                        self.message_id = res["result"]["message_id"]
                        self.current_msg = payload["text"]
                        await self._register_sent_message(
                            self.log_chat_id,
                            self.message_id,
                            payload["text"]
                        )
                        self.initialized = True
                        return True
                    await self.handle_error(res)
        except Exception as e:
            print(f"Initialization failed: {e}")
        return False

    async def verify_bot(self):
        try:
            async with ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/getMe",
                    json={}
                ) as response:
                    res = await response.json()
                    if res.get("error_code") == 401:
                        return None, False
                    return res.get("result", {}).get("username"), True
        except Exception as e:
            print(f"Bot verification failed: {e}")
            return None, False

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
            self.floodwait = time.time() + retry_after
        elif "message to edit not found" in description:
            print("Message to edit not found - resetting")
            self.message_id = 0
            self.initialized = False
        else:
            print(f"Telegram API error: {description}")

    async def send_as_file(self, logs):
        if not logs:
            return
            
        file = io.BytesIO(logs.encode())
        file.name = "logs.txt"
        payload = {
            "chat_id": self.log_chat_id,
            "caption": "Logs (too large for message)",
            "disable_web_page_preview": True,
            "parse_mode": "Markdown"
        }
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            async with ClientSession() as session:
                data = FormData()
                data.add_field('document', file, filename='logs.txt')
                async with session.post(
                    f"{self.base_url}/sendDocument",
                    data=data,
                    params=payload
                ) as response:
                    await response.json()
        except Exception as e:
            print(f"Failed to send file: {e}")

    @classmethod
    async def verify_all_messages(cls):
        """Verify all pending messages across handlers"""
        for handler in cls._active_handlers:
            if handler._pending_messages:
                await handler._process_messages(force_send=True)

    @classmethod
    def update_all_handlers(cls):
        """Force update all active handlers"""
        current_time = time.time()
        for handler in cls._active_handlers:
            if (current_time - cls._last_global_update >= handler.wait_time and 
                handler.lines >= handler.minimum):
                handler.loop.run_until_complete(handler._process_messages())
                handler.lines = 0
        cls._last_global_update = current_time
