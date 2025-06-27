import contextlib
import io
import time
import asyncio
import nest_asyncio
from logging import StreamHandler
from aiohttp import ClientSession, FormData

nest_asyncio.apply()

DEFAULT_PAYLOAD = {
    "disable_web_page_preview": True,
    "parse_mode": "Markdown"
}

class TelegramLogHandler(StreamHandler):
    """
    Thread-aware Telegram logging handler that preserves message history.
    
    Features:
    - Separate message tracking per thread
    - Automatic file upload for large logs
    - Flood control
    - Markdown formatting
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
        self.messages = ""
        self.floodwait = 0
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        
        # Thread-specific message tracking
        self.thread_messages = {}  # Format: {topic_id: message_id}
        
        # Initialize default payload
        self.default_payload = DEFAULT_PAYLOAD.copy()
        self.default_payload["chat_id"] = self.log_chat_id

    def emit(self, record):
        """Queue log messages for processing"""
        msg = self.format(record)
        self.lines += 1
        self.messages += f"{msg}\n"
        
        # Trigger processing if conditions met
        diff = time.time() - self.last_update
        if diff >= max(self.wait_time, self.floodwait) and self.lines >= self.minimum:
            if self.floodwait:
                self.floodwait = 0
            self.loop.run_until_complete(self._process_logs())
            self.lines = 0
            self.last_update = time.time()

    async def _process_logs(self):
        """Process accumulated logs"""
        if len(self.messages) > self.pending:
            await self._send_oversized_logs()
            return
            
        # Process normal messages
        msg_chunk = self.messages[:4050].rsplit("\n", 1)[0] or self.messages[:4050]
        self.messages = self.messages[len(msg_chunk):]
        
        if not self._get_current_message_id():
            await self._initialize_thread()
        
        await self._send_or_update(msg_chunk)

    async def _send_or_update(self, message):
        """Smart message delivery with thread awareness"""
        current_msg_id = self._get_current_message_id()
        
        # Only edit if we have an existing recent message
        if current_msg_id and (time.time() - self.last_update < 3600):
            await self._edit_message(message, current_msg_id)
        else:
            await self._send_new_message(message)

    async def _send_new_message(self, message):
        """Send a brand new message to the thread"""
        payload = self.default_payload.copy()
        payload["text"] = f"```{message}```"
        
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id
            
        response = await self._send_request(
            f"{self.base_url}/sendMessage",
            payload
        )
        
        if response.get("ok"):
            self._update_message_id(response["result"]["message_id"])

    async def _edit_message(self, message, message_id):
        """Edit an existing message"""
        payload = self.default_payload.copy()
        payload.update({
            "message_id": message_id,
            "text": f"```{message}```"
        })
        
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id
            
        response = await self._send_request(
            f"{self.base_url}/editMessageText",
            payload
        )
        
        # If edit fails, fall back to new message
        if not response.get("ok"):
            await self._send_new_message(message)

    async def _send_oversized_logs(self):
        """Handle logs that exceed size limits"""
        msg = self.messages.rsplit("\n", 1)[0] or self.messages
        self.messages = self.messages[len(msg):]
        
        file = io.BytesIO(msg.encode())
        file.name = "logs.txt"
        
        payload = self.default_payload.copy()
        payload["caption"] = "Logs (exceeded size limit)"
        
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
        
        # Reset message tracking after file upload
        self._update_message_id(None)

    async def _initialize_thread(self):
        """Initialize message tracking for the thread"""
        payload = self.default_payload.copy()
        payload["text"] = "```Logging initialized```"
        
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id
            
        response = await self._send_request(
            f"{self.base_url}/sendMessage",
            payload
        )
        
        if response.get("ok"):
            self._update_message_id(response["result"]["message_id"])

    def _get_current_message_id(self):
        """Get message ID for current thread"""
        return self.thread_messages.get(self.topic_id)

    def _update_message_id(self, message_id):
        """Update message tracking for current thread"""
        if self.topic_id is None:
            return
        self.thread_messages[self.topic_id] = message_id

    async def _send_request(self, url, payload):
        """Universal request handler with error management"""
        async with ClientSession() as session:
            async with session.post(url, json=payload) as response:
                data = await response.json()
                if not data.get("ok"):
                    await self._handle_error(data)
                return data

    async def _handle_error(self, response):
        """Handle API errors"""
        error = response.get("parameters", {})
        
        # Special handling for thread errors
        if response.get("description") == "message thread not found":
            print(f"Thread {self.topic_id} not found - resetting")
            self._update_message_id(None)
            
        # Flood control
        elif error.get("retry_after"):
            self.floodwait = error["retry_after"]
            print(f'Floodwait: {self.floodwait} seconds')
            
        # Other errors
        elif response.get("error_code") == 401:
            print("Invalid bot token")
        else:
            print(f"Telegram API error: {response}")
