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
    Fixed Telegram logging handler that properly handles message limits
    without replacing previous logs in threads.
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
        self.floodwait = 0
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        
        # Per-thread message tracking
        self.thread_buffers = {}  # {topic_id: {"buffer": "", "message_id": None}}
        self.default_payload = DEFAULT_PAYLOAD.copy()
        self.default_payload["chat_id"] = self.log_chat_id
        
        # Initialize buffer for this thread
        self._get_thread_buffer()

    def _get_thread_buffer(self):
        """Get or create buffer for current thread"""
        buffer_key = self.topic_id or "main"
        if buffer_key not in self.thread_buffers:
            self.thread_buffers[buffer_key] = {
                "buffer": "",
                "message_id": None
            }
        return self.thread_buffers[buffer_key]

    def emit(self, record):
        """Queue log messages for processing"""
        msg = self.format(record)
        self.lines += 1
        
        buffer = self._get_thread_buffer()
        buffer["buffer"] += f"{msg}\n"
        
        # Trigger processing if conditions met
        diff = time.time() - self.last_update
        if diff >= max(self.wait_time, self.floodwait) and self.lines >= self.minimum:
            if self.floodwait:
                self.floodwait = 0
            self.loop.run_until_complete(self._process_logs())
            self.lines = 0
            self.last_update = time.time()

    async def _process_logs(self):
        """Process accumulated logs without replacing existing messages"""
        buffer = self._get_thread_buffer()
        buffer_content = buffer["buffer"]
        
        if not buffer_content:
            return
            
        # Handle oversized logs as file
        if len(buffer_content) > self.pending:
            await self._send_as_file(buffer_content)
            buffer["buffer"] = ""
            buffer["message_id"] = None
            return
            
        # Process all content in the buffer
        while buffer_content:
            # Get current message buffer
            current_msg = buffer["message_id"]
            current_content = buffer["buffer"]
            
            # Calculate available space (Telegram limit is 4096 chars)
            max_chunk = 4096 - len("```\n```")  # Account for Markdown formatting
            
            # Get next chunk that fits in a message
            if len(current_content) <= max_chunk:
                chunk = current_content
                buffer_content = ""
            else:
                # Find last newline within limit to avoid breaking lines
                chunk = current_content[:max_chunk]
                last_newline = chunk.rfind('\n') + 1
                if last_newline > 0:
                    chunk = current_content[:last_newline]
                else:
                    # No newline found - force break
                    chunk = current_content[:max_chunk]
                buffer_content = current_content[len(chunk):]
            
            # Send or update message
            if current_msg and (time.time() - self.last_update < 3600):
                await self._update_message(chunk, current_msg)
            else:
                new_msg_id = await self._send_new_message(chunk)
                buffer["message_id"] = new_msg_id
            
            # Update buffer with remaining content
            buffer["buffer"] = buffer_content

    async def _send_new_message(self, message):
        """Send a new message to the thread and return its ID"""
        payload = self.default_payload.copy()
        payload["text"] = f"```{message}```"
        
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id
            
        response = await self._send_request(
            f"{self.base_url}/sendMessage",
            payload
        )
        
        if response.get("ok"):
            return response["result"]["message_id"]
        return None

    async def _update_message(self, message, message_id):
        """Update existing message with new content"""
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
        
        # If edit fails, create new message
        if not response.get("ok"):
            new_msg_id = await self._send_new_message(message)
            buffer = self._get_thread_buffer()
            buffer["message_id"] = new_msg_id

    async def _send_as_file(self, logs):
        """Send large logs as a text file"""
        file = io.BytesIO(logs.encode())
        file.name = "logs.txt"
        
        payload = self.default_payload.copy()
        payload["caption"] = "Log batch (exceeded size limit)"
        
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id
            
        try:
            async with ClientSession() as session:
                data = FormData()
                data.add_field('document', file, filename='logs.txt')
                await session.post(
                    f"{self.base_url}/sendDocument",
                    data=data,
                    params=payload
                )
        except Exception as e:
            print(f"Failed to send file: {e}")
        
        # Reset b
