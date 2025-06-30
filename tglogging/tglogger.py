
import contextlib
import io
import time
import asyncio
import nest_asyncio
import weakref
from logging import StreamHandler
from aiohttp import ClientSession, FormData

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}

class TelegramLogHandler(StreamHandler):
    """
    Improved handler to send logs to Telegram chats with thread/topic support.
    Preserves all messages without deletion and handles both time and size-based triggers.
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
        self.current_msg = ""
        self.floodwait = 0
        self.message_id = 0
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        DEFAULT_PAYLOAD.update({"chat_id": self.log_chat_id})
        self.initialized = False
        self.last_sent_content = ""  # Track last successfully sent content
        
        # Register this handler
        TelegramLogHandler._handlers.add(self)

    def emit(self, record):
        msg = self.format(record)
        self.lines += 1
        self.message_buffer.append(msg)
        
        # Check if we should send immediately due to size
        current_length = len('\n'.join(self.message_buffer))
        if current_length >= 3000:
            self.loop.run_until_complete(self.handle_logs(force_send=True))
            self.lines = 0
            self.last_update = time.time()
            # Continue to check time condition and other handlers
        
        # Check if we should send due to interval
        diff = time.time() - self.last_update
        if diff >= max(self.wait_time, self.floodwait) and self.lines >= self.minimum:
            if self.floodwait:
                self.floodwait = 0
            self.loop.run_until_complete(self.handle_logs())
            self.lines = 0
            self.last_update = time.time()
        
        # Check all other handlers if it's time to send their pending messages
        current_time = time.time()
        for handler in TelegramLogHandler._handlers:
            if handler is self:
                continue  # Skip current handler
            if not handler.message_buffer:
                continue  # Skip handlers with empty buffers
                
            # Check if handler meets flush conditions
            time_diff = current_time - handler.last_update
            if time_diff >= max(handler.wait_time, handler.floodwait) and handler.lines >= handler.minimum:
                if handler.floodwait:
                    handler.floodwait = 0
                handler.loop.run_until_complete(handler.handle_logs())
                handler.lines = 0
                handler.last_update = current_time

    async def handle_logs(self, force_send=False):
        if not self.message_buffer:
            return

        # Combine all pending messages
        full_message = '\n'.join(self.message_buffer)
        self.message_buffer = []  # Clear buffer immediately
        
        if not full_message.strip():
            return

        # Initialize if needed
        if not self.initialized:
            success = await self.initialize_bot()
            if not success:
                return

        # Start with current message and append new content
        if self.current_msg:
            full_message = f"{self.current_msg}\n{full_message}"
            self.current_msg = ""  # Reset after combination

        # Split into chunks that fit Telegram's limits
        chunks = self._split_into_chunks(full_message)
        
        # Send/update messages
        for i, chunk in enumerate(chunks):
            # Skip empty chunks
            if not chunk.strip():
                continue
                
            # For the first chunk, try appending to existing message
            if i == 0 and self.message_id:
                # Combine with existing content
                combined_message = chunk
                
                # Only edit if content has changed
                if combined_message != self.last_sent_content:
                    success = await self.edit_message(combined_message)
                    if success:
                        self.last_sent_content = combined_message
                    else:
                        # If edit fails, send as new message
                        await self.send_message(chunk)
                else:
                    # Skip sending duplicate content
                    self.current_msg = chunk
            else:
                # Send new message
                await self.send_message(chunk)
        
        # Store the last chunk for future appends
        if chunks:
            self.current_msg = chunks[-1]

    def _split_into_chunks(self, message):
        """Split message into chunks respecting line boundaries and size limits"""
        chunks = []
        current_chunk = ""
        
        # Process each line while preserving newlines
        for line in message.split('\n'):
            # Preserve empty lines as they are part of the log format
            if line == "":
                line = " "  # Use space to represent empty line
            
            # If line is too long, split it into smaller parts
            while len(line) > 3000:
                # Find safe split position (last space before 3000)
                split_pos = line[:3000].rfind(' ')
                if split_pos <= 0:
                    split_pos = 3000  # Force split if no space found
                part = line[:split_pos]
                line = line[split_pos:].lstrip()
                
                # Add current part to chunk
                if current_chunk:
                    current_chunk += '\n' + part
                else:
                    current_chunk = part
                    
                # If chunk reached limit, add to chunks
                if len(current_chunk) >= 3000:
                    chunks.append(current_chunk)
                    current_chunk = ""
            
            # Check if we can add the remaining line to current chunk
            if len(current_chunk) + len(line) + 1 <= 3000:
                if current_chunk:
                    current_chunk += '\n' + line
                else:
                    current_chunk = line
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = line
        
        # Add remaining content
        if current_chunk:
            chunks.append(current_chunk)
            
        return chunks

    async def initialize_bot(self):
        """Initialize bot connection and message thread"""
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
            self.last_sent_content = payload["text"]
            return True
        return False

    async def send_message(self, message):
        if not message.strip():
            return False
            
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"```{message}```"
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
            
        # Don't edit if content is identical to last sent
        if message == self.last_sent_content:
            return True
            
        payload = DEFAULT_PAYLOAD.copy()
        payload["message_id"] = self.message_id
        payload["text"] = f"```{message}```"
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
        elif "message is not modified" in description:
            # Treat as success but don't update content
            print("Message not modified (no changes), skipping")
        else:
            print(f"Telegram API error: {description}")
