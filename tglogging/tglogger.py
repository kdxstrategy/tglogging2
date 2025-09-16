import io
import time
import asyncio
import nest_asyncio
import weakref
from logging import StreamHandler
from aiohttp import ClientSession, FormData, ClientConnectionError, ClientResponseError

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
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.wait_time = update_interval
        self.minimum = minimum_lines
        self.pending = pending_logs
        self.message_buffer = []
        self.floodwait = 0
        self.message_id = 0
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        DEFAULT_PAYLOAD.update({"chat_id": self.log_chat_id})
        self.initialized = False
        self.last_sent_content = ""  # Track last successfully sent content
        self._handlers.add(self)
        self._task = None

    def emit(self, record):
        msg = self.format(record)
        self.lines += 1
        self.message_buffer.append(msg)
        
        # Check if we should send immediately due to size
        current_length = len('\n'.join(self.message_buffer))
        current_time = time.time()
        if current_length >= 3000:
            if not self.floodwait:  # Only send if not in floodwait
                self._schedule_send(current_time)
            # Buffer is preserved during floodwait
        
        # Check if we should send due to interval
        diff = current_time - self.last_update
        if diff >= max(self.wait_time, self.floodwait) and self.lines >= self.minimum:
            if self.floodwait:
                self.floodwait = 0  # Reset floodwait after waiting
            self._schedule_send(current_time)
        
        # Check all other handlers for pending messages
        for handler in TelegramLogHandler._handlers:
            if handler is self:
                continue
            if not handler.message_buffer:
                continue
                
            time_diff = current_time - handler.last_update
            buffer_length = len('\n'.join(handler.message_buffer))
            if (time_diff >= max(handler.wait_time, handler.floodwait) and 
                handler.lines >= handler.minimum) or buffer_length >= 3000:
                if handler.floodwait:
                    handler.floodwait = 0
                handler._schedule_send(current_time)

    def _schedule_send(self, current_time):
        """Schedule async send operation without blocking"""
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self.handle_logs())
        self.lines = 0
        self.last_update = current_time

    async def handle_logs(self, force_send=False):
        if not self.message_buffer or self.floodwait:
            return

        full_message = '\n'.join(self.message_buffer)
        self.message_buffer = []  # Clear buffer only after checking floodwait
        
        if not full_message.strip():
            return

        # Initialize if needed
        if not self.initialized:
            success = await self._safe_initialize_bot()
            if not success:
                self.message_buffer = [full_message]  # Restore buffer if init fails
                return

        # Check if we can append to the existing message
        if self.message_id and len(self.last_sent_content + '\n' + full_message) <= 4096:
            combined_message = self.last_sent_content + '\n' + full_message
            success = await self._safe_edit_message(combined_message)
            if success:
                self.last_sent_content = combined_message
            else:
                # If edit fails, send as new message
                await self._safe_send_message(full_message)
        else:
            # Send as new message(s)
            chunks = self._split_into_chunks(full_message)
            for chunk in chunks:
                if chunk.strip():
                    await self._safe_send_message(chunk)

    def _split_into_chunks(self, message):
        """Split message into chunks respecting line boundaries and size limits"""
        chunks = []
        current_chunk = ""
        lines = message.split('\n')
        
        for line in lines:
            # Preserve empty lines
            if line == "":
                line = " "
            
            # If line is too long, split it
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
            
            # Add line to chunk
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

    async def _safe_initialize_bot(self):
        """Initialize bot connection without sending an initial message"""
        try:
            uname, is_alive = await self.verify_bot()
            if not is_alive:
                print("TGLogger: [ERROR] - Invalid bot token")
                return False
            self.initialized = True
            return True
        except (ClientConnectionError, asyncio.TimeoutError) as e:
            print(f"Initialization failed: {str(e)}")
            return False

    async def send_request(self, url, payload):
        async with ClientSession() as session:
            try:
                async with session.post(url, json=payload, timeout=10) as response:
                    return await response.json()
            except (ClientConnectionError, asyncio.TimeoutError) as e:
                print(f"Connection error: {str(e)}")
                return {"error_code": 503, "description": str(e)}
            except ClientResponseError as e:
                return {"error_code": e.status, "description": str(e)}

    async def verify_bot(self):
        res = await self.send_request(f"{self.base_url}/getMe", {})
        if res.get("error_code") == 401:
            return None, False
        return res.get("result", {}).get("username"), True

    async def _safe_send_message(self, message):
        if not message.strip():
            return False
            
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"```k-server\n{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            res = await self.send_request(f"{self.base_url}/sendMessage", payload)
            if res.get("ok"):
                self.message_id = res["result"]["message_id"]
                self.last_sent_content = message
                return True
            else:
                self.message_buffer = [message] + self.message_buffer
                await self.handle_error(res)
                return False
        except Exception as e:
            print(f"Send message failed: {str(e)}")
            self.message_buffer = [message] + self.message_buffer
            return False

    async def _safe_edit_message(self, message):
        if not message.strip() or not self.message_id:
            return False
            
        if message == self.last_sent_content:
            return True
            
        payload = DEFAULT_PAYLOAD.copy()
        payload["message_id"] = self.message_id
        payload["text"] = f"```k-server\n{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            res = await self.send_request(f"{self.base_url}/editMessageText", payload)
            if res.get("ok"):
                self.last_sent_content = message
                return True
            else:
                self.message_buffer = [message] + self.message_buffer
                await self.handle_error(res)
                return False
        except Exception as e:
            print(f"Edit message failed: {str(e)}")
            self.message_buffer = [message] + self.message_buffer
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
                    timeout=10
                ) as response:
                    await response.json()
        except Exception as e:
            print(f"Send file failed: {str(e)}")
            self.message_buffer = [logs] + self.message_buffer

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
            # Buffer is preserved, no need to restore here
        elif error_code == 503 or "timeout" in description.lower():
            print(f"Connection timeout: {description}")
        elif "message to edit not found" in description:
            print("Message to edit not found - resetting")
            self.message_id = 0
            self.initialized = False
        elif "message is not modified" in description:
            print("Message not modified (no changes), skipping")
        else:
            print(f"Telegram API error: {description}")

    def close(self):
        if self._task and not self._task.done():
            self._task.cancel()
        super().close()
