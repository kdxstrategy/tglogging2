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
    Thread-safe handler to send logs to Telegram chats with thread/topic support.
    Uses asyncio locks for proper synchronization.
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
        self.current_msg = ""
        self.floodwait = 0
        self.message_id = 0
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        DEFAULT_PAYLOAD.update({"chat_id": self.log_chat_id})
        self.initialized = False
        self.buffer_lock = asyncio.Lock()  # Using asyncio lock instead of threading

    def emit(self, record):
        msg = self.format(record)
        
        # Run coroutine synchronously in the event loop
        future = asyncio.run_coroutine_threadsafe(
            self._threadsafe_emit(msg), 
            self.loop
        )
        future.result()  # Wait for completion

    async def _threadsafe_emit(self, msg):
        async with self.buffer_lock:
            self.lines += 1
            self.message_buffer.append(msg)
            
            # Check if we should send immediately due to size
            if len('\n'.join(self.message_buffer)) >= 3000:
                await self.handle_logs(force_send=True)
                return
                
        # Check if we should send due to interval
        diff = time.time() - self.last_update
        if diff >= max(self.wait_time, self.floodwait) and self.lines >= self.minimum:
            if self.floodwait:
                self.floodwait = 0
            await self.handle_logs()
            async with self.buffer_lock:
                self.lines = 0
                self.last_update = time.time()

    async def handle_logs(self, force_send=False):
        # Get the current buffer contents in a thread-safe way
        async with self.buffer_lock:
            if not self.message_buffer:
                return
            full_message = '\n'.join(self.message_buffer)
            self.message_buffer = []  # Clear buffer immediately
            
        if not full_message.strip():
            return

        # Initialize if needed
        if not self.initialized:
            success = await self.initialize_bot()
            if not success:
                return

        # Split into chunks that fit Telegram's limits
        chunks = self._split_into_chunks(full_message)
        
        # Send/update messages
        for i, chunk in enumerate(chunks):
            if i == 0 and self.message_id:
                # Try to edit existing message
                success = await self.edit_message(chunk)
                if not success:
                    # If edit fails, send as new message
                    await self.send_message(chunk)
            else:
                # Send new message
                await self.send_message(chunk)
        
        # Store the last chunk for future edits
        self.current_msg = chunks[-1] if chunks else ""

    def _split_into_chunks(self, message):
        """Split message into chunks respecting line boundaries and size limits"""
        chunks = []
        current_chunk = self.current_msg
        
        for line in message.split('\n'):
            if not line:
                continue
                
            if len(current_chunk) + len(line) + 1 <= 3000:  # +1 for newline
                current_chunk = f"{current_chunk}\n{line}" if current_chunk else line
            else:
                chunks.append(current_chunk)
                current_chunk = line
        
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
            self.message_id = res["result"]["message_id"]
            return True
            
        await self.handle_error(res)
        return False

    async def edit_message(self, message):
        if not message or not self.message_id:
            return False
            
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
        else:
            print(f"Telegram API error: {description}")
