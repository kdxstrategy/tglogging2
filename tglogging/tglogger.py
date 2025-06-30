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
    Final improved version with:
    - Proper handler registration
    - Non-blocking floodwait handling
    - Message queuing during floodwait
    - Automatic retry on next log call
    - All previous functionality preserved
    """

    _last_global_update = 0
    _active_handlers = []

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
        self.floodwait_until = 0  # Using timestamp instead of sleep
        self.message_id = 0
        self.lines = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.initialized = False
        self.last_edit_time = 0
        self.sent_messages = {}
        self.pending_messages = []
        self.retry_count = 0
        
        DEFAULT_PAYLOAD.update({"chat_id": self.log_chat_id})
        self._register_handler()

    def _register_handler(self):
        """Register this handler in the class's active handlers list"""
        TelegramLogHandler._active_handlers.append(self)

    def emit(self, record):
        msg = self.format(record)
        self.lines += msg.count('\n') + 1
        self.message_buffer.append(msg)
        
        # Immediate send if buffer too large
        if len('\n'.join(self.message_buffer)) >= 3000:
            self.loop.run_until_complete(self.handle_logs(force_send=True))
            return
            
        current_time = time.time()
        if (current_time - TelegramLogHandler._last_global_update >= self.wait_time and 
            self.lines >= self.minimum):
            self.loop.run_until_complete(self.handle_logs())
            self.lines = 0
            TelegramLogHandler._last_global_update = current_time

    async def handle_logs(self, force_send=False):
        if not self.message_buffer and not self.pending_messages:
            return

        # Check floodwait status first
        if time.time() < self.floodwait_until:
            if self.message_buffer:
                self.pending_messages.extend(self.message_buffer)
                self.message_buffer = []
            return

        # Process new messages and any pending ones
        all_messages = self.message_buffer + self.pending_messages
        self.message_buffer = []
        self.pending_messages = []
        
        new_messages = '\n'.join(all_messages)
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
                        await self.send_message(chunk)
            else:
                await self.send_message(chunk)
        
        self.current_msg = chunks[-1] if chunks else ""

    async def send_message(self, message):
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

    async def handle_error(self, resp: dict, original_message=None):
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
