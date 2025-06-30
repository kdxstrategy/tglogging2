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
    Fixed handler to send logs to telegram chats with thread/topic support.
    Preserves previous messages when reaching maximum length.
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
        self.current_msg = ""
        self.floodwait = 0
        self.message_id = 0
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        DEFAULT_PAYLOAD.update({"chat_id": self.log_chat_id})

    def emit(self, record):
        msg = self.format(record)
        self.lines += 1
        self.messages += f"{msg}\n"
        
        # Check if we should send immediately due to size
        if len(self.messages) >= 3000:
            self.loop.run_until_complete(self.handle_logs(force_send=True))
            return
            
        # Check if we should send due to interval
        diff = time.time() - self.last_update
        if diff >= max(self.wait_time, self.floodwait) and self.lines >= self.minimum:
            if self.floodwait:
                self.floodwait = 0
            self.loop.run_until_complete(self.handle_logs())
            self.lines = 0
            self.last_update = time.time()

    async def handle_logs(self, force_send=False):
        # Handle very large messages first
        if len(self.messages) > self.pending:
            _msg = self.messages
            msg = _msg.rsplit("\n", 1)[0] or _msg
            self.current_msg = ""
            self.message_id = 0
            self.messages = self.messages[len(msg):]
            await self.send_as_file(msg)
            return

        # Process messages in chunks while respecting Telegram's limits
        while self.messages:
            # Get next chunk (up to 3000 characters)
            _message = self.messages[:3000]
            msg = _message.rsplit("\n", 1)[0] or _message
            letter_count = len(msg)
            
            # Remove processed part from buffer
            self.messages = self.messages[letter_count:]
            
            # Skip empty messages
            if not msg:
                break

            # Initialize if needed
            if not self.message_id:
                uname, is_alive = await self.verify_bot()
                if not is_alive:
                    print("TGLogger: [ERROR] - Invalid bot token")
                    return
                await self.initialise()
                # Don't add initial message to logs
                self.current_msg = ""
                continue  # Continue processing messages

            # Handle message composition
            computed_message = self.current_msg + msg
            
            # FIX: When exceeding limit, finalize current message and start new one
            if len(computed_message) > 3000:
                # Send current message as is
                if self.current_msg:
                    await self.edit_message(self.current_msg)
                
                # Start new message with the current content
                self.current_msg = msg
                await self.send_message(msg)
            else:
                self.current_msg = computed_message
                await self.edit_message(computed_message)
                
            # If not forcing a full send, break after one chunk
            if not force_send:
                break

    # ... (keep all other methods unchanged: send_request, verify_bot, initialise, 
    # send_message, edit_message, send_as_file, handle_error)

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

    def shutdown(self):
        """Ensure all logs are sent before destruction"""
        # Signal worker to process any remaining messages
        with self.processing_lock:
            if self.message_queue:
                self.send_event.set()
        
        # Give worker time to finish
        self.worker_thread.join(timeout=5.0)
        
        # Close the event loop
        self.loop.call_soon_threadsafe(self.loop.stop)
        if self.loop.is_running():
            self.loop.close()
