import io
import time
import asyncio
import nest_asyncio
from logging import StreamHandler
from aiohttp import ClientSession, FormData
from collections import deque

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}

class TelegramLogHandler(StreamHandler):
    """
    A non-blocking Telegram log handler that properly handles both synchronous
    logging calls and asynchronous Telegram API operations.
    """

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: int = None,
        update_interval: int = 5,
        minimum_lines: int = 1,
        max_buffer_size: int = 200000,
    ):
        StreamHandler.__init__(self)
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.wait_time = update_interval
        self.minimum = minimum_lines
        self.max_buffer = max_buffer_size
        
        # Message handling state
        self.message_buffer = deque()
        self.current_msg = ""
        self.message_id = 0
        self.last_update = 0
        self.lines_since_last_update = 0
        self.floodwait = 0
        
        # Telegram API configuration
        self.base_url = f"https://api.telegram.org/bot{token}"
        DEFAULT_PAYLOAD.update({"chat_id": self.log_chat_id})
        
        # Asyncio event loop and task management
        self.loop = asyncio.get_event_loop()
        self.initialized = False
        self._shutdown = False
        self._processing_task = None
        self._queue = asyncio.Queue()
        
        # Start the background processor
        self._start_processor()

    def _start_processor(self):
        """Start the background processing task"""
        if not self._processing_task or self._processing_task.done():
            self._processing_task = asyncio.create_task(self._process_messages())

    def emit(self, record):
        """Handle synchronous logging calls"""
        if self._shutdown:
            return
            
        msg = self.format(record)
        
        # Put message in queue without blocking
        try:
            self.loop.call_soon_threadsafe(self._queue.put_nowait, msg)
        except:
            # Fallback if event loop is closed
            pass

    async def _process_messages(self):
        """Background task to process messages"""
        try:
            await self._initialize()
            
            while not self._shutdown:
                try:
                    # Wait for either a message or the update interval
                    try:
                        msg = await asyncio.wait_for(
                            self._queue.get(),
                            timeout=max(0.1, self.wait_time)
                        )
                        self.message_buffer.append(msg)
                        self.lines_since_last_update += 1
                    except asyncio.TimeoutError:
                        pass
                    
                    # Check if we should send messages
                    should_send = (
                        len(self.message_buffer) >= self.minimum or
                        (time.time() - self.last_update) >= self.wait_time or
                        sum(len(m) for m in self.message_buffer) >= 3000
                    )
                    
                    if should_send and self.message_buffer and not self.floodwait:
                        await self._send_buffered_messages()
                        
                except Exception as e:
                    print(f"Error in message processor: {e}")
                    await asyncio.sleep(1)
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Fatal error in message processor: {e}")
        finally:
            # Send any remaining messages before shutdown
            if self.message_buffer:
                await self._send_buffered_messages()

    async def _initialize(self):
        """Initialize the Telegram connection"""
        if self.initialized:
            return True
            
        # Verify bot token
        uname, is_alive = await self._verify_bot()
        if not is_alive:
            print("TGLogger: [ERROR] - Invalid bot token")
            return False
            
        # Send initial message
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = "```Logging initialized```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            res = await self._send_request(f"{self.base_url}/sendMessage", payload)
            if res.get("ok"):
                self.message_id = res["result"]["message_id"]
                self.current_msg = payload["text"]
                self.initialized = True
                return True
        except Exception as e:
            print(f"Initialization failed: {e}")
            
        return False

    async def _send_buffered_messages(self):
        """Send all buffered messages to Telegram"""
        if not self.message_buffer or self.floodwait:
            return
            
        # Combine messages
        messages = []
        while self.message_buffer:
            messages.append(self.message_buffer.popleft())
            
        full_message = '\n'.join(messages)
        
        if not full_message.strip():
            return
            
        # Split into chunks
        chunks = self._split_into_chunks(full_message)
        
        # Send messages
        for i, chunk in enumerate(chunks):
            if i == 0 and self.message_id:
                success = await self._edit_message(chunk)
                if not success:
                    await self._send_message(chunk)
            else:
                await self._send_message(chunk)
                
        self.last_update = time.time()
        self.lines_since_last_update = 0

    def _split_into_chunks(self, message):
        """Split message into Telegram-friendly chunks"""
        chunks = []
        current_chunk = self.current_msg
        
        for line in message.split('\n'):
            if not line:
                continue
                
            if len(current_chunk) + len(line) + 1 <= 3000:
                current_chunk = f"{current_chunk}\n{line}" if current_chunk else line
            else:
                chunks.append(current_chunk)
                current_chunk = line
                
        if current_chunk:
            chunks.append(current_chunk)
            
        self.current_msg = chunks[-1] if chunks else ""
        return chunks

    async def _verify_bot(self):
        """Verify the bot token is valid"""
        try:
            res = await self._send_request(f"{self.base_url}/getMe", {})
            if res.get("error_code") == 401:
                return None, False
            return res.get("result", {}).get("username"), True
        except Exception as e:
            print(f"Bot verification failed: {e}")
            return None, False

    async def _send_request(self, url, payload):
        """Send API request to Telegram"""
        async with ClientSession() as session:
            async with session.post(url, json=payload) as response:
                return await response.json()

    async def _send_message(self, message):
        """Send a new message to Telegram"""
        if not message:
            return False
            
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"```{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            res = await self._send_request(f"{self.base_url}/sendMessage", payload)
            if res.get("ok"):
                self.message_id = res["result"]["message_id"]
                return True
            await self._handle_error(res)
        except Exception as e:
            print(f"Error sending message: {e}")
            
        return False

    async def _edit_message(self, message):
        """Edit an existing message on Telegram"""
        if not message or not self.message_id:
            return False
            
        payload = DEFAULT_PAYLOAD.copy()
        payload["message_id"] = self.message_id
        payload["text"] = f"```{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            res = await self._send_request(f"{self.base_url}/editMessageText", payload)
            if res.get("ok"):
                return True
            await self._handle_error(res)
        except Exception as e:
            print(f"Error editing message: {e}")
            
        return False

    async def _handle_error(self, resp):
        """Handle Telegram API errors"""
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
            await asyncio.sleep(retry_after)
            self.floodwait = 0
        elif "message to edit not found" in description:
            print("Message to edit not found - resetting")
            self.message_id = 0
            self.initialized = False
        else:
            print(f"Telegram API error: {description}")

    def close(self):
        """Clean up resources"""
        self._shutdown = True
        if self._processing_task and not self._processing_task.done():
            self._processing_task.cancel()
        super().close()
