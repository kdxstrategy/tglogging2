import contextlib
import io
import time
import asyncio
import nest_asyncio
import re
from logging import StreamHandler, getLogger
from aiohttp import ClientSession, FormData
from collections import defaultdict

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "MarkdownV2"}

class TelegramLogHandler(StreamHandler):
    """
    Robust handler with floodwait recovery and cross-thread message verification.
    Ensures no messages are lost after rate limiting.
    Uses 'k_server' as header for all messages with proper MarkdownV2 escaping.
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

    def _escape_markdown(self, text):
        """Escape special MarkdownV2 characters"""
        escape_chars = r'\_*[]()~`>#+-=|{}.!'
        return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

    async def _safe_send_message(self, message):
        """Send message with k_server header and proper MarkdownV2 escaping"""
        escaped_message = self._escape_markdown(message)
        payload = {
            "chat_id": self.log_chat_id,
            "text": f"*k\_server*\n```\n{escaped_message}\n```",
            "disable_web_page_preview": True,
            "parse_mode": "MarkdownV2"
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
        """Edit message with k_server header and proper MarkdownV2 escaping"""
        if time.time() - self.last_edit_time < 1.0:
            return False
            
        if self.current_msg == message:
            return True

        escaped_message = self._escape_markdown(message)
        payload = {
            "chat_id": self.log_chat_id,
            "message_id": self.message_id,
            "text": f"*k\_server*\n```\n{escaped_message}\n```",
            "disable_web_page_preview": True,
            "parse_mode": "MarkdownV2"
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

    async def initialize_bot(self):
        uname, is_alive = await self.verify_bot()
        if not is_alive:
            print("TGLogger: [ERROR] - Invalid bot token")
            return False
            
        payload = {
            "chat_id": self.log_chat_id,
            "text": "*k\\_server*\n```\nLogging initialized\n```",
            "disable_web_page_preview": True,
            "parse_mode": "MarkdownV2"
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

    async def send_as_file(self, logs):
        if not logs:
            return
            
        file = io.BytesIO(logs.encode())
        file.name = "logs.txt"
        payload = {
            "chat_id": self.log_chat_id,
            "caption": "*k\\_server* - Logs (too large for message)",
            "disable_web_page_preview": True,
            "parse_mode": "MarkdownV2"
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

    # ... [rest of the class methods remain unchanged] ...
