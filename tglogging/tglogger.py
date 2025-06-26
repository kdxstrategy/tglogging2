import contextlib
import io
import time
import asyncio
import nest_asyncio
from logging import StreamHandler
from aiohttp import ClientSession

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}

class TelegramLogHandler(StreamHandler):
    """
    Handler to send logs to telegram chats with thread/topic support.

    Parameters:
        token: Telegram bot token
        log_chat_id: Chat ID of the target supergroup
        topic_id: Thread/topic ID (optional)
        update_interval: Seconds between posts (min 5 recommended)
        minimum_lines: Minimum new lines required to post/edit
        pending_logs: Max characters before sending as file (default 200000)
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
        diff = time.time() - self.last_update
        if diff >= max(self.wait_time, self.floodwait) and self.lines >= self.minimum:
            if self.floodwait:
                self.floodwait = 0
            self.loop.run_until_complete(self.handle_logs())
            self.lines = 0
            self.last_update = time.time()

    async def handle_logs(self):
        if len(self.messages) > self.pending:
            _msg = self.messages
            msg = _msg.rsplit("\n", 1)[0] or _msg
            self.current_msg = ""
            self.message_id = 0
            self.messages = self.messages[len(msg):]
            await self.send_as_file(msg)
            return

        _message = self.messages[:4050]  # Telegram message length limit
        msg = _message.rsplit("\n", 1)[0] or _message
        letter_count = len(msg)
        self.messages = self.messages[letter_count:]

        if not self.message_id:
            uname, is_alive = await self.verify_bot()
            if not is_alive:
                print("TGLogger: [ERROR] - Invalid bot token")
                return
            await self.initialise()
            await self.edit_message(f"Logging started by @{uname}")

        computed_message = self.current_msg + msg
        if len(computed_message) > 4050:
            _to_edit = computed_message[:4050]
            to_edit = _to_edit.rsplit("\n", 1)[0] or _to_edit
            to_new = computed_message[len(to_edit):]
            if to_edit != self.current_msg:
                await self.edit_message(to_edit)
            self.current_msg = to_new
            await self.send_message(to_new)
        else:
            await self.edit_message(computed_message)
            self.current_msg = computed_message

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
        payload["text"] = "```Initializing```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            self.message_id = res["result"]["message_id"]

    async def send_message(self, message):
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"```{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            self.message_id = res["result"]["message_id"]

    async def edit_message(self, message):
        payload = DEFAULT_PAYLOAD.copy()
        payload["message_id"] = self.message_id
        payload["text"] = f"```{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        await self.send_request(f"{self.base_url}/editMessageText", payload)

    async def send_as_file(self, logs):
        file = io.BytesIO(logs.encode())
        file.name = "logs.txt"
        payload = DEFAULT_PAYLOAD.copy()
        payload["caption"] = "Logs (too large for message)"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        async with ClientSession() as session:
            data = aiohttp.FormData()
            data.add_field('document', file, filename='logs.txt')
            async with session.post(
                f"{self.base_url}/sendDocument",
                data=data,
                params=payload
            ) as response:
                await response.json()

    async def handle_error(self, resp: dict):
        error = resp.get("parameters", {})
        if resp.get("description") == "message thread not found":
            print(f"Thread {self.topic_id} not found - resetting")
            self.message_id = 0
        elif error.get("retry_after"):
            self.floodwait = error["retry_after"]
            print(f'Floodwait: {self.floodwait} seconds')
