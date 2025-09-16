import io
import time
import asyncio
import nest_asyncio
import weakref
from logging import StreamHandler
from aiohttp import ClientSession, FormData, ClientTimeout, ClientError

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}
REQUEST_TIMEOUT = ClientTimeout(total=1)  # максимум 1 секунда

class TelegramLogHandler(StreamHandler):
    """
    Handler для отправки логов в Telegram с поддержкой топиков.
    - Логи копятся в буфере для каждого топика отдельно
    - Сообщения отправляются чанками
    - При timeout логи возвращаются в очередь
    - Не блокирует синхронный код
    """
    _handlers = weakref.WeakSet()

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: int = None,
        update_interval: int = 5,
        minimum_lines: int = 1,
        pending_logs: int = 200000,
    ):
        super().__init__()
        self.loop = asyncio.get_event_loop()
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.wait_time = update_interval
        self.minimum = minimum_lines
        self.pending = pending_logs

        self.buffers = {}  # (chat_id, topic_id) -> список строк
        self.sent_messages = {}  # (chat_id, topic_id) -> список {id, content}
        self.floodwait = 0
        self.lines = {}
        self.last_update = {}
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.initialized = False

        self._handlers.add(self)

    def _key(self):
        return (self.log_chat_id, self.topic_id)

    def emit(self, record):
        msg = self.format(record)
        key = self._key()
        self.buffers.setdefault(key, []).append(msg)
        self.lines[key] = self.lines.get(key, 0) + 1

        now = time.time()
        last_upd = self.last_update.get(key, 0)
        diff = now - last_upd
        buffer_length = len("\n".join(self.buffers[key]))

        if buffer_length >= 3000 or (diff >= max(self.wait_time, self.floodwait) and self.lines[key] >= self.minimum):
            if self.floodwait:
                self.floodwait = 0
            fut = asyncio.run_coroutine_threadsafe(self.handle_logs(key), self.loop)
            try:
                fut.result()
            except Exception as e:
                print(f"TGLogger emit error: {e}")
            self.lines[key] = 0
            self.last_update[key] = now

    async def handle_logs(self, key, force_send=False):
        if key not in self.buffers or not self.buffers[key] or self.floodwait:
            return

        full_message = "\n".join(self.buffers[key])
        self.buffers[key] = []

        if not full_message.strip():
            return

        if not self.initialized:
            success = await self.initialize_bot()
            if not success:
                self.buffers[key] = [full_message] + self.buffers.get(key, [])
                return

        messages = self.sent_messages.setdefault(key, [])
        last_msg = messages[-1] if messages else None

        if last_msg and len(last_msg["content"] + "\n" + full_message) <= 4096:
            combined = last_msg["content"] + "\n" + full_message
            success = await self.edit_message(key, last_msg["id"], combined)
            if success:
                last_msg["content"] = combined
                return
            # если edit не удался — отправляем новым сообщением
        chunks = self._split_into_chunks(full_message)
        for chunk in chunks:
            if chunk.strip():
                msg_id = await self.send_message(key, chunk)
                if msg_id:
                    messages.append({"id": msg_id, "content": chunk})

    def _split_into_chunks(self, message):
        chunks, current_chunk = [], ""
        for line in message.split("\n"):
            if not line:
                line = " "
            while len(line) > 4096:
                split_pos = line[:4096].rfind(" ")
                if split_pos <= 0:
                    split_pos = 4096
                part = line[:split_pos]
                line = line[split_pos:].lstrip()
                if current_chunk:
                    current_chunk += "\n" + part
                else:
                    current_chunk = part
                if len(current_chunk) >= 4096:
                    chunks.append(current_chunk)
                    current_chunk = ""
            if len(current_chunk) + len(line) + 1 <= 4096:
                current_chunk = (current_chunk + "\n" + line) if current_chunk else line
            else:
                chunks.append(current_chunk)
                current_chunk = line
        if current_chunk:
            chunks.append(current_chunk)
        return chunks

    async def initialize_bot(self):
        uname, is_alive = await self.verify_bot()
        if not is_alive:
            print("TGLogger: [ERROR] - Invalid bot token")
            return False
        self.initialized = True
        return True

    async def send_request(self, url, payload):
        try:
            async with ClientSession(timeout=REQUEST_TIMEOUT) as session:
                async with session.post(url, json=payload) as resp:
                    return await resp.json()
        except (asyncio.TimeoutError, ClientError):
            return {"ok": False, "description": "Connection timeout"}

    async def verify_bot(self):
        res = await self.send_request(f"{self.base_url}/getMe", {})
        if res.get("error_code") == 401:
            return None, False
        return res.get("result", {}).get("username"), True

    async def send_message(self, key, message):
        if not message.strip():
            return None
        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"```k-server\n{message}```"
        payload["chat_id"] = key[0]
        if key[1]:
            payload["message_thread_id"] = key[1]

        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            return res["result"]["message_id"]

        if res.get("description") == "Connection timeout":
            self.buffers[key] = [message] + self.buffers.get(key, [])
        else:
            await self.handle_error(res, key)
        return None

    async def edit_message(self, key, message_id, message):
        if not message.strip():
            return False
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = key[0]
        payload["message_id"] = message_id
        payload["text"] = f"```k-server\n{message}```"
        if key[1]:
            payload["message_thread_id"] = key[1]

        res = await self.send_request(f"{self.base_url}/editMessageText", payload)
        if res.get("ok"):
            return True

        if res.get("description") == "Connection timeout":
            self.buffers[key] = [message] + self.buffers.get(key, [])
        else:
            await self.handle_error(res, key)
        return False

    async def handle_error(self, resp: dict, key):
        error = resp.get("parameters", {})
        error_code = resp.get("error_code")
        desc = resp.get("description", "")

        if desc == "message thread not found":
            print(f"Thread {key[1]} not found - resetting")
        elif error_code == 429:
            retry_after = error.get("retry_after", 30)
            print(f"Floodwait: {retry_after}s")
            self.floodwait = retry_after
        elif "message to edit not found" in desc:
            print("Message to edit not found - resetting")
        elif "message is not modified" in desc:
            print("Message not modified, skipping")
        else:
            print(f"Telegram API error: {desc}")
