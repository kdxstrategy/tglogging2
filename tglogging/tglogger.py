import io
import time
import asyncio
import nest_asyncio
import weakref
from logging import StreamHandler
from aiohttp import ClientSession, FormData, ClientTimeout, ClientError

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}
REQUEST_TIMEOUT = ClientTimeout(total=1)  # максимум 1 секунда ожидания


class TelegramLogHandler(StreamHandler):
    """
    Improved handler to send logs to Telegram chats with thread/topic support.
    Preserves all messages during floodwait and appends new logs to previous message when possible.
    Minimal changes from original structure; fixes:
      - avoid overwriting old logs by not clearing buffer before success
      - handle Connection timeout by returning chunks to queue
      - limit request wait to 1 second
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
        super().__init__()
        # get_event_loop may raise if no loop; user already applied nest_asyncio
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            self.loop = None

        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.wait_time = update_interval
        self.minimum = minimum_lines
        self.pending = pending_logs

        # per-handler buffer and state
        self.message_buffer = []
        self.floodwait = 0
        self.message_id = 0
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        # don't mutate DEFAULT_PAYLOAD globally here
        self.initialized = False
        self.last_sent_content = ""  # Track last successfully sent content
        self._handlers.add(self)

    def emit(self, record):
        msg = self.format(record)
        self.lines += 1
        self.message_buffer.append(msg)

        # Check if we should send immediately due to size
        current_length = len("\n".join(self.message_buffer))
        if current_length >= 3000:
            if not self.floodwait:
                # attempt to call handle_logs synchronously (works with nest_asyncio)
                try:
                    if self.loop and self.loop.is_running():
                        # schedule and wait (since user expects sync behaviour)
                        asyncio.run_coroutine_threadsafe(self.handle_logs(force_send=True), self.loop).result()
                    else:
                        asyncio.get_event_loop().run_until_complete(self.handle_logs(force_send=True))
                except Exception as e:
                    # on any error, keep buffer intact (we didn't clear it)
                    print(f"[TGLogger] send immediate error: {e}")
                self.lines = 0
                self.last_update = time.time()

        # Check if we should send due to interval
        diff = time.time() - self.last_update
        if diff >= max(self.wait_time, self.floodwait) and self.lines >= self.minimum:
            try:
                if self.loop and self.loop.is_running():
                    asyncio.run_coroutine_threadsafe(self.handle_logs(), self.loop).result()
                else:
                    asyncio.get_event_loop().run_until_complete(self.handle_logs())
            except Exception as e:
                # do not drop buffer
                print(f"[TGLogger] periodic send error: {e}")
            self.lines = 0
            self.last_update = time.time()

        # Check all other handlers for pending messages
        current_time = time.time()
        for handler in TelegramLogHandler._handlers:
            if handler is self:
                continue
            if not handler.message_buffer:
                continue

            time_diff = current_time - handler.last_update
            buffer_length = len("\n".join(handler.message_buffer))
            if (
                (time_diff >= max(handler.wait_time, handler.floodwait) and handler.lines >= handler.minimum)
                or buffer_length >= 3000
            ):
                try:
                    if handler.floodwait:
                        handler.floodwait = 0
                    if handler.loop and handler.loop.is_running():
                        asyncio.run_coroutine_threadsafe(handler.handle_logs(), handler.loop).result()
                    else:
                        asyncio.get_event_loop().run_until_complete(handler.handle_logs())
                except Exception as e:
                    print(f"[TGLogger] other handler send error: {e}")
                handler.lines = 0
                handler.last_update = current_time

    async def handle_logs(self, force_send=False):
        # don't touch buffer if floodwait or nothing to send
        if not self.message_buffer or self.floodwait:
            return

        # prepare full_message but DO NOT clear buffer yet
        full_message = "\n".join(self.message_buffer)

        if not full_message.strip():
            return

        # Initialize if needed
        if not self.initialized:
            success = await self.initialize_bot()
            if not success:
                # keep buffer intact: do not remove messages
                return

        # If there is an existing sent message and we can edit it, try to edit.
        # If edit fails (network), DO NOT clear buffer; restore on failure.
        try:
            if self.message_id and len(self.last_sent_content + "\n" + full_message) <= 4096:
                combined_message = self.last_sent_content + "\n" + full_message
                success = await self.edit_message(combined_message)
                if success:
                    # only now we can clear buffer (we successfully merged into previous message)
                    self.last_sent_content = combined_message
                    self.message_buffer = []
                    return
                else:
                    # edit failed: don't clear buffer; try sending as new messages below
                    pass

            # Split into chunks and send one by one.
            chunks = self._split_into_chunks(full_message)
            # We'll try to send all chunks. If any chunk fails due to network/timeout,
            # we restore full_message back to buffer start and stop sending further.
            for chunk in chunks:
                if not chunk.strip():
                    continue
                ok = await self.send_message(chunk)
                if not ok:
                    # send failed (likely timeout or API error). restore full_message to front of buffer
                    # so it will be retried next time; preserve ordering: put it at the front
                    self.message_buffer = [full_message] + self.message_buffer
                    return
            # if all chunks sent successfully — clear buffer
            self.message_buffer = []
        except Exception as e:
            # on unexpected error, restore buffer (do not lose logs)
            print(f"[TGLogger] handle_logs unexpected error: {e}")
            # ensure full_message is at front so it will be retried
            self.message_buffer = [full_message] + self.message_buffer

    def _split_into_chunks(self, message: str):
        """Split message into chunks respecting line boundaries and size limits"""
        chunks = []
        current_chunk = ""
        lines = message.split("\n")

        for line in lines:
            # Preserve empty lines
            if line == "":
                line = " "
            # If line is too long, split it
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
            # Add line to chunk
            if len(current_chunk) + len(line) + 1 <= 4096:
                if current_chunk:
                    current_chunk += "\n" + line
                else:
                    current_chunk = line
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = line
        if current_chunk:
            chunks.append(current_chunk)
        return chunks

    async def initialize_bot(self):
        """Initialize bot connection without sending an initial message"""
        uname, is_alive = await self.verify_bot()
        if not is_alive:
            print("TGLogger: [ERROR] - Invalid bot token")
            return False
        self.initialized = True
        return True

    async def send_request(self, url, payload):
        # use REQUEST_TIMEOUT to limit blocking to 1 second
        try:
            async with ClientSession(timeout=REQUEST_TIMEOUT) as session:
                async with session.post(url, json=payload) as response:
                    return await response.json()
        except asyncio.TimeoutError as e:
            # return standardized response indicating timeout
            return {"ok": False, "description": "Connection timeout"}
        except ClientError as e:
            return {"ok": False, "description": f"ClientError: {e}"}
        except Exception as e:
            return {"ok": False, "description": f"Unexpected: {e}"}

    async def verify_bot(self):
        res = await self.send_request(f"{self.base_url}/getMe", {})
        if res.get("error_code") == 401:
            return None, False
        return res.get("result", {}).get("username"), True

    async def send_message(self, message):
        if not message.strip():
            return False

        payload = DEFAULT_PAYLOAD.copy()
        payload["text"] = f"k-server\n{message}"
        payload["chat_id"] = self.log_chat_id
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/sendMessage", payload)
        if res.get("ok"):
            self.message_id = res["result"]["message_id"]
            self.last_sent_content = message
            return True

        # handle timeout separately: restore to queue handled by caller (we just signal failure)
        desc = res.get("description", "").lower()
        if "timeout" in desc or "timed out" in desc:
            print("[TGLogger] send_message: connection timeout, will requeue")
            return False

        await self.handle_error(res)
        return False

    async def edit_message(self, message):
        if not message.strip() or not self.message_id:
            return False

        if message == self.last_sent_content:
            return True

        payload = DEFAULT_PAYLOAD.copy()
        payload["message_id"] = self.message_id
        payload["text"] = f"k-server\n{message}"
        payload["chat_id"] = self.log_chat_id
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self.send_request(f"{self.base_url}/editMessageText", payload)
        if res.get("ok"):
            self.last_sent_content = message
            return True

        desc = res.get("description", "").lower()
        if "timeout" in desc or "timed out" in desc:
            print("[TGLogger] edit_message: connection timeout, will requeue")
            return False

        await self.handle_error(res)
        return False

    async def send_as_file(self, logs):
        if not logs:
            return

        file = io.BytesIO(f"k-server\n{logs}".encode())
        file.name = "logs.txt"
        payload = DEFAULT_PAYLOAD.copy()
        payload["caption"] = "k-server\nLogs (too large for message)"
        payload["chat_id"] = self.log_chat_id
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            async with ClientSession(timeout=REQUEST_TIMEOUT) as session:
                data = FormData()
                data.add_field("document", file, filename="logs.txt")
                async with session.post(
                    f"{self.base_url}/sendDocument", data=data, params=payload
                ) as response:
                    await response.json()
        except asyncio.TimeoutError:
            print("[TGLogger] send_as_file: timeout, logs preserved")
            # do not drop logs

    async def handle_error(self, resp: dict):
        error = resp.get("parameters", {}) or {}
        error_code = resp.get("error_code")
        description = (resp.get("description", "") or "").lower()

        if "message thread not found" in description:
            print(f"[TGLogger] Thread {self.topic_id} not found — resetting")
            self.message_id = 0
            self.initialized = False
        elif error_code == 429:
            retry_after = error.get("retry_after", 30)
            print(f"[TGLogger] Floodwait {retry_after}s")
            self.floodwait = retry_after
        elif "message to edit not found" in description:
            print("[TGLogger] Message to edit not found — resetting")
            self.message_id = 0
            self.initialized = False
        elif "message is not modified" in description:
            print("[TGLogger] Message not modified, skipping")
        else:
            print(f"[TGLogger] API error: {resp.get('description')}")
