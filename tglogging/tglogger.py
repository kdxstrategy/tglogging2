import io
import time
import asyncio
import threading
import weakref
from collections import deque
from logging import StreamHandler
from aiohttp import ClientSession, FormData, ClientTimeout

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    """
    Universal async/sync handler to send logs to Telegram chats with thread/topic support.
    - Each topic has its own buffer and message_id
    - Logs are queued and never lost on timeout
    - Floodwait is respected
    - Works both with synchronous and asynchronous code
    """

    _handlers = weakref.WeakSet()
    _worker_started = False
    _lock = threading.Lock()

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
        self.token = token
        self.chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.update_interval = update_interval
        self.minimum_lines = minimum_lines
        self.pending_logs = pending_logs

        self.base_url = f"https://api.telegram.org/bot{token}"
        self.queue = deque(maxlen=pending_logs)  # очередь логов
        self.last_update = 0
        self.floodwait = 0
        self.message_id = 0
        self.last_sent_content = ""
        self.initialized = False

        self._handlers.add(self)

        # Запустить фонового воркера один раз на все хендлеры
        with TelegramLogHandler._lock:
            if not TelegramLogHandler._worker_started:
                TelegramLogHandler._start_worker()
                TelegramLogHandler._worker_started = True

    # ---------------- PUBLIC ---------------- #

    def emit(self, record):
        """Кладёт сообщение в очередь"""
        msg = self.format(record)
        self.queue.append(msg)

    # ---------------- WORKER ---------------- #

    @classmethod
    def _start_worker(cls):
        """Запускает фонового воркера в отдельном потоке"""

        def runner():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.create_task(cls._worker_loop())
            loop.run_forever()

        threading.Thread(target=runner, daemon=True).start()

    @classmethod
    async def _worker_loop(cls):
        """Фоновая задача, опрашивающая все хендлеры"""
        while True:
            tasks = [h._flush() for h in list(cls._handlers)]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(1)

    async def _flush(self):
        """Отправка накопленных логов"""
        if self.floodwait > 0:
            self.floodwait -= 1
            return
        if not self.queue:
            return

        now = time.time()
        if now - self.last_update < self.update_interval and len(self.queue) < self.minimum_lines:
            return

        # собрать сообщение
        logs = []
        while self.queue and len("\n".join(logs)) < 3000:
            logs.append(self.queue.popleft())
        message = "\n".join(logs)

        if not message.strip():
            return

        # отправить
        if not self.initialized:
            ok = await self._initialize()
            if not ok:
                self.queue.extendleft(reversed(logs))
                return

        # попытка апдейта или нового сообщения
        if self.message_id and len(self.last_sent_content + "\n" + message) <= 4096:
            combined = self.last_sent_content + "\n" + message
            ok = await self._edit_message(combined)
            if ok:
                self.last_sent_content = combined
            else:
                await self._send_message(message)
        else:
            for chunk in self._split_chunks(message):
                ok = await self._send_message(chunk)
                if not ok:
                    # вернуть обратно
                    self.queue.appendleft(chunk)
                    break

        self.last_update = now

    # ---------------- TELEGRAM API ---------------- #

    async def _initialize(self):
        res = await self._request("getMe", {})
        if not res.get("ok"):
            print("TGLogger: bad token")
            return False
        self.initialized = True
        return True

    async def _request(self, method, payload, as_form=False, file=None):
        url = f"{self.base_url}/{method}"
        timeout = ClientTimeout(total=5)
        try:
            async with ClientSession(timeout=timeout) as session:
                if as_form:
                    data = FormData()
                    data.add_field("document", file, filename="logs.txt")
                    async with session.post(url, data=data, params=payload) as r:
                        return await r.json()
                else:
                    async with session.post(url, json=payload) as r:
                        return await r.json()
        except Exception as e:
            # вернуть ошибку, но не падать
            return {"ok": False, "description": str(e)}

    async def _send_message(self, text):
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.chat_id
        payload["text"] = f"```logs\n{text}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self._request("sendMessage", payload)
        if res.get("ok"):
            self.message_id = res["result"]["message_id"]
            self.last_sent_content = text
            return True
        return await self._handle_error(res, text)

    async def _edit_message(self, text):
        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.chat_id
        payload["message_id"] = self.message_id
        payload["text"] = f"```logs\n{text}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        res = await self._request("editMessageText", payload)
        if res.get("ok"):
            self.last_sent_content = text
            return True
        return await self._handle_error(res, text)

    async def _handle_error(self, res, failed_message):
        desc = res.get("description", "")
        code = res.get("error_code")

        if "timeout" in desc.lower():
            # вернуть сообщение обратно
            self.queue.appendleft(failed_message)
            return False
        if code == 429:
            retry_after = res.get("parameters", {}).get("retry_after", 5)
            self.floodwait = retry_after
            self.queue.appendleft(failed_message)
            return False
        if "message to edit not found" in desc:
            self.message_id = 0
            return False
        if "not modified" in desc:
            return True

        print(f"TGLogger error: {desc}")
        return False

    def _split_chunks(self, text):
        lines = text.split("\n")
        chunk, chunks = "", []
        for line in lines:
            if len(chunk) + len(line) + 1 > 4000:
                chunks.append(chunk)
                chunk = ""
            chunk += ("\n" if chunk else "") + line
        if chunk:
            chunks.append(chunk)
        return chunks
