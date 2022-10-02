from async_sqs_consumer.queue import (
    get_queue_messages,
)
from async_sqs_consumer.resources import (
    sqs_shutdown,
    sqs_startup,
)
from async_sqs_consumer.utils import (
    validate_message,
)
import asyncio
from contextlib import (
    suppress,
)
from functools import (
    partial,
)
import json
import logging
import signal
import sys
from typing import (
    Any,
    Optional,
)

logging.basicConfig(level=logging.INFO)

LOGGER = logging.getLogger(__name__)


class Worker:
    def __init__(self, queue_url: str) -> None:
        self.queue_url = queue_url
        self.running = False
        self.handlers = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._main_task = None

    def _sigint_handler(self, *_args: Any) -> None:
        sys.stdout.write("\b\b\r")
        sys.stdout.flush()
        logging.getLogger("system").warning(
            "Received <ctrl+c> interrupt [SIGINT]"
        )
        self._stop_worker()

    def _sigterm_handler(self, *_args: Any) -> None:
        logging.getLogger("system").warning(
            "Received termination signal [SIGTERM]"
        )
        self._stop_worker()

    def _stop_worker(self, *_) -> None:
        self._loop.create_task(sqs_shutdown())
        LOGGER.debug("Stop worker")
        self.running = False

        self._main_task.cancel()

    def task(self, task_name: str):
        def decorator(func):
            self.handlers[task_name] = func

        return decorator

    def start(self) -> None:
        self.running = True
        loop = asyncio.get_event_loop()
        self._loop = loop

        if self._loop and self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

        for signame in ("SIGINT", "SIGTERM"):
            self._loop.add_signal_handler(
                getattr(signal, signame), partial(self._stop_worker, self)
            )

        signal.siginterrupt(signal.SIGTERM, False)
        signal.siginterrupt(signal.SIGUSR1, False)
        signal.signal(signal.SIGINT, partial(self._sigint_handler, self))
        signal.signal(signal.SIGTERM, partial(self._sigterm_handler, self))

        self._loop.run_until_complete(sqs_startup())
        self._main_task = loop.create_task(self.consumer())
        self._main_task.add_done_callback(partial(self._stop_worker, self))
        self._loop.run_until_complete(self._main_task)

    async def _process_message(self, message_content: dict[str, Any]) -> None:
        try:
            body = json.loads(message_content["Body"])
        except json.JSONDecodeError:
            return

        if not validate_message(body):
            LOGGER.error("Failed to validate message")
            return

        handler = self.handlers.get(body["task"])
        if not handler:
            LOGGER.warning(
                "The task %s has not a candidate to be proceeded",
                body["task_name"],
            )
            return
        task = self._loop.create_task(
            handler(*body.get("args", []), **body.get("kwargs", {}))
        )

    async def consumer(self) -> None:
        while self.running:
            with suppress(asyncio.CancelledError):
                messages = await get_queue_messages(self.queue_url)
                for message in messages:
                    await self._process_message(message)
