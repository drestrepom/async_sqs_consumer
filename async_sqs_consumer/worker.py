from async_sqs_consumer.queue import (
    delete_messages,
    get_queue_messages,
)
from async_sqs_consumer.resources import (
    sqs_shutdown,
    sqs_startup,
)
from async_sqs_consumer.utils import (
    validate_message,
)
from async_sqs_consumer.utils.retry import (
    retry_call,
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
    Callable,
    Literal,
    Optional,
)

logging.basicConfig(level=logging.INFO)

LOGGER = logging.getLogger(__name__)


class Worker:
    def __init__(self, queue_url: str) -> None:
        self.queue_url = queue_url
        self.running = False
        self.handlers: dict[str, Callable[..., Any]] = {}
        self._events: dict[str, list[Callable[[], None]]]
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._main_task: Optional[asyncio.Task] = None

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

    def _stop_worker(self, *_: Any) -> None:
        if self._loop:
            self._loop.create_task(sqs_shutdown())
            LOGGER.debug("Stop worker")
            self.running = False
            for event in self._events.get("shutdown", []):
                self._loop.run_until_complete(event())
            if self._main_task:
                self._main_task.cancel()

    def task(self, task_name: str) -> Callable[[Callable[[], None]], None]:
        def decorator(func: Callable[[], Any]) -> None:
            self.handlers[task_name] = func

        return decorator

    def on_event(
        self, event_name: Literal[""]
    ) -> Callable[[Callable[[], None]], None]:
        def decorator(func: Callable[[], None]) -> None:
            if event_name not in self._events:
                self._events[event_name] = [func]
            else:
                self._events[event_name] = [*self._events[event_name], func]

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
        if self._main_task:
            self._main_task.add_done_callback(partial(self._stop_worker, self))

        for event in self._events.get("startup", []):
            self._loop.run_until_complete(event())

        self._loop.run_until_complete(self._main_task)

    def _delete_message(
        self, _future: asyncio.Future, *, receipt_handle: Any
    ) -> None:
        asyncio.ensure_future(
            delete_messages(self.queue_url, receipt_handle),
            loop=self._loop,
        )

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
        if not self._loop:
            LOGGER.warning("The main event loop is not initialized")
            return
        task = self._loop.create_task(
            retry_call(
                handler,
                fargs=body.get("args", []),
                fkwargs=body.get("kwargs", {}),
                tries=body.get("retries", 1),
            )
        )
        task.add_done_callback(
            partial(
                self._delete_message,
                receipt_handle=message_content["ReceiptHandle"],
            )
        )

    async def consumer(self) -> None:
        while self.running:
            with suppress(asyncio.CancelledError):
                messages = await get_queue_messages(self.queue_url)
                for message in messages:
                    await self._process_message(message)
