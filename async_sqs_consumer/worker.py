from async_sqs_consumer.queue import (
    Queue,
)
from async_sqs_consumer.utils import (
    TASK_NAME_PREFIX,
    validate_message,
)
from async_sqs_consumer.utils.retry import (
    retry_call,
)
import asyncio
from contextlib import (
    suppress,
)
from datetime import (
    datetime,
)
from functools import (
    partial,
)
import json
import logging
import pytz
import signal
import sys
from typing import (
    Any,
    Callable,
    Iterable,
    Literal,
    Optional,
)
from uuid import (
    uuid4,
)

logging.basicConfig(level=logging.INFO)

LOGGER = logging.getLogger(__name__)


class Worker:  # pylint: disable=too-many-instance-attributes
    def __init__(
        self, queues: dict[str, Queue], max_workers: Optional[int] = None
    ) -> None:
        self.queues = queues
        self.running = False
        self.handlers: dict[str, Callable[..., Any]] = {}
        self.handlers_by_queue: dict[str, dict[str, Callable[..., Any]]] = {}
        self._events: dict[str, list[Callable[[], None]]] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._tasks: list[asyncio.Task] = []
        self._max_workers = max_workers or 1024

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
            LOGGER.debug("Stop worker")
            self.running = False
            for queue in self.queues.values():
                queue.stop_polling()
            for event in self._events.get("shutdown", []):
                self._loop.run_until_complete(event())  # type: ignore
            for task in self._tasks:
                task.cancel()
            _queue_tasks = [
                task
                for task in asyncio.all_tasks(self._loop)
                if task.get_name().startswith(TASK_NAME_PREFIX)
            ]
            # TODO: make sure all tasks are finished before finishing the entire process

            self._loop.stop()

    def task(
        self, name: str, queue_name: Optional[str] = None
    ) -> Callable[..., None]:
        """A decorator to add a task that could be handled by the worker.

        Args:
            name (str): tasks received with this name will be
            processed by the decorated function
        Returns:
            Callable[[Callable[[], None]], None]: _description_
        """

        def decorator(func: Callable[[], Any]) -> None:
            if queue_name:
                if queue_name not in self.handlers_by_queue:
                    self.handlers_by_queue[queue_name] = {name: func}
                else:
                    self.handlers_by_queue[queue_name][name] = func
            else:
                self.handlers[name] = func

        return decorator

    def on_event(
        self, event_name: Literal["startup", "shutdown"]
    ) -> Callable[[Callable[[], None]], None]:
        """Yo can define event handlers that need to be executed before
        the application startups, or when the application is shutting down.
        The valid events are `startup`, `shutdown`.

        Args:
            event_name (Literal[&quot;startup&quot;, &quot;shutdown&quot;]):

        Returns:
            Callable[[Callable[[], None]], None]:
        """

        def decorator(func: Callable[[], None]) -> None:
            if event_name not in self._events:
                self._events[event_name] = [func]
            else:
                self._events[event_name] = [*self._events[event_name], func]

        return decorator

    def start(
        self, event_loop: Optional[asyncio.AbstractEventLoop] = None
    ) -> None:
        """Start the workert

        Args:
            event_loop (Optional[asyncio.AbstractEventLoop], optional):
            if an eventloop is already running you can run the worker in it.
            Defaults to None.
        """
        self.running = True
        loop = event_loop or asyncio.get_event_loop()
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

        for queue_name, queue in self.queues.items():
            polling_daemon_task = loop.create_task(
                queue.start_polling(
                    self._consumer_callback,
                    queue_name,
                    max_parallel_messages=self._max_workers,
                )
            )
            self._tasks = [*self._tasks, polling_daemon_task]

        for event in self._events.get("startup", []):
            self._loop.run_until_complete(event())  # type: ignore

        self._loop.run_forever()

    def _finish_message(
        self,
        _future: Optional[asyncio.Future],
        *,
        handler_name: str,
        task_id: str,
        queue_alias: str,
        start_time: float,
        receipt_handle: Optional[Any] = None,
    ) -> None:
        start_date = datetime.fromtimestamp(start_time, pytz.UTC)
        end_date = datetime.utcnow().replace(tzinfo=pytz.UTC)
        total_seconds = end_date - start_date
        if _future.exception() is not None:
            LOGGER.warning(
                "Failed to execute task %s[%s]", handler_name, task_id
            )
        else:
            LOGGER.info(
                "Task %s[%s] succeeded in %ss: None",
                handler_name,
                task_id,
                total_seconds.seconds,
            )
            if receipt_handle:
                self._delete_message(receipt_handle, queue_alias)

    def _delete_message(
        self,
        receipt_handle: Any,
        queue_alias: str,
    ) -> None:
        asyncio.ensure_future(
            self.queues[queue_alias].delete_messages(receipt_handle),
            loop=self._loop,
        )

    async def queue_message_to_worker(
        self,
        *,
        task_id: Optional[str] = None,
        task_name: Optional[str] = None,
        queue_alias: Optional[str] = None,
        retries: Optional[int] = None,
        receipt_handler: Optional[Any] = None,
        args: Optional[Iterable[Any]] = None,
        kwargs: Optional[dict[str, Any]] = None,
    ) -> None:
        task_id = task_id or uuid4().hex
        LOGGER.info("Task %s[%s] received", task_name, task_id)
        handler = self.handlers_by_queue.get(queue_alias, {}).get(task_name)
        handler = handler or self.handlers.get(task_name)
        if not handler:
            LOGGER.warning(
                "The task %s has not a candidate to be proceeded",
                task_name,
            )
            return False

        task = self._loop.create_task(
            retry_call(
                handler,
                fargs=args or [],
                fkwargs=kwargs or {},
                tries=retries or 1,
            ),
            name=(
                f"{TASK_NAME_PREFIX}{queue_alias}"
                f"_{task_name}_{uuid4().hex[:8]}"
            ),
        )
        task.add_done_callback(
            partial(
                self._finish_message,
                receipt_handle=receipt_handler,
                queue_alias=queue_alias,
                start_time=datetime.utcnow().timestamp(),
                handler_name=task_name,
                task_id=task_id,
            )
        )
        return True

    async def _process_message(
        self, message_content: dict[str, Any], queue_alias: str
    ) -> bool:
        try:
            body = json.loads(message_content["Body"])
        except json.JSONDecodeError:
            return False

        if not validate_message(body):
            LOGGER.error("Failed to validate message")
            return False

        await self.queue_message_to_worker(
            task_id=body["id"],
            task_name=body["task"],
            queue_alias=queue_alias,
            retries=body.get("retries", 1),
            receipt_handler=message_content["ReceiptHandle"],
            args=body.get("args", []),
            kwargs=body.get("kwargs", {}),
        )

    async def _consumer_callback(
        self, message: dict[str, Any], queue_alias: str
    ) -> None:
        with suppress(asyncio.CancelledError):
            success = await self._process_message(message, queue_alias)
            if not success:
                await self.queues[queue_alias].delete_messages(
                    message["ReceiptHandle"]
                )
