from .queue import (
    Queue,
)
from .resources import (
    RESOURCE_OPTIONS_SQS,
    SESSION,
)
from .types import (
    MessageType,
)
from .utils import (
    inverse_proportional_distribution,
    TASK_NAME_PREFIX,
    validate_message,
)
from .utils.retry import (
    retry_call,
)
import asyncio
from contextlib import (
    suppress,
)
from datetime import (
    datetime,
    timezone,
)
from functools import (
    partial,
)
import json
import logging
import math
import signal
import sys
from typing import (
    Any,
    Callable,
    Literal,
)
from uuid import (
    uuid4,
)

logging.basicConfig(level=logging.INFO)

LOGGER = logging.getLogger(__name__)


class Worker:
    def __init__(
        self, queues: dict[str, Queue], max_workers: int | None = None
    ) -> None:
        self.queues = queues
        self.running = False
        self.handlers_by_queue: dict[
            str, dict[str, Callable[..., object]]
        ] = {}
        self._events: dict[str, list[Callable[[], None]]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tasks: list[asyncio.Task] = []
        self._max_workers = max_workers or 1024

    def _sigint_handler(self, *_args: object) -> None:
        sys.stdout.write("\b\b\r")
        sys.stdout.flush()
        logging.getLogger("system").warning(
            "Received <ctrl+c> interrupt [SIGINT]"
        )
        self._stop_worker()

    def _sigterm_handler(self, *_args: object) -> None:
        logging.getLogger("system").warning(
            "Received termination signal [SIGTERM]"
        )
        self._stop_worker()

    def _stop_worker(self, *_: object) -> None:
        if self._loop:
            LOGGER.debug("Stop worker")
            self.running = False
            for queue in self.queues.values():
                queue.stop_polling()
            for event in self._events.get("shutdown", []):
                self._loop.run_until_complete(event())  # type: ignore

            map(lambda task: task.cancel(), self._tasks)

            # make sure all tasks are finished before finishing the
            # entire process
            self._loop.stop()

    def task(
        self, name: str, queue_name: str | None = None
    ) -> Callable[..., None]:
        """A decorator to add a task that could be handled by the worker.

        Args:
            name (str): tasks received with this name will be
            processed by the decorated function
        Returns:
            Callable[[Callable[[], None]], None]: _description_
        """

        queue_name = queue_name or "default"

        def decorator(func: Callable[[], object]) -> None:
            queue_ = queue_name or "default"
            if queue_ not in self.handlers_by_queue:
                self.handlers_by_queue[queue_] = {name: func}
            else:
                self.handlers_by_queue[queue_][name] = func

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
        self, event_loop: asyncio.AbstractEventLoop | None = None
    ) -> None:
        """Start the workert

        Args:
            event_loop (asyncio.AbstractEventLoop  | None, optional):
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

        for event in self._events.get("startup", []):
            self._loop.run_until_complete(event())  # type: ignore
        loop.create_task(self._poll_messages())
        self._loop.run_forever()

    def _finish_message(
        self,
        _future: asyncio.Future | None,
        *,
        handler_name: str,
        task_id: str,
        queue_alias: str,
        start_time: float,
        receipt_handle: object | None = None,
    ) -> None:
        start_date = datetime.fromtimestamp(start_time, tz=timezone.utc)
        end_date = datetime.now(tz=timezone.utc)
        total_seconds = end_date - start_date
        if _future and _future.exception() is not None:
            LOGGER.warning(
                "Failed to execute task %s[%s]", handler_name, task_id
            )
            LOGGER.exception(
                "Following exception",
                exc_info=_future.exception(),
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
        receipt_handle: object,
        queue_alias: str,
    ) -> None:
        asyncio.ensure_future(
            self.queues[queue_alias].delete_messages(
                receipt_handle  # type: ignore[arg-type]
            ),
            loop=self._loop,
        )

    def _available_workers_to_consume(self) -> int:
        current_tasks = [
            task
            for task in asyncio.all_tasks(asyncio.get_event_loop())
            if task.get_name().startswith(TASK_NAME_PREFIX)
        ]
        return self._max_workers - len(current_tasks)

    def _get_tasks_by_queue(
        self, queue_alias: str
    ) -> tuple[asyncio.Task, ...]:
        return tuple(
            task
            for task in asyncio.all_tasks(asyncio.get_event_loop())
            if task.get_name().startswith(f"{TASK_NAME_PREFIX}_{queue_alias}")
        )

    async def queue_message_to_worker(
        self,
        *,
        body: dict[str, Any],
        message_type: MessageType,
        queue_alias: str,
        receipt_handler: str,
    ) -> bool:
        task_id = body.get("id") or uuid4().hex
        task_name = body.get("task") or message_type.value.lower()
        LOGGER.info("Task %s[%s] received", task_name, task_id)

        if not self._loop:
            return False

        handler = self.handlers_by_queue.get(queue_alias, {}).get(task_name)
        if not handler:
            LOGGER.warning(
                "The task %s has not a candidate to be proceeded",
                task_name,
            )
            return False

        task = self._loop.create_task(
            retry_call(
                handler,
                fargs=tuple(body.get("args", [])),
                fkwargs=(
                    body
                    if message_type == MessageType.SNS_NOTIFICATION
                    else body.get("kwargs", {})
                ),
                tries=body.get("retries", 1),
            ),
            name=(
                f"{TASK_NAME_PREFIX}_{queue_alias}"
                f"_{task_name}_{uuid4().hex[:8]}"
            ),
        )
        task.add_done_callback(
            partial(
                self._finish_message,
                receipt_handle=receipt_handler,
                queue_alias=queue_alias,
                start_time=datetime.now().timestamp(),
                handler_name=task_name,
                task_id=task_id,
            )
        )
        return True

    async def _process_queue_message(
        self, message_content: dict[str, Any], queue_alias: str
    ) -> bool:
        try:
            body = json.loads(message_content["Body"])
        except json.JSONDecodeError:
            return False

        message_type = (
            MessageType.SNS_NOTIFICATION
            if body.get("Type") == "Notification"
            else MessageType.CUSTOM
        )
        if not validate_message(body, message_type):
            LOGGER.error("Failed to validate message")
            return False

        await self.queue_message_to_worker(
            body=body,
            message_type=message_type,
            queue_alias=queue_alias,
            receipt_handler=message_content["ReceiptHandle"],
        )

        return True

    async def _consumer_callback(
        self, message: dict[str, Any], queue_alias: str
    ) -> None:
        with suppress(asyncio.CancelledError):
            success = await self._process_queue_message(message, queue_alias)
            if not success:
                await self.queues[queue_alias].delete_messages(
                    message["ReceiptHandle"]
                )

    async def _queue_n_messages(
        self,
        *,
        queue: Queue,
        queue_alias: str,
        sqs_client: object,
        required_messages: int,
    ) -> None:
        for _ in range(math.ceil(required_messages / 10)):
            messages = await queue.get_messages(
                sqs_client,
                max_number_of_messages=(required_messages),
            )

            if len(messages) == 0:
                break

            await asyncio.gather(
                *[
                    self._consumer_callback(message, queue_alias)
                    for message in messages
                ]
            )

    async def _poll_messages(self) -> None:
        queues = list(sorted(self.queues.items(), key=lambda x: x[1].priority))
        queues = [item for item in queues if item[1].enabled]
        async with SESSION.client(**RESOURCE_OPTIONS_SQS) as sqs_client:
            while True:
                if self._available_workers_to_consume() < 1:
                    await asyncio.sleep(1)
                    continue
                for index, (queue_alias, queue) in enumerate(queues):
                    required_messages = math.ceil(
                        inverse_proportional_distribution(
                            self._available_workers_to_consume(),
                            [q[1].priority for q in queues[index:]],
                        )[0]
                    )

                    # it is necessary to request the messages several times
                    # because sqs only allows to obtain 10 messages at the
                    # same time
                    await self._queue_n_messages(
                        queue=queue,
                        queue_alias=queue_alias,
                        sqs_client=sqs_client,
                        required_messages=required_messages,
                    )

                    LOGGER.info(
                        "Queue messages info",
                        extra={
                            "extra": {
                                "queue": queue,
                                "queue_alias": queue_alias,
                                "sqs_client": sqs_client,
                                "required_messages": required_messages,
                            }
                        },
                    )
