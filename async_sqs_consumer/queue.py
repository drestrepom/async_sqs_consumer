from .resources import (
    get_sqs_client,
    RESOURCE_OPTIONS_SQS,
    SESSION,
)
from .types import (
    AwsCredentials,
)
from .utils import (
    TASK_NAME_PREFIX,
)
from .utils.retry import (
    retry,
)
from aiohttp.client_exceptions import (
    ClientConnectorError,
    ClientPayloadError,
    ServerDisconnectedError,
    ServerTimeoutError,
)
import asyncio
from asyncio import (
    get_event_loop,
    sleep,
)
from botocore.client import (
    BaseClient,
)
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    HTTPClientError,
    ReadTimeoutError,
)
from contextlib import (
    suppress,
)
import logging
from typing import (
    Any,
    Callable,
    Coroutine,
)

LOGGER = logging.getLogger(__name__)
NETWORK_ERRORS = (
    asyncio.TimeoutError,
    ClientConnectorError,
    ClientError,
    ClientPayloadError,
    ConnectionResetError,
    ConnectTimeoutError,
    HTTPClientError,
    ReadTimeoutError,
    ServerDisconnectedError,
    ServerTimeoutError,
)


def set_max_number_of_messages(max_number_of_messages: int | None) -> int:
    max_number_of_messages = max_number_of_messages or 10
    return min(10, max_number_of_messages)


@retry(exceptions=NETWORK_ERRORS, tries=3, delay=1)
async def get_queue_messages(
    *,
    queue_url: str,
    credentials: AwsCredentials | None = None,
    client: BaseClient | None = None,
    visibility_timeout: int | None = None,
    wait_time_seconds: int | None = None,
    max_number_of_messages: int | None = None,
) -> list[dict[str, object]]:
    client = client or await get_sqs_client(credentials)
    max_number_of_messages = set_max_number_of_messages(max_number_of_messages)
    try:
        response = await client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_number_of_messages,
            VisibilityTimeout=visibility_timeout or 60,
            WaitTimeSeconds=wait_time_seconds or 0,
        )
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.error(
            "There was an error with the response",
            extra={
                "extra": {
                    "client": client,
                    "queue_url": queue_url,
                    "max_number_of_messages": max_number_of_messages,
                    "exc": exc,
                }
            },
        )

    return response.get("Messages", [])


@retry(exceptions=NETWORK_ERRORS, tries=3, delay=1)
async def delete_messages(
    queue_url: str,
    receipt_handle: dict[str, object],
    credentials: AwsCredentials | None = None,
) -> None:
    try:
        client = await get_sqs_client(credentials)
        await client.delete_message(
            ReceiptHandle=receipt_handle,
            QueueUrl=queue_url,
        )
    except NETWORK_ERRORS as exc:
        LOGGER.exception(
            "There was an error deleting messages",
            extra={
                "extra": {
                    "receipt_handle": receipt_handle,
                    "queue_url": queue_url,
                    "exc": exc,
                }
            },
        )


class Queue:  # pylint: disable=too-many-instance-attributes
    def __init__(  # pylint: disable=too-many-arguments
        self,
        url: str,
        priority: int | None = None,
        authentication: AwsCredentials | None = None,
        polling_interval: float | None = None,
        visibility_timeout: int | None = None,
        max_queue_parallel_messages: int | None = None,
        enabled: bool | None = None,
    ) -> None:
        self.url = url
        self.priority = priority or 1
        self.authentication = authentication
        self.polling_interval = polling_interval or 1.0
        self.visibility_timeout = visibility_timeout or 60
        self._max_queue_parallel_messages = max_queue_parallel_messages
        self._polling = False
        self.enabled = True if enabled is None else enabled

    async def get_messages(
        self, sqs_client: object, max_number_of_messages: int | None = None
    ) -> list[dict[str, object]]:
        with suppress(asyncio.CancelledError):
            messages = await get_queue_messages(
                queue_url=self.url,
                client=sqs_client,
                visibility_timeout=self.visibility_timeout,
                max_number_of_messages=max_number_of_messages,
            )
            return messages
        return []

    def is_task_limit_exceeded(
        self, max_parallel_messages: int | None
    ) -> bool:
        number_of_tasks = len(
            [
                task
                for task in asyncio.all_tasks(get_event_loop())
                if task.get_name().startswith(TASK_NAME_PREFIX)
            ]
        )
        return number_of_tasks > (
            self._max_queue_parallel_messages or max_parallel_messages or 1024
        )

    async def start_polling(
        self,
        callback: Callable[
            [dict[str, object], str], Coroutine[object, object, None]
        ],
        queue_alias: str,
        max_parallel_messages: int | None = None,
    ) -> None:
        self._polling = self._polling or True
        async with SESSION.client(**RESOURCE_OPTIONS_SQS) as sqs_client:
            while self._polling:
                if self.is_task_limit_exceeded(max_parallel_messages):
                    await sleep(self.polling_interval)
                    continue

                with suppress(asyncio.CancelledError):
                    messages = await get_queue_messages(
                        queue_url=self.url,
                        client=sqs_client,
                        visibility_timeout=self.visibility_timeout,
                    )
                    await asyncio.gather(
                        *[
                            callback(message, queue_alias)
                            for message in messages
                        ]
                    )
                    await sleep(self.polling_interval)

    def stop_polling(self) -> None:
        self._polling = False

    async def delete_messages(
        self,
        receipt_handle: dict[str, Any],
    ) -> None:
        async with SESSION.client(**RESOURCE_OPTIONS_SQS) as sqs_client:
            await sqs_client.delete_message(
                ReceiptHandle=receipt_handle,
                QueueUrl=self.url,
            )
        LOGGER.info(
            "Deleted messages parameters",
            extra={
                "extra": {
                    "sqs_client": self.url,
                    "receipt_handle": receipt_handle,
                }
            },
        )
