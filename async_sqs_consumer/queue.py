from aiohttp.client_exceptions import (
    ClientConnectorError,
    ServerDisconnectedError,
)
from async_sqs_consumer.resources import (
    get_sqs_client,
    RESOURCE_OPTIONS_SQS,
    SESSION,
)
from async_sqs_consumer.types import (
    AwsCredentials,
)
from async_sqs_consumer.utils import (
    TASK_NAME_PREFIX,
)
from async_sqs_consumer.utils.retry import (
    retry,
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
)
from contextlib import (
    suppress,
)
import logging
from typing import (
    Any,
    Callable,
    Coroutine,
    Optional,
)

LOGGER = logging.getLogger(__name__)
NETWORK_ERRORS = (
    ServerDisconnectedError,
    ClientConnectorError,
    ClientError,
    asyncio.TimeoutError,
)


@retry(exceptions=NETWORK_ERRORS, tries=3, delay=0.2)
async def get_queue_messages(
    queue_url: str,
    credentials: Optional[AwsCredentials] = None,
    client: Optional[BaseClient] = None,
    visibility_timeout: Optional[int] = None,
    wait_time_seconds: Optional[int] = None,
) -> list[dict[str, Any]]:
    client = await get_sqs_client(credentials)
    response = await client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        VisibilityTimeout=visibility_timeout or 60,
        WaitTimeSeconds=wait_time_seconds or 0,
    )
    return response.get("Messages", [])


@retry(exceptions=NETWORK_ERRORS, tries=3, delay=0.2)
async def delete_messages(
    queue_url: str,
    receipt_handle: dict[str, Any],
    credentials: Optional[AwsCredentials] = None,
) -> None:
    client = await get_sqs_client(credentials)
    await client.delete_message(
        ReceiptHandle=receipt_handle,
        QueueUrl=queue_url,
    )


class Queue:
    def __init__(  # pytlint: disable=too-many-arguments,too-many-arguments
        self,
        url: str,
        priority: Optional[int] = None,
        authentication: Optional[AwsCredentials] = None,
        polling_interval: Optional[float] = None,
        visibility_timeout: Optional[int] = None,
        max_queue_parallel_messages: Optional[int] = None,
    ) -> None:
        self.url = url
        self.priority = priority or 1
        self.authentication = authentication
        self.polling_interval = polling_interval or 1.0
        self.visibility_timeout = visibility_timeout or 60
        self._max_queue_parallel_messages = max_queue_parallel_messages
        self._polling = False

    async def get_messages(self, sqs_client: Any) -> list[dict[str, Any]]:
        with suppress(asyncio.CancelledError):
            messages = await get_queue_messages(
                self.url,
                client=sqs_client,
                visibility_timeout=self.visibility_timeout,
            )
            return messages
        return []

    async def start_polling(
        self,
        callback: Callable[[dict[str, Any], str], Coroutine[Any, Any, None]],
        queue_alias: str,
        max_parallel_messages: Optional[int] = None,
    ) -> None:
        self._polling = self._polling or True
        async with SESSION.client(**RESOURCE_OPTIONS_SQS) as sqs_client:
            while self._polling:
                if len(
                    [
                        task
                        for task in asyncio.all_tasks(get_event_loop())
                        if task.get_name().startswith(TASK_NAME_PREFIX)
                    ]
                ) > (
                    self._max_queue_parallel_messages
                    or max_parallel_messages
                    or 1024
                ):
                    await sleep(self.polling_interval)
                    continue

                with suppress(asyncio.CancelledError):
                    messages = await get_queue_messages(
                        self.url,
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
