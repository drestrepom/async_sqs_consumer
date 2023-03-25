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
from async_sqs_consumer.utils.retry import (
    retry,
)
import asyncio
from asyncio import (
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
from typing import (
    Any,
    Callable,
    Coroutine,
    Optional,
)

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
) -> list[dict[str, Any]]:
    client = await get_sqs_client(credentials)
    response = await client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        VisibilityTimeout=visibility_timeout or 60,
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
    def __init__(
        self,
        url: str,
        authentication: Optional[AwsCredentials] = None,
        polling_interval: Optional[float] = None,
        visibility_timeout: Optional[int] = None,
    ) -> None:
        self.url = url
        self.authentication = authentication
        self.polling_interval = polling_interval or 1.0
        self.visibility_timeout = visibility_timeout or 60
        self._polling = False

    async def start_polling(
        self,
        callback: Callable[[dict[str, Any], str], Coroutine[Any, Any, None]],
        queue_alias: str,
    ) -> None:
        self._polling = self._polling or True
        async with SESSION.client(**RESOURCE_OPTIONS_SQS) as sqs_client:
            while self._polling:
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
