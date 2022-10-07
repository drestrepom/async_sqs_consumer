from aiohttp.client_exceptions import (
    ClientConnectorError,
    ServerDisconnectedError,
)
from async_sqs_consumer.resources import (
    get_sqs_client,
)
from async_sqs_consumer.utils.retry import (
    retry,
)
import asyncio
from botocore.exceptions import (
    ClientError,
)
from typing import (
    Any,
)

NETWORK_ERRORS = (
    ServerDisconnectedError,
    ClientConnectorError,
    ClientError,
    asyncio.TimeoutError,
)


@retry(exceptions=NETWORK_ERRORS, tries=3, delay=0.2)
async def get_queue_messages(queue_url: str) -> list[dict[str, Any]]:
    client = await get_sqs_client()
    response = await client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        VisibilityTimeout=600,
    )
    return response.get("Messages", [])


@retry(exceptions=NETWORK_ERRORS, tries=3, delay=0.2)
async def delete_messages(
    queue_url: str, receipt_handle: dict[str, Any]
) -> None:
    client = await get_sqs_client()
    await client.delete_message(
        ReceiptHandle=receipt_handle,
        QueueUrl=queue_url,
    )
