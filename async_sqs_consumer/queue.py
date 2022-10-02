from async_sqs_consumer.resources import (
    get_sqs_client,
)
from typing import (
    Any,
)


async def get_queue_messages(queue_url: str) -> list[dict[str, Any]]:
    client = await get_sqs_client()
    response = await client.receive_message(
        QueueUrl=queue_url, WaitTimeSeconds=1
    )
    return response.get("Messages", [])
