# Async SQS consumer

Python asynchronous (**async** / **await**) worker for consuming messages
from AWS SQS.

This is a hobby project, if you find the project interesting
any contribution is welcome.

## Usage

You must create an instance of the worker with the url of the queue.

Aws credentials are taken from environment variables, you must set the
following environment variables. Or you can provide a Context object with the
aws credentials `async_sqs_consumer.types.Context`

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

Example:

You can get the queue url with the follow aws cli command
`aws sqs get-queue-url --queue-name xxxxxx`

```python
# test_worker.py

from async_sqs_consumer.worker import (
    Worker,
)

worker = Worker(
    queue_url="https://sqs.us-east-1.amazonaws.com/xxxxxxx/queue_name"
)


@worker.task("report")
async def report(text: str) -> None:
    print(text)

if __name__: "__main__":
    worker.start()
```

Now you can initialize the worker `python test_worker.py`

Now you need to post a message for the worker to process

```python
import json
import boto3
import uuid

client = boto3.client("sqs")

client.send_message(
    QueueUrl="https://sqs.us-east-1.amazonaws.com/xxxxxxx/queue_name",
    MessageBody=json.dumps(
        {
            "task": "report",
            "id": uuid.uuid4().hex,
            "args": ["hello world"],
        }
    ),
)
```

Or you can use aioboto3

```python
import asyncio
import json
import aioboto3
import uuid


async def main() -> None:
    session = aioboto3.Session()
    async with session.client("sqs") as client:
        await client.send_message(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/xxxxxxx/queue_name",
            MessageBody=json.dumps(
                {
                    "task": "report",
                    "id": uuid.uuid4().hex,
                    "args": ["hello world"],
                }
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

To publish the messages they must have the following structure

```json
{
    "type": "object",
    "properties": {
        "task": {"type": "string"},
        "id": {"type": "string"},
        "args": {"type": "array"},
        "kwargs": {"type": "object"},
        "retries": {"type": "number"},
        "eta": {"type": "string"},
        "expires": {"type": "string"},
    },
    "required": ["task", "id"],
}
```
