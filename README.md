# Async SQS consumer

This is a simple asynchronous worker for consuming messages from AWS SQS.

This is a hobby project, if you find the project interesting
any contribution is welcome


## Usage

You must create an instance of the worker with the url of the queue.

Aws credentials are taken from environment variables, you must set the following environment variables

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

```python
from async_sqs_consumer.worker import (
    Worker,
)

worker = Worker(
    queue_url="https://sqs.us-east-1.amazonaws.com/xxxxxxx/queue_name"
)


@worker.task("report")
async def report(text: str) -> None:
    print(text)

worker.start()
```

To publish the messages they must have the following structure

```json
{
    "task": {"type": "string"},
    "id": {"type": "string"},
    "args": {"type": "array"},
    "kwargs": {"type": "object"},
    "retries": {"type": "number"},
    "eta": {"type": "string"},
    "expires": {"type": "string"},
}
```
