from typing import (
    NamedTuple,
    Optional,
)

MESSAGE_SCHEMA = {
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


class AwsCredentials(NamedTuple):
    access_key_id: str
    secret_access_key: str
    session_token: Optional[str] = None


class SqsOptions(NamedTuple):
    visibility_timeout: Optional[int] = None


class Context(NamedTuple):
    queue_url: str
    aws_credentials: Optional[AwsCredentials] = None
    sqs: Optional[SqsOptions] = None
