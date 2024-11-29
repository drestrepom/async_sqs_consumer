from enum import (
    Enum,
)
from typing import (
    NamedTuple,
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
SNS_NOTIFICATION_SCHEMA = {
    "type": "object",
    "properties": {
        "Type": {"type": "string"},
        "MessageId": {"type": "string"},
        "TopicArn": {"type": "string"},
        "Message": {"type": "string"},
        "Timestamp": {"type": "string"},
        "SignatureVersion": {"type": "string"},
        "Signature": {"type": "string"},
        "SigningCertURL": {"type": "string"},
        "UnsubscribeURL": {"type": "string"},
    },
    "required": ["Type", "Message", "Timestamp"],
}


class AwsCredentials(NamedTuple):
    access_key_id: str
    secret_access_key: str
    session_token: str | None = None


class MessageType(Enum):
    CUSTOM: str = "CUSTOM"
    SNS_NOTIFICATION: str = "SNS_NOTIFICATION"


class SqsOptions(NamedTuple):
    visibility_timeout: int | None = None


class Context(NamedTuple):
    queue_url: str
    aws_credentials: AwsCredentials | None = None
    sqs: SqsOptions | None = None
