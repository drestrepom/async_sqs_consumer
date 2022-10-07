from async_sqs_consumer.types import (
    MESSAGE_SCHEMA,
)
from contextlib import (
    suppress,
)
from jsonschema import (
    validate,
)
from jsonschema.exceptions import (
    ValidationError,
)
from typing import (
    Any,
)


def validate_message(message: dict[str, Any]) -> bool:
    with suppress(ValidationError):
        validate(message, MESSAGE_SCHEMA)
        return True
    return False
