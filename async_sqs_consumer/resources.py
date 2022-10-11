import aioboto3
from aiobotocore.config import (
    AioConfig,
)
from async_sqs_consumer.types import (
    AwsCredentials,
)
from contextlib import (
    AsyncExitStack,
)
from typing import (
    Any,
    Optional,
)

RESOURCE_OPTIONS_SQS = {
    "aws_access_key_id": (None),
    "aws_secret_access_key": (None),
    "config": AioConfig(
        # The time in seconds till a timeout exception is thrown when
        # attempting to make a connection. [60]
        connect_timeout=15,
        # Maximum amount of simultaneously opened connections. [10]
        # https://docs.aiohttp.org/en/stable/client_advanced.html#limiting-connection-pool-size
        max_pool_connections=2000,
        # The time in seconds till a timeout exception is thrown when
        # attempting to read from a connection. [60]
        read_timeout=30,
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html
        retries={"max_attempts": 10, "mode": "standard"},
        # Signature version for signing URLs
        # https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3.html#generating-presigned-urls
        signature_version="s3v4",
    ),
    "endpoint_url": None,
    "region_name": "us-east-1",
    "service_name": "sqs",
    "use_ssl": True,
    "verify": True,
}

SESSION = aioboto3.Session()
CONTEXT_STACK_SQS = None
RESOURCE_SQS = None


async def sqs_startup(credentials: Optional[AwsCredentials] = None) -> None:
    # pylint: disable=global-statement
    global CONTEXT_STACK_SQS, RESOURCE_SQS

    CONTEXT_STACK_SQS = AsyncExitStack()
    if credentials:
        RESOURCE_OPTIONS_SQS["aws_access_key_id"] = credentials.access_key_id
        RESOURCE_OPTIONS_SQS[
            "aws_secret_access_key"
        ] = credentials.secret_access_key
        RESOURCE_OPTIONS_SQS["aws_session_token"] = credentials.session_token

    RESOURCE_SQS = await CONTEXT_STACK_SQS.enter_async_context(
        SESSION.client(**RESOURCE_OPTIONS_SQS)
    )


async def sqs_shutdown() -> None:
    if CONTEXT_STACK_SQS:
        await CONTEXT_STACK_SQS.aclose()


async def get_sqs_client(credentials: Optional[AwsCredentials] = None) -> Any:
    if RESOURCE_SQS is None:
        await sqs_startup(credentials)

    return RESOURCE_SQS
