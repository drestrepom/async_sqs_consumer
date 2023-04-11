import asyncio
import functools
from functools import (
    partial,
)
import logging
import random
import traceback
from typing import (
    Any,
    Callable,
    Literal,
    Optional,
    Tuple,
    Type,
    Union,
)

logging_logger = logging.getLogger(__name__)


async def __retry_internal(  # pylint: disable=too-many-arguments
    func: partial,
    tries: int,
    exceptions: Union[
        Type[BaseException], list[type[BaseException]]
    ] = Exception,
    delay: Optional[float] = None,
    max_delay: Optional[float] = None,
    backoff: Optional[float] = None,
    jitter: Union[Literal[1], Literal[0]] = 0,
    logger: Optional[logging.Logger] = None,
    log_traceback: bool = False,
    on_exception: Optional[Callable[[Type[Exception]], bool]] = None,
):
    _tries, _delay = tries, (delay or 0)
    logger = logger or logging_logger
    while _tries:
        try:
            return await func()
        except exceptions as exc:
            if on_exception is not None:
                if on_exception(exc):
                    break

            _tries -= 1
            if not _tries:
                raise

            if logger is not None:
                try:
                    func_qualname = func.func.__qualname__
                except AttributeError:
                    func_qualname = str(func.func)
                logger.warning(
                    "%s: %s in %s.%s, retrying in %s seconds...",
                    exc.__class__.__qualname__,
                    exc,
                    func.func.__module__,
                    func_qualname,
                    _delay,
                )
                if log_traceback:
                    logger.warning(traceback.format_exc())

            await asyncio.sleep(_delay)
            _delay *= backoff or 1

            if isinstance(jitter, tuple):
                _delay += random.uniform(*jitter)
            else:
                _delay += jitter

            if max_delay is not None:
                _delay = min(_delay, max_delay)


def retry(
    tries: int,
    exceptions: Union[
        Type[BaseException], list[type[BaseException]]
    ] = Exception,
    delay: Optional[float] = None,
    max_delay: Optional[float] = None,
    backoff: Optional[float] = None,
    jitter: Union[Literal[1], Literal[0]] = 0,
    logger: Optional[logging.Logger] = None,
    log_traceback: bool = False,
    on_exception: Optional[Callable[[Type[Exception]], bool]] = None,
):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*fargs, **fkwargs):
            args = fargs if fargs else []
            kwargs = fkwargs if fkwargs else {}
            return await __retry_internal(
                partial(func, *args, **kwargs),
                tries,
                exceptions,
                delay,
                max_delay,
                backoff,
                jitter,
                logger,
                log_traceback,
                on_exception,
            )

        return wrapper

    return decorator


async def retry_call(
    func: Union[partial, Callable[..., Any]],
    tries: int,
    exceptions: Union[
        Type[BaseException], list[type[BaseException]]
    ] = Exception,
    delay: Optional[float] = None,
    max_delay: Optional[float] = None,
    backoff: Optional[float] = None,
    jitter: Union[Literal[1], Literal[0]] = 0,
    logger: Optional[logging.Logger] = None,
    fargs: Optional[Tuple[Any, ...]] = None,
    fkwargs: Optional[dict[str, Any]] = None,
):
    args = fargs if fargs else tuple()
    kwargs = fkwargs if fkwargs else {}
    return await __retry_internal(
        partial(func, *args, **kwargs),
        tries,
        exceptions,
        delay,
        max_delay,
        backoff,
        jitter,
        logger,
    )
