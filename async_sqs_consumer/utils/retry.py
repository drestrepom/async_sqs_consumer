# pylint: disable=broad-except
# pylint: disable=too-many-arguments
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
    Type,
)

logging_logger = logging.getLogger(__name__)


def _log_exception(
    func: partial,
    exc: Exception,
    logger: logging.Logger | None = None,
    log_traceback: bool = False,
    _delay: float | int | None = None,
) -> None:
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


def _increase_delay(
    _delay: float,
    backoff: float | None = None,
    jitter: Literal[1] | Literal[0] = 0,
    max_delay: float | None = None,
) -> float:
    _delay *= backoff or 1

    if isinstance(jitter, tuple):
        _delay += random.uniform(*jitter)
    else:
        _delay += jitter

    if max_delay is not None:
        _delay = min(_delay, max_delay)

    return _delay


async def __retry_internal(
    func: partial,
    tries: int,
    exceptions: Type[Exception] | tuple[type[Exception], ...] | None = None,
    delay: float | None = None,
    max_delay: float | None = None,
    backoff: float | None = None,
    jitter: Literal[1] | Literal[0] = 0,
    logger: logging.Logger | None = None,
    log_traceback: bool = False,
    on_exception: Callable[[Exception], bool] | None = None,
) -> None:
    exceptions = exceptions or Exception
    _tries, _delay = tries, (delay or 0.0)
    logger = logger or logging_logger
    while _tries >= 0:
        try:
            return await func()
        except exceptions as exc:
            if on_exception is not None and on_exception(exc):
                break

            _tries -= 1
            _log_exception(func, exc, logger, log_traceback, _delay)

            await asyncio.sleep(_delay)
            _delay = _increase_delay(_delay, backoff, jitter, max_delay)


def retry(  # pylint: disable=too-many-arguments
    tries: int,
    exceptions: Type[Exception] | tuple[type[Exception], ...] = Exception,
    delay: float | None = None,
    max_delay: float | None = None,
    backoff: float | None = None,
    jitter: Literal[1] | Literal[0] = 0,
    logger: logging.Logger | None = None,
    log_traceback: bool = False,
    on_exception: Callable[[Exception], bool] | None = None,
) -> Callable[[Callable], Callable]:
    def decorator(func: Callable[..., str]) -> Any:
        @functools.wraps(func)
        async def wrapper(
            *fargs: list[str], **fkwargs: dict[str, str]
        ) -> None:
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


async def retry_call(  # pylint: disable=too-many-arguments
    func: partial | Callable[..., object],
    tries: int,
    exceptions: Type[Exception] | tuple[type[Exception], ...] = Exception,
    delay: float | None = None,
    max_delay: float | None = None,
    backoff: float | None = None,
    jitter: Literal[1] | Literal[0] = 0,
    logger: logging.Logger | None = None,
    fargs: tuple[object, ...] | None = None,
    fkwargs: dict[str, object] | None = None,
) -> None:
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
