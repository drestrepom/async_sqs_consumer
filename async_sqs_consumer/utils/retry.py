import functools
from functools import (
    partial,
)
import logging
import random
from time import (
    sleep,
)
import traceback

logging_logger = logging.getLogger(__name__)


async def __retry_internal(  # pylint: disable=too-many-arguments
    func,
    exceptions=Exception,
    tries=-1,
    delay=0,
    max_delay=None,
    backoff=1,
    jitter=0,
    logger=logging_logger,
    log_traceback=False,
    on_exception=None,
):
    _tries, _delay = tries, delay
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

            sleep(_delay)
            _delay *= backoff

            if isinstance(jitter, tuple):
                _delay += random.uniform(*jitter)
            else:
                _delay += jitter

            if max_delay is not None:
                _delay = min(_delay, max_delay)


def retry(
    exceptions=Exception,
    tries=-1,
    delay=0,
    max_delay=None,
    backoff=1,
    jitter=0,
    logger=logging_logger,
    log_traceback=False,
    on_exception=None,
):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*fargs, **fkwargs):
            args = fargs if fargs else []
            kwargs = fkwargs if fkwargs else {}
            return await __retry_internal(
                partial(func, *args, **kwargs),
                exceptions,
                tries,
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
    func,
    fargs=None,
    fkwargs=None,
    exceptions=Exception,
    tries=-1,
    delay=0,
    max_delay=None,
    backoff=1,
    jitter=0,
    logger=logging_logger,
):
    args = fargs if fargs else []
    kwargs = fkwargs if fkwargs else {}
    return await __retry_internal(
        partial(func, *args, **kwargs),
        exceptions,
        tries,
        delay,
        max_delay,
        backoff,
        jitter,
        logger,
    )
