import typing as T
import logging
import random
import time
from functools import partial
from decorator import decorator

logging_logger = logging.getLogger(__name__)

EXCEPTIONS = T.Union[T.Tuple[T.Type[Exception], ...], T.Type[Exception]]

P = T.ParamSpec("P")


def __retry_internal_sync(
    f: T.Callable[P, T.Any],
    exceptions: EXCEPTIONS = Exception,
    tries: int = -1,
    delay: float = 0,
    max_delay: float = None,
    backoff: float = 1,
    jitter: float = 0,
    logger: logging.Logger = logging_logger,
) -> T.Any:
    """
    Executes a function and retries it if it failed.

    :param f: the function to execute.
    :param exceptions: an exception or a tuple of exceptions to catch. default: Exception.
    :param tries: the maximum number of attempts. default: -1 (infinite).
    :param delay: initial delay between attempts. default: 0.
    :param max_delay: the maximum value of delay. default: None (no limit).
    :param backoff: multiplier applied to delay between attempts. default: 1 (no backoff).
    :param jitter: extra seconds added to delay between attempts. default: 0.
                   fixed if a number, random if a range tuple (min, max)
    :param logger: logger.warning(fmt, error, delay) will be called on failed attempts.
                   default: retry.logging_logger. if None, logging is disabled.
    :returns: the result of the f function.
    """
    _tries, _delay = tries, delay
    while _tries:
        try:
            return f()
        except exceptions as e:
            _tries -= 1
            if not _tries:
                raise

            if logger is not None:
                logger.warning("%s, retrying in %s seconds...", e, _delay)

            time.sleep(_delay)
            _delay *= backoff

            if isinstance(jitter, tuple):
                _delay += random.uniform(*jitter)
            else:
                _delay += jitter

            if max_delay is not None:
                _delay = min(_delay, max_delay)


async def __retry_internal_async(
    f: T.Callable[P, T.Any],
    exceptions: EXCEPTIONS = Exception,
    tries: int = -1,
    delay: float = 0,
    max_delay: float = None,
    backoff: float = 1,
    jitter: float = 0,
    logger: logging.Logger = logging_logger,
) -> T.Any:
    """
    Executes a function and retries it if it failed.

    :param f: the function to execute.
    :param exceptions: an exception or a tuple of exceptions to catch. default: Exception.
    :param tries: the maximum number of attempts. default: -1 (infinite).
    :param delay: initial delay between attempts. default: 0.
    :param max_delay: the maximum value of delay. default: None (no limit).
    :param backoff: multiplier applied to delay between attempts. default: 1 (no backoff).
    :param jitter: extra seconds added to delay between attempts. default: 0.
                   fixed if a number, random if a range tuple (min, max)
    :param logger: logger.warning(fmt, error, delay) will be called on failed attempts.
                   default: retry.logging_logger. if None, logging is disabled.
    :returns: the result of the f function.
    """
    _tries, _delay = tries, delay
    while _tries:
        try:
            return await f()
        except exceptions as e:
            _tries -= 1
            if not _tries:
                raise

            if logger is not None:
                logger.warning("%s, retrying in %s seconds...", e, _delay)

            time.sleep(_delay)
            _delay *= backoff

            if isinstance(jitter, tuple):
                _delay += random.uniform(*jitter)
            else:
                _delay += jitter

            if max_delay is not None:
                _delay = min(_delay, max_delay)


DecSpecs = T.ParamSpec("DecSpecs")


def retry(
    *,
    is_async: bool,
    exceptions: EXCEPTIONS = Exception,
    tries: int = -1,
    delay: float = 0,
    max_delay: float = None,
    backoff: float = 1,
    jitter: float = 0,
    logger: logging.Logger = logging_logger,
) -> T.Callable[..., T.Any]:
    """Returns a retry decorator.

    :param exceptions: an exception or a tuple of exceptions to catch. default: Exception.
    :param tries: the maximum number of attempts. default: -1 (infinite).
    :param delay: initial delay between attempts. default: 0.
    :param max_delay: the maximum value of delay. default: None (no limit).
    :param backoff: multiplier applied to delay between attempts. default: 1 (no backoff).
    :param jitter: extra seconds added to delay between attempts. default: 0.
                   fixed if a number, random if a range tuple (min, max)
    :param logger: logger.warning(fmt, error, delay) will be called on failed attempts.
                   default: retry.logging_logger. if None, logging is disabled.
    :returns: a retry decorator.
    """

    if is_async:

        @decorator
        async def retry_decorator_async(
            f: T.Callable[P, T.Any], *fargs: T.Any, **fkwargs: T.Any
        ) -> T.Any:
            args = fargs if fargs else list()
            kwargs = fkwargs if fkwargs else dict()
            return await __retry_internal_async(
                partial(f, *args, **kwargs),
                exceptions,
                tries,
                delay,
                max_delay,
                backoff,
                jitter,
                logger,
            )

        return retry_decorator_async
    else:

        @decorator
        def retry_decorator_sync(
            f: T.Callable[P, T.Any], *fargs: T.Any, **fkwargs: T.Any
        ) -> T.Any:
            args = fargs if fargs else list()
            kwargs = fkwargs if fkwargs else dict()
            return __retry_internal_sync(
                partial(f, *args, **kwargs),
                exceptions,
                tries,
                delay,
                max_delay,
                backoff,
                jitter,
                logger,
            )

        return retry_decorator_sync


def retry_call_sync(
    f: T.Callable[P, T.Any],
    fargs: T.Any = None,
    fkwargs: T.Any = None,
    exceptions: EXCEPTIONS = Exception,
    tries: int = -1,
    delay: float = 0,
    max_delay: float = None,
    backoff: float = 1,
    jitter: float = 0,
    logger: logging.Logger = logging_logger,
) -> T.Any:
    """
    Calls a function and re-executes it if it failed.

    :param f: the function to execute.
    :param fargs: the positional arguments of the function to execute.
    :param fkwargs: the named arguments of the function to execute.
    :param exceptions: an exception or a tuple of exceptions to catch. default: Exception.
    :param tries: the maximum number of attempts. default: -1 (infinite).
    :param delay: initial delay between attempts. default: 0.
    :param max_delay: the maximum value of delay. default: None (no limit).
    :param backoff: multiplier applied to delay between attempts. default: 1 (no backoff).
    :param jitter: extra seconds added to delay between attempts. default: 0.
                   fixed if a number, random if a range tuple (min, max)
    :param logger: logger.warning(fmt, error, delay) will be called on failed attempts.
                   default: retry.logging_logger. if None, logging is disabled.
    :returns: the result of the f function.
    """
    args = fargs if fargs else list()
    kwargs = fkwargs if fkwargs else dict()
    return __retry_internal_sync(
        partial(f, *args, **kwargs),
        exceptions,
        tries,
        delay,
        max_delay,
        backoff,
        jitter,
        logger,
    )


async def retry_call_async(
    f: T.Callable[P, T.Any],
    fargs: T.Any = None,
    fkwargs: T.Any = None,
    exceptions: EXCEPTIONS = Exception,
    tries: int = -1,
    delay: float = 0,
    max_delay: float = None,
    backoff: float = 1,
    jitter: float = 0,
    logger: logging.Logger = logging_logger,
) -> T.Any:
    """
    Calls a function and re-executes it if it failed.

    :param f: the function to execute.
    :param fargs: the positional arguments of the function to execute.
    :param fkwargs: the named arguments of the function to execute.
    :param exceptions: an exception or a tuple of exceptions to catch. default: Exception.
    :param tries: the maximum number of attempts. default: -1 (infinite).
    :param delay: initial delay between attempts. default: 0.
    :param max_delay: the maximum value of delay. default: None (no limit).
    :param backoff: multiplier applied to delay between attempts. default: 1 (no backoff).
    :param jitter: extra seconds added to delay between attempts. default: 0.
                   fixed if a number, random if a range tuple (min, max)
    :param logger: logger.warning(fmt, error, delay) will be called on failed attempts.
                   default: retry.logging_logger. if None, logging is disabled.
    :returns: the result of the f function.
    """
    args = fargs if fargs else list()
    kwargs = fkwargs if fkwargs else dict()
    return await __retry_internal_async(
        partial(f, *args, **kwargs),
        exceptions,
        tries,
        delay,
        max_delay,
        backoff,
        jitter,
        logger,
    )
