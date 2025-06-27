import inspect
from functools import wraps
from typing import Callable, Any, Awaitable
import logging

from ormrepo.types_ import log_level

logger = logging.getLogger('ORMrepo')


def log(level_n: log_level = 'DEBUG', level_r: log_level = 'DEBUG'):
    def wrapper(func: Callable) -> Callable | Awaitable:
        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_inner(*args, **kwargs):
                if logger.isEnabledFor(getattr(logging, level_n)):
                    logger.log(getattr(logging, level_n),
                               "Called async %s (args=%s, kwargs=%s)",
                               func.__name__, args, kwargs)
                result = await func(*args, **kwargs)
                if logger.isEnabledFor(getattr(logging, level_r)):
                    logger.log(getattr(logging, level_r),
                               "%s returned %s",
                               func.__name__, result)
                return result

            return async_inner
        else:
            @wraps(func)
            def sync_inner(*args, **kwargs):
                if logger.isEnabledFor(getattr(logging, level_n)):
                    logger.log(getattr(logging, level_n),
                               "Called %s (args=%s, kwargs=%s)",
                               func.__name__, args, kwargs)
                result = func(*args, **kwargs)
                if logger.isEnabledFor(getattr(logging, level_r)):
                    logger.log(getattr(logging, level_r),
                               "%s returned %s",
                               func.__name__, result)
                return result

            return sync_inner

    return wrapper


def format_list_log_preview(items: list[Any], preview: int = 5) -> str:
    """formats a string with a large number of elements"""
    total = len(items)
    if total <= preview * 2:
        return str(items)
    return f"{items[:preview]} ... {items[-preview:]} (total: {total})"
