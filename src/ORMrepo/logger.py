import inspect
from functools import wraps
from typing import Callable, Any, Awaitable
import logging

logger = logging.getLogger('ORMrepo')

def log(func: Callable) -> Callable | Awaitable:
    if inspect.iscoroutinefunction(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Called async %s (args=%s, kwargs=%s)", func.__name__, args, kwargs)
            result = await func(*args, **kwargs)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("%s returned %s", func.__name__, result)
            return result
        return async_wrapper
    else:
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Called %s (args=%s, kwargs=%s)", func.__name__, args, kwargs)
            result = func(*args, **kwargs)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("%s returned %s", func.__name__, result)
            return result
        return sync_wrapper

def format_list_log_preview(items: list[Any], preview: int = 5) -> str:
    total = len(items)
    if total <= preview * 2:
        return str(items)
    return f"{items[:preview]} ... {items[-preview:]} (total: {total})"