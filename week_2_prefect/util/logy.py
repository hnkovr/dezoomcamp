import functools
import sys

from loguru import logger
from loguru import logger as log
from util.printy import fmt_a_or_kw, value_shortext

log = logger
# format = '{time:YY/MM/DD HH:mm:ss} {level.name[0]} file://{file.path} :{line} {message}'
# format = '{time:YY/MM/DD HH:mm:ss} {level.name[0]} "{file.path}:{line}" {message}'
format = '{time:YY/MM/DD HH:mm:ss} {level.name[0]} "{file.path}", line {line}: {message}'

log.remove()  # remove any existing handlers
log.add(
    sys.stdout,
    format=format,
    level="DEBUG",
    backtrace=True,
    diagnose=True,
    colorize=True,
)
log.debug(f"{ format=}")


def _log_args_and_result(func):
    """
    A decorator that logs the input arguments and the result of the decorated function or method.
    #ChatGPT by created w/o changes (almost))
    """
    print_fn = log.debug

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print_fn(
            f"Calling {func.__name__}{'' if (args and kwargs) else ''}"
            '('
            f"{f' {fmt_a_or_kw(args)}' if args else ''}"
            f"{f', ' if (args and kwargs) else ''}"
            f"{f' {fmt_a_or_kw(kwargs)}' if kwargs else ''}"
            ')->'
        )
        result = func(*args, **kwargs)
        print_fn(f'{func.__name__} returned: {value_shortext(result)}')
        return result

    return wrapper


call_log = _log_args_and_result
