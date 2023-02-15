# from util.ef import f


# def ef(arg_max_len: int = 222, ignore_exceptions: List[Type[Exception]] = None,
#        ignore_exception_texts: List[str] = None, ignore_exception_text_pairs: List[Tuple[Type[Exception], str]] = None):
#     """
#     Decorator that logs all function/method calls, adds exception handling and allows for ignoring certain exceptions.
#
#     :param arg_max_len: maximum length of arguments and function result that will be logged. Default is 222.
#     :param ignore_exceptions: list of exceptions to ignore.
#     :param ignore_exception_texts: list of texts in exception message to ignore.
#     :param ignore_exception_text_pairs: list of pairs of (exception, error text) to ignore.
#
#     :Example:
#
#     .. code-block:: python
#
#         @ef(ignore_exceptions=(ValueError,))
#         def example_function(a, b, c=3):
#             return a + b + c
#     """
#
#     def decorator(func):
#         stub = '..'
#
#         def fmt_arg(arg: Any): sarg = repr(arg);return sarg[:arg_max_len] + (stub if len(sarg) > arg_max_len else '')
#
#         @functools.wraps(func)
#         def wrapper(*args, **kwargs):
#             logger.debug(
#                 f"# call {func.__name__} ("
#                 f"{', '.join(fmt_arg(arg) for arg in args)}"
#                 # and keyword arguments
#                 f"{', ' if args and kwargs else ''}"
#                 f"{', '.join(f'{key}={fmt_arg(value)}' for key, value in kwargs.items())}"
#                 f")"
#             )
#
#             with logger.catch(reraise=True):
#                 result = func(*args, **kwargs)
#                 logger.debug(f"# call {func.__name__} returned: {repr(result)[:arg_max_len]}")
#                 return result
#
#             exception_text = str(logger.catch.exception)
#             if ((ignore_exceptions and isinstance(logger.catch.exception, ignore_exceptions))
#                     or (ignore_exception_texts
#                         and any(text in exception_text for text in ignore_exception_texts))
#                     or (ignore_exception_text_pairs
#                         and any(
#                                 (isinstance(catch.exception, exception), text in exception_text)
#                                 for exception, text in ignore_exception_text_pairs)
#                     )
#             ):
#                 raise catch.exception
#
#             logger.exception(f"Function {func.__name__} raised an exception")
#             raise catch.exception
#
#         return wrapper
#
#     return decorator


# To replace a stack of decorators with a single unioned decorator in Python, you can define a new decorator that takes a list of decorated functions and returns a single function that applies the behavior of each decorator in the list.
#
# Here is an example implementation:
#
# ```
def union(*decorators):
    def decorator(func):
        for decorator in reversed(decorators):
            func = decorator(func)
        return func

    return decorator


# # ```
# #
# # You can use this `union_decorator` to replace multiple decorators applied to a function like this:
# #
# # ```
# @union(decorator1, decorator2, decorator3)
# def my_function():
#     pass
# # ```
# #
# # This is equivalent to:
# #
# # ```
# @decorator1
# @decorator2
# @decorator3
# def my_function():
#     pass
# # ```
