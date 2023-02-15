from .logy import log
from .deco import union
from .logy import call_log

ef = log.catch
ef = union(log.catch,
           call_log
           )

# if __name__ == '__main__':
#     @ef(ignore_exceptions=(ValueError,))
#     def example_function(a, b, c=3):
#         return a + b + c
#
#
#     example_function(11, 22)
#     example_function(11, 22, c=333)

# def ef(f):
#     @call_log
#     @log.catch
#     def wrap(*a, **k): return f(*a, **k)
#     return wrap

if __name__ == '__main__':
    # @ef
    def f():
        1 / 0


    f()
