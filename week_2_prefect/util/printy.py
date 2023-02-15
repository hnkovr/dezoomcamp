import textwrap
from typing import Any


def fmt_df(df): return f"\n{df.head(3)}\n..."
pass_=lambda _:_

quote = pass_


def fmt_arg(a):
    """
    >>> fmt_arg('aaabbbcccddddeeee ffffgggghhhhiiiijjjj aaabbbcccdddd eeeeffffgggg hhhhiiii jjjj')
    "'aaabbbcccddddeeee ffffgggghhhhiiiijjjj [...] eeeeffffgggg hhhhiiii jjjj'"
    """
    return value_shortext(a if not isinstance(a, str) else quote(a))


def fmt_a_or_kw(a):
    """
    >>> fmt_a_or_kw((1, 22, 'aaa'))
    "1, 22, 'aaa'"
    >>> fmt_a_or_kw(dict(a=1, b=22, c='aaabbb cccddddeeeeffff gggghhhh iiii jj jj'))
    "a=1, b=22, c='aaabbb [...] iiii jj jj'"
    """
    return (
        ', '.join(map(fmt_arg, a))
        if isinstance(a, tuple) else
        ', '.join(f"{k}={fmt_arg(v)}" for k, v in a.items())
        if isinstance(a, dict) else '?'
    )


# def_shortext_len, DEF_LENGHTS = 111, 111
def_shortext_len = 1111


# @constants
class DEF_LENGHTS:
    # (enum.Enum):
    VAR_VALUE = 111  # for variables/args trace/log output


def shorten(s: str, length, len_tail_percent=0.3, placeholder='[...]', **k) -> str:
    """
    >>> shorten('12345 67890 5678 90 qwerty qwe rt y', 22)
    '12345 [...] rt y'
    """
    if length >= len(s): length = len(s)
    len_tail = int(length * len_tail_percent)
    len_start = int(length - len_tail)
    # print('>>>>', len(s), length, len_start, len_tail)
    try:
        return (
                textwrap.shorten(s, width=len_start, **k) +
                textwrap.shorten(s[::-1], width=max(len(placeholder), len_tail + len(placeholder)),
                                 **k)[::-1].replace(placeholder[::-1], '')
        )
    except ValueError as e:
        def raisee():
            raise

        # print(f">>>>>>>>>>>>> {e}")
        ##` if in_exc('placeholder too large for max width', else_raise=True):
        # if not 'placeholder too large for max width' in str(e): raise
        # else:
        #     return f"{s[len_start:]}...{s[:len_tail]}"
        return (
            s if len(s) <= length else
            f"{s[len_start:]}...{s[:len_tail]}" if 'placeholder too large for max width' in str(e)
            else raisee()
        )


def shortext(s: Any, len=def_shortext_len, **k) -> str:
    """
    >>> shortext('aaabbbcccddddeeee ffffgggghhhhiiiijjjj aaabbbcccdddd eeeeffffgggg hhhhiiii jjjj')
    'aaabbbcccddddeeee ffffgggghhhhiiiijjjj [...] hhhhiiii jjjj'
    """
    if isinstance(s, int) and not isinstance(len, int): a, len = len, s
    return shorten(str(s), len, **k)


def value_shortext(s: Any, len=DEF_LENGHTS.VAR_VALUE, len_tail_percent=0.4) -> str:
    return shortext(s, len, len_tail_percent=len_tail_percent)


