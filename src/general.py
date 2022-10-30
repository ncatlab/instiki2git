import itertools
from typing import Iterator, Optional, TypeVar

T = TypeVar('T')

def iter_inhabited(it: Iterator[T]) -> (bool, Iterator[T]):
    """
    Test whether an iterable is inhabited.
    Since this modifies the original iterable, a replacement iterable is returned.
    """
    try:
        x = next(it)
    except StopIteration:
        return (False, tuple())
    return (True, itertools.chain((x,), it))

def iter_to_maybe(it: Iterator[T]) -> Optional[T]:
    try:
        x = next(it)
    except StopIteration:
        return None
    () = it
    return x
