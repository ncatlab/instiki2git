from collections.abc import Iterable

class PercentCode:
    '''Percent coding operating on bytes.'''

    def __init__(self, *, special: int = ord('%'), reserved: Iterable[int] = []):
        self.special = special
        self.to_encode = frozenset([special] + list(reserved))

    def decode_iterable(self, jt: Iterable[int]) -> Iterable[int]:
        jt = iter(jt)
        for b in jt:
            if b == self.special:
                yield int(bytes(next(jt) for _ in range(2)), 16)
            else:
                yield b

    def encode_iterable(self, it: Iterable[int]) -> Iterable[int]:
        for x in it:
            if x in self.to_encode:
                yield self.special
                yield from f'{x:02x}'.encode()
            else:
                yield x

    def decode(self, xs: Iterable[int]) -> bytes:
        return bytes(self.decode_iterable(xs))

    def encode(self, xs: Iterable[int]) -> bytes:
        return bytes(self.encode_iterable(xs))
