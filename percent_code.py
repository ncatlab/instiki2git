from collections.abc import Iterable

class PercentCode:
    '''Percent coding operating on bytes.'''

    def __init__(self, *, special : int = ord('%'), reserved: Iterable[int] = []):
        self.special = special
        self.to_encode = frozenset([special] + list(reserved))

    def decode_iterable(self, jt: Iterable[int]) -> Iterable[int]:
        jt = iter(jt)
        while True:
            try:
                b = next(jt)
            except StopIteration:
                return
            if b == self.special:
                c1 = next(jt) - 0x30
                c0 = next(jt) - 0x30
                yield 0x10 * c1 + c0
            else:
                yield b

    def encode_iterable(self, it: Iterable[int]) -> Iterable[int]:
        for x in it:
            if x in self.to_encode:
                (c1, c0) = divmod(x, 16)
                yield self.special
                yield c1 + 0x30
                yield c0 + 0x30
            else:
                yield x

    def decode(self, xs: Iterable[int]) -> bytes:
        return bytes(self.decode_iterable(xs))

    def encode(self, xs: Iterable[int]) -> bytes:
        return bytes(self.encode_iterable(xs))

# These are the reserved bytes in git commit authors and committers.
git_identity = PercentCode(reserved = map(ord, ['\0', '\n', '<', '>']))
