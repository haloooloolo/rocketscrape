import argparse
from enum import Enum, IntEnum


class _EnumArg(IntEnum):
    @classmethod
    def argtype(cls, s: str) -> int:
        try:
            return cls[s]
        except KeyError:
            pass  # fallback to regular int
        try:
            return int(s)
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"{s!r} is not a valid {cls.__name__.lower()}")

    def __str__(self):
        return self.name


class Server(_EnumArg):
    rocketpool = 405159462932971535


class Channel(_EnumArg):
    general = 704196071881965589
    trading = 405163713063288832
    support = 468923220607762485


class Role(Enum):
    rocketpool = (405169632195117078, Server.rocketpool)


def sanitize_str(string: str) -> str:
    return string.encode('ascii', 'ignore').decode('ascii')
