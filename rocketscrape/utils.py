import argparse
from enum import Enum, IntEnum


class _IntEnumArg(IntEnum):
    @classmethod
    def argtype(cls, s: str) -> int:
        try:
            return cls[s].value
        except KeyError:
            pass  # fallback to regular int
        try:
            return int(s)
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"{s!r} is not a valid {cls.__name__.lower()}")

    def __str__(self):
        return self.name


class Server(_IntEnumArg):
    rocketpool = 405159462932971535
    rp_imc = 1114304337041166368
    rp_gmc = 1109303903767507016
    rp_ping_service = 823815099991064586
    nodeset = 968587363536220252


class Channel(_IntEnumArg):
    general = 704196071881965589
    trading = 405163713063288832
    support = 468923220607762485


class Role(Enum):
    rpcore = (405169632195117078, Server.rocketpool)


def sanitize_str(string: str) -> str:
    return string.encode('ascii', 'ignore').decode('ascii')
