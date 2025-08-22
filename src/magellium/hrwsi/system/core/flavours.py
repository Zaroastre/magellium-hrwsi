from enum import Enum

class Flavour(Enum):
    HMA_LARGE = 'hma.large'
    EO1_LARGE = 'eo1.large'

    @staticmethod
    def of(value: str) -> 'Flavour | None':
        flavour: Flavour | None = None
        for member in Flavour:
            if member.value.lower() == value.lower():
                flavour = member
                break
        return flavour
