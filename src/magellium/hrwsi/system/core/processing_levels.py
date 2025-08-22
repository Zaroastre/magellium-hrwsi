from enum import Enum

class ProcessingLevel(Enum):
    L0 = 'L0'
    L1 = 'L1'
    L1A = 'L1A'
    L1B = 'L1B'
    L1C = 'L1C'
    L1S = 'L1S'
    L2A = 'L2A'
    L2B = 'L2B'
    L2C = 'L2C'

    @staticmethod
    def of(value: str) -> 'ProcessingLevel | None':
        level: ProcessingLevel | None = None
        for member in ProcessingLevel:
            if member.value.lower() == value.lower():
                level = member
                break
        return level