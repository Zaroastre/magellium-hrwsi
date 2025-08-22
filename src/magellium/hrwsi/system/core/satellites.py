from enum import Enum

class Satellite(Enum):
    S1 = 'S1'
    S2 = 'S2'
    S3 = 'S3'
    S4 = 'S4'
    S5 = 'S5'
    S6 = 'S6'

    @staticmethod
    def of(value: str) -> 'Satellite | None':
        satellite: Satellite | None = None
        for member in Satellite:
            if member.value.lower() == value.lower():
                satellite = member
                break
        return satellite