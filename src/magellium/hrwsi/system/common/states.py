from enum import Enum

class TileProcessState(Enum):
    IDLE = "IDLE"
    IN_PROGRESS = "IN_PROGRESS"
    ERROR = "ERROR"
    FAILED = "FAILED"
    SUCCEDDED = "SUCCEDEDDED"