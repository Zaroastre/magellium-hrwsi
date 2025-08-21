from dataclasses import dataclass
from enum import Enum
from datetime import datetime as DateTime


class ProcessingStatus(Enum):
    NOT_PROCESSED = "NOT_PROCESSED"
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"

class Flavour(Enum):
    HMA_LARGE = "hma.large"
    EO1_LARGE = "eo1.large"


@dataclass
class SentinelTile:
    pass

@dataclass
class ProcessingRoutine:
    pass

@dataclass
class ProcessingTask:
    pass

@dataclass
class TriggerValidation:
    pass

@dataclass
class TriggerCondition:
    pass

@dataclass
class NomadJobDispatcher:
    pass