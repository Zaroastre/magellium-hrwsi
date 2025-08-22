from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class _ProcessingStatusValue:
    id: int
    code: int
    name: str
    description: str

class ProcessingStatus(Enum):
    STARTED: _ProcessingStatusValue = _ProcessingStatusValue(
        1, 0, "started", "The tile processing has been started but is not yet complete."
    )
    PROCESSED: _ProcessingStatusValue = _ProcessingStatusValue(
        2, 1, "processed", "The tile has been successfully processed and validated."
    )
    PENDING: _ProcessingStatusValue = _ProcessingStatusValue(
        3, 2, "pending", "Tile processing is pending."
    )
    INTERNAL_ERROR: _ProcessingStatusValue = _ProcessingStatusValue(
        4, 110, "internal_error", "An internal error occurred in the system while processing the tile."
    )
    EXTERNAL_ERROR: _ProcessingStatusValue = _ProcessingStatusValue(
        5, 210, "external_error", "Tile processing failed due to an external source or missing data."
    )
    FAILED: _ProcessingStatusValue = _ProcessingStatusValue(
        6, 99, "terminated", "Unable to treat the tile despite attempts."
    )