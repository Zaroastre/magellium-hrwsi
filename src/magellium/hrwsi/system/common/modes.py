from enum import Enum

class RunMode(Enum):
    ARCHIVE = "ARCHIVE"
    NEAR_REAL_TIME = "NEAR_REAL_TIME"
    
    @staticmethod
    def from_string(value: str) -> "RunMode":
        run_mode: RunMode | None = None
        for mode in RunMode:
            if (mode.value.lower() == value.lower()):
                run_mode = mode
                break
        return run_mode