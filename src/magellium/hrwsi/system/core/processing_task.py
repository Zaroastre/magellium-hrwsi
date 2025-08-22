from dataclasses import dataclass
from datetime import datetime as DateTime
from pathlib import Path

from magellium.hrwsi.system.core.trigger_validations import TriggerValidation


@dataclass
class ProcessingTask:
    id: int
    trigger_validation: TriggerValidation
    creation_date: DateTime
    processing_date: DateTime
    preceding_input_id: str
    has_ended: bool
    intermediate_files_path: list[Path]