from dataclasses import dataclass
from datetime import datetime as DateTime
from pathlib import Path
from uuid import UUID


@dataclass
class NomadJobDispatch:
    id: UUID
    nomad_job_dispatch: str
    dispatch_date: DateTime
    log_path: Path