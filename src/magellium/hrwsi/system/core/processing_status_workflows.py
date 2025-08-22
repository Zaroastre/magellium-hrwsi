from dataclasses import dataclass
from datetime import datetime as DateTime

from magellium.hrwsi.system.core.nomad_job_dispatches import NomadJobDispatch
from magellium.hrwsi.system.core.processing_status import ProcessingStatus


@dataclass
class ProcessingStatusWorkflow:
    id: int
    nomad_job_dispatch: NomadJobDispatch
    processing_status: ProcessingStatus
    date: DateTime
    message: str
    exit_code: int
