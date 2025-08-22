from dataclasses import dataclass

from magellium.hrwsi.system.core.nomad_job_dispatches import NomadJobDispatch
from magellium.hrwsi.system.core.processing_task import ProcessingTask


@dataclass(frozen=True)
class ProcessingTaskToNomadJobAssignment:
    nomad_job: NomadJobDispatch
    processing_task: ProcessingTask

