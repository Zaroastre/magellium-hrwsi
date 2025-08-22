from dataclasses import dataclass
from datetime import datetime as DateTime

from magellium.hrwsi.system.core.run_modes import RunMode
from magellium.hrwsi.system.core.triggering_conditions import TriggeringCondition


@dataclass
class TriggerValidation:
    id: int
    triggering_condition: TriggeringCondition
    validation_date: DateTime
    run_mode: RunMode
    artificial_measurement_day: int


    @staticmethod
    def from_dict_record(record: dict) -> "TriggerValidation":
        return TriggerValidation()
    