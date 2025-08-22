from dataclasses import dataclass

from magellium.hrwsi.system.core.flavours import Flavour


@dataclass
class TriggeringCondition:
    name: str
    processing_routine_name: str
    description: str

    @staticmethod
    def from_dict_record(record: dict) -> "TriggeringCondition":
        return TriggeringCondition()