from dataclasses import dataclass

from magellium.hrwsi.system.core.sentinel_tiles import SentinelTile
from magellium.hrwsi.system.core.trigger_validations import TriggerValidation


@dataclass(frozen=True)
class TileToValidationAssignment:
    sentinel_tile: SentinelTile
    trigger_validation: TriggerValidation

