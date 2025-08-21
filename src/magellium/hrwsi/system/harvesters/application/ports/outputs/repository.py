from abc import ABC, abstractmethod
from datetime import datetime as DateTime

from magellium.hrwsi.system.core.entities import SentinelTile
from magellium.hrwsi.system.common.states import TileProcessState


class HarvesterRepository(ABC):

    def find_all_sentinel_tiles_by_state(self, state: TileProcessState) -> list[SentinelTile]:
        raise NotImplementedError()
    
    def find_all_sentinel_tiles_before_date_by_state(self, to_date: DateTime, state: TileProcessState) -> list[SentinelTile]:
        raise NotImplementedError()
    
    def find_all_sentinel_tiles_between_dates_by_state(self, from_date: DateTime, to_date: DateTime, state: TileProcessState) -> list[SentinelTile]:
        raise NotImplementedError()
    
    def find_all_sentinel_tiles_after_date_by_state(self, from_date: DateTime, state: TileProcessState) -> list[SentinelTile]:
        raise NotImplementedError()