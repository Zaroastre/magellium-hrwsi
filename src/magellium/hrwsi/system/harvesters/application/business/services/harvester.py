from abc import ABC, abstractmethod
from datetime import datetime as DateTime

from magellium.hrwsi.system.common.modes import RunMode
from magellium.hrwsi.system.harvesters.application.ports.outputs.repository import HarvesterRepository
from magellium.hrwsi.system.core.entities import SentinelTile
from magellium.hrwsi.system.common.logger import LoggerFactory
from magellium.hrwsi.system.common.states import TileProcessState
from magellium.serviceproviders.vault import VaultServiceProvider
from magellium.serviceproviders.s3 import S3ServiceProvider


class HarvesterService:

    def __init__(self):
        raise NotImplementedError()
    
    @abstractmethod
    def harvest_by_state(self, state: TileProcessState) -> None:
        raise NotImplementedError()

    @abstractmethod
    def harvest_before_date_by_state(self, to_date: DateTime, state: TileProcessState) -> None:
        raise NotImplementedError()
    
    @abstractmethod
    def harvest_after_date_by_state(self, from_date: DateTime, state: TileProcessState) -> None:
        raise NotImplementedError()
    
    @abstractmethod
    def harvest_between_dates_by_state(self, from_date: DateTime, to_date: DateTime, state: TileProcessState) -> None:
        raise NotImplementedError()
    

class HarvesterServiceImpl(HarvesterService, ABC):

    LOGGER = LoggerFactory.get_logger(__name__)

    def __init__(self, run_mode: RunMode, repository: HarvesterRepository, vault: VaultServiceProvider, s3: S3ServiceProvider):
        self.__run_mode: RunMode = run_mode
        self.__repository: HarvesterRepository = repository
        self.__vault: VaultServiceProvider = vault
        self.__s3: S3ServiceProvider = s3


    @property
    def run_mode(self) -> RunMode:
        return self.__run_mode
    
    @property
    def _repository(self) -> HarvesterRepository:
        return self.__repository

    def harvest_by_state(self, state: TileProcessState) -> None:
        self.LOGGER.info(f"Harvesting all tiles with state: {state.name}...")
        sentinel_tiles: list[SentinelTile] = self.__repository.find_all_sentinel_tiles_by_state(state)
        self.LOGGER.info(f"Harvested {len(sentinel_tiles)} tiles with state {state.name}")

    def harvest_before_date_by_state(self, to_date: DateTime, state: TileProcessState) -> None:
        self.LOGGER.info(f"Harvesting tiles before {to_date.isoformat()} with state {state.name}...")
        sentinel_tiles: list[SentinelTile] = self.__repository.find_all_sentinel_tiles_before_date_by_state(to_date, state)
        self.LOGGER.info(f"Harvested {len(sentinel_tiles)} tiles before {to_date.isoformat()} with state {state.name}")

    def harvest_after_date_by_state(self, from_date: DateTime, state: TileProcessState) -> None:
        self.LOGGER.info(f"Harvesting tiles after {from_date.isoformat()} with state {state.name}...")
        sentinel_tiles: list[SentinelTile] = self.__repository.find_all_sentinel_tiles_after_date_by_state(from_date, state)
        self.LOGGER.info(f"Harvested {len(sentinel_tiles)} tiles after {from_date.isoformat()} with state {state.name}")

    def harvest_between_dates_by_state(self, from_date: DateTime, to_date: DateTime, state: TileProcessState) -> None:
        self.LOGGER.info(f"Harvesting tiles between {from_date.isoformat()} and {to_date.isoformat()} with state {state.name}...")
        sentinel_tiles: list[SentinelTile] = self.__repository.find_all_sentinel_tiles_between_dates_by_state(from_date, to_date, state)
        self.LOGGER.info(f"Harvested {len(sentinel_tiles)} tiles between {from_date.isoformat()} and {to_date.isoformat()} with state {state.name}")