from abc import ABC, abstractmethod
from datetime import datetime as DateTime

from magellium.hrwsi.system.harvesters.application.business.services.harvester import HarvesterService
from magellium.hrwsi.system.common.states import TileProcessState
from magellium.hrwsi.system.common.logger import LoggerFactory


class UseCase(ABC):
    def __init__(self):
        raise NotImplementedError()

    @abstractmethod
    def execute(self) -> None:
        raise NotImplementedError()
    

class AbstractUseCase(UseCase, ABC):

    LOGGER = LoggerFactory.get_logger(__name__)

    def __init__(self, service: HarvesterService):
        self.__service = service

    @property
    def _service(self) -> HarvesterService:
        return self.__service


class HarvestAllTilesWithIdleStateUseCase(AbstractUseCase):
    def __init__(self, service: HarvesterService):
        super().__init__(service)
        self.__tile_processing_state: TileProcessState = TileProcessState.IDLE

    def execute(self) -> None:
        self.LOGGER.info("Harvesting tiles in idle state...")
        self.__service.harvest_after_date

class HarvestAllTilesWithErrorStateUseCase(AbstractUseCase):
    def __init__(self, service: HarvesterService):
        super().__init__(service)
        self.__tile_processing_state: TileProcessState = TileProcessState.ERROR

    def execute(self) -> None:
        self.LOGGER.info("Harvesting tiles in error state...")


class HarvestTilesWithIdleStateBeforeDateUseCase(AbstractUseCase):
    def __init__(self, service: HarvesterService, to_date: DateTime):
        super().__init__(service)
        self.__tile_processing_state: TileProcessState = TileProcessState.IDLE
        self.__to_date = to_date


    def execute(self) -> None:
        self.LOGGER.info("Harvesting tiles in idle state...")

class HarvestTilesWithErrorStateBeforeDateUseCase(AbstractUseCase):
    def __init__(self, service: HarvesterService, to_date: DateTime):
        super().__init__(service)
        self.__tile_processing_state: TileProcessState = TileProcessState.ERROR
        self.__to_date = to_date


    def execute(self) -> None:
        self.LOGGER.info("Harvesting tiles in error state...")

class HarvestTilesWithIdleStateAfterDateUseCase(AbstractUseCase):
    def __init__(self, service: HarvesterService, from_date: DateTime):
        super().__init__(service)
        self.__tile_processing_state: TileProcessState = TileProcessState.IDLE
        self.__from_date = from_date


    def execute(self) -> None:
        self.LOGGER.info("Harvesting tiles in idle state...")

class HarvestTilesWithErrorStateAfterDateUseCase(AbstractUseCase):
    def __init__(self, service: HarvesterService, from_date: DateTime):
        super().__init__(service)
        self.__tile_processing_state: TileProcessState = TileProcessState.ERROR
        self.__from_date = from_date


    def execute(self) -> None:
        self.LOGGER.info("Harvesting tiles in error state...")

class HarvestTilesWithIdleStateBetweenDatesUseCase(AbstractUseCase):
    def __init__(self, service: HarvesterService, from_date: DateTime, to_date: DateTime):
        super().__init__(service)
        self.__tile_processing_state: TileProcessState = TileProcessState.IDLE
        self.__from_date = from_date
        self.__to_date = to_date


    def execute(self) -> None:
        self.LOGGER.info("Harvesting tiles in idle state...")

class HarvestTilesWithErrorStateBetweenDatesUseCase(AbstractUseCase):
    def __init__(self, service: HarvesterService, from_date: DateTime, to_date: DateTime):
        super().__init__(service)
        self.__tile_processing_state: TileProcessState = TileProcessState.ERROR
        self.__from_date = from_date
        self.__to_date = to_date


    def execute(self) -> None:
        self.LOGGER.info("Harvesting tiles in error state...")

