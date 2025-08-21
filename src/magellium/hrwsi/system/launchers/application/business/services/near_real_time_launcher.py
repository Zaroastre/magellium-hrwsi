from magellium.hrwsi.system.harvesters.application.business.services.harvester import H
from magellium.hrwsi.system.common.modes import RunMode
from magellium.hrwsi.system.harvesters.application.ports.outputs.repository import HarvesterRepository
from magellium.hrwsi.system.common.logger import LoggerFactory

class NearRealTimeHarvesterService(AbstractHarvesterService):

    LOGGER = LoggerFactory.get_logger(__name__)

    def __init__(self, repository: HarvesterRepository):
        super().__init__(RunMode.NEAR_REAL_TIME, repository)

    def harvest(self) -> None:
        self.LOGGER.info("Harvesting in NRT mode...")