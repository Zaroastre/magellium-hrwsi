from magellium.hrwsi.system.harvesters.application.business.services.harvester import AbstractHarvesterService
from magellium.hrwsi.system.common.modes import RunMode
from magellium.hrwsi.system.harvesters.application.ports.outputs.repository import Repository

class NearRealTimeHarvesterService(AbstractHarvesterService):
    def __init__(self, repository: Repository):
        super().__init__(RunMode.NEAR_REAL_TIME, repository)

    def harvest(self) -> None:
        print("Harvesting in NRT mode...")