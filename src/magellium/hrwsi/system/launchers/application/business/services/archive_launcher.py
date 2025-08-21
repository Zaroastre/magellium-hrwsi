from magellium.hrwsi.system.launchers.application.business.services.launcher import AbstractHarvesterService
from magellium.hrwsi.system.common.modes import RunMode
from magellium.hrwsi.system.launchers.application.ports.outputs.repository import Repository


class ArchiveHarvesterService(AbstractHarvesterService):
    def __init__(self, repository: Repository):
        super().__init__(RunMode.ARCHIVE, repository)

    def harvest(self) -> None:
        print("Harvesting in Archive mode...")