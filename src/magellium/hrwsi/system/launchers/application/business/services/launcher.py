from abc import ABC, abstractmethod
from datetime import datetime as DateTime

from magellium.hrwsi.system.common.modes import RunMode
from magellium.hrwsi.system.launchers.application.ports.outputs.repository import LauncherRepository


class LauncherService:

    def __init__(self):
        raise NotImplementedError()

    @abstractmethod
    def launch(self) -> None:
        raise NotImplementedError()

class LauncherServiceImpl(LauncherService, ABC):
    def __init__(self, run_mode: RunMode, repository: LauncherRepository):
        self.__run_mode: RunMode = run_mode
        self.__repository: LauncherRepository = repository

    @property
    def run_mode(self) -> RunMode:
        return self.__run_mode
    
    @property
    def _repository(self) -> LauncherRepository:
        return self.__repository