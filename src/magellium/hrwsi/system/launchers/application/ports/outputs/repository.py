from abc import ABC, abstractmethod

class LauncherRepository(ABC):

    @abstractmethod
    def save(self, data: str) -> None:
        raise NotImplementedError()