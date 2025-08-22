from enum import Enum

class RunMode(Enum):
    """
    Define the running mode of the harvester.
    """
    NRT = "NRT"
    ARCHIVE = "Archive"

    @staticmethod
    def of(name: str) -> 'RunMode':
        result = None
        if name:
            # Test sur les noms des membres
            if name.upper() in RunMode.__members__:
                result = RunMode[name.upper()]
        return result