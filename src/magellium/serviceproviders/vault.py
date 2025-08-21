from abc import ABC, abstractmethod
from typing import Optional

from hvac import Client
from retry import retry
from magellium.hrwsi.system.common.logger import LoggerFactory


class VaultServiceProvider(ABC):

    def __init__(self):
        raise NotImplementedError()

    @abstractmethod
    def read_secret(self, key: str, path: str) -> Optional[dict[str, str]]:
        raise NotImplementedError()


class HashcorpVaultClient(VaultServiceProvider):

    __instances = {}

    LOGGER = LoggerFactory.get_logger(__name__)

    def __new__(cls, url: str, token: str):
        key = (url, token)
        if key not in cls.__instances:
            cls.__instances[key] = super().__new__(cls)
        return cls.__instances[key]

    def __init__(self, url: str, token: str):
        self.__url: str = url
        self.__token: str = token
        self.__client: Client = Client(url=self.__url, token=self.__token)

        if not self.__client.is_authenticated():
            error_message = "Vault client is not authenticated at init"
            self.LOGGER.error(error_message)
            raise RuntimeError(error_message)
        self.LOGGER.info("Vault client initialized and authenticated")

    @retry(tries=3, delay=2, backoff=2)
    def read_secret(self, key: str, path: str = "secrets") -> Optional[dict[str, str]]:
        """
        Lire un secret depuis Vault avec retry automatique
        """
        self.LOGGER.debug(f"Attempting to read secret '{key}' from path '{path}'")
        try:
            result = self.__client.secrets.kv.v2.read_secret_version(path=path, secret_id=key)
        except Exception as exception:
            self.LOGGER.warning(f"Failed to read secret '{key}': {exception}, retrying...")
            raise
        else:
            data = result["data"]["data"]
            self.LOGGER.info(f"Successfully read secret '{key}'")
            return data

