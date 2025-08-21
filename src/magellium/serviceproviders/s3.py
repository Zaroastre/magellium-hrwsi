from abc import ABC, abstractmethod
from pathlib import Path

import boto3
from retry import retry

from magellium.hrwsi.system.common.logger import LoggerFactory

class S3ServiceProvider(ABC):

    def __init__(self):
        raise NotImplementedError()

    @abstractmethod
    def download_file(self) -> Path:
        raise NotImplementedError()
    
    @abstractmethod
    def upload_file(self) -> Path:
        raise NotImplementedError()

class WekeoS3Client(S3ServiceProvider):
    __instances = {}
    
    LOGGER = LoggerFactory.get_logger(__name__)


    def __new__(cls, url: str, token: str):
        key = (url, token)
        if key not in cls.__instances:
            cls.__instances[key] = super().__new__(cls)
        return cls.__instances[key]
    
    
    def __init__(self, configuration_file: Path):
        self.__configuration_file: Path = configuration_file

        self.LOGGER.info("S3 client initialized and authenticated")

    @retry(tries=3, delay=2, backoff=2)
    def download_file(self):
        return super().download_file()
    
    @retry(tries=3, delay=2, backoff=2)
    def upload_file(self):
        return super().upload_file()