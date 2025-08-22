from abc import ABC, abstractmethod
from pathlib import Path

import boto3
from retry import retry

from magellium.hrwsi.system.common.logger import LoggerFactory

class S3ServiceProvider(ABC):

    def __init__(self):
        raise NotImplementedError()

    @abstractmethod
    def download_file(self, bucket_name: str, source_file_path: str, destination_file_path: Path) -> Path:
        raise NotImplementedError()
    
    @abstractmethod
    def upload_file(self, bucket_name: str, source_file_path: Path, destination_file_path: str) -> Path:
        raise NotImplementedError()

class S3Client(S3ServiceProvider):
    __instances = {}
    
    LOGGER = LoggerFactory.get_logger(__name__)

    def __new__(cls, endpoint_url: str, access_key: str, secret_key: str, region: str):
        key = (endpoint_url, access_key, secret_key, region)
        if key not in cls.__instances:
            cls.__instances[key] = super().__new__(cls)
        return cls.__instances[key]
    
    
    def __init__(self, endpoint_url: str, access_key_id: str, secret_access_key: str, region_name: str):
        self.s3 = boto3.resource(
                service_name='s3',
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                endpoint_url=endpoint_url,
                region_name=region_name
            )

        self.LOGGER.info("S3 client initialized and authenticated")

    @retry(tries=3, delay=2, backoff=2, jitter=(1, 3))
    def download_file(self, bucket_name: str, source_file_path: str, destination_file_path: Path):
        return super().download_file()
    
    @retry(tries=3, delay=2, backoff=2, jitter=(1, 3))
    def upload_file(self, bucket_name: str, source_file_path: Path, destination_file_path: str):
        return super().upload_file()