#!/usr/bin/env python3
"""This script is to be used at integration stage to generate configuration YAML files at will."""

from os import environ
from abc import ABC, abstractmethod

from magellium.hrwsi.system.common.logger import LoggerFactory

class Generator(ABC):

    @abstractmethod
    def generate(self):
        raise NotImplementedError()

class ConfigurationFileGenerator(Generator):

    @abstractmethod
    def _build_yaml_conf(self):
        raise NotImplementedError()

class AbstractConfigurationFileGenerator(ConfigurationFileGenerator):
    """Main class of the YAML config file generator for a processing routine.
    """

    S3_AUX_ROOT="s3://HRWSI-AUX"
    S3_LOGS_ROOT="s3://HRWSI-LOGS"


    def __init__(self, tile_id: str) -> None:
        """Initialization function."""
        self.logger = LoggerFactory.get_logger(__name__)
        self.tile_id = tile_id


    def generate(self):
        self._build_yaml_conf()
