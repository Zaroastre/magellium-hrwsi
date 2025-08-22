from datetime import datetime as DateTime
from enum import Enum
from os import environ
from pathlib import Path


from magellium.hrwsi.system.harvesters.application.ports.inputs.user_interface import UserInterface
from magellium.hrwsi.system.harvesters.infrastructure.adapters.outputs.repository import HarvesterRepository, PostgreSqlHarvesterRepository
from magellium.hrwsi.system.harvesters.application.business.services.harvester import HarvesterService, HarvesterServiceImpl
from magellium.hrwsi.system.common.modes import RunMode
from magellium.hrwsi.system.harvesters.application.process_manager import HarvesterProcessManager
from magellium.serviceproviders.vault import HashcorpVaultClient, VaultServiceProvider
from magellium.serviceproviders.s3 import S3Client, S3ServiceProvider
from magellium.hrwsi.system.common.logger import LoggerFactory




class EnvironmentVariablesNames(Enum):
    HRWSI_HARVESTER_RUN_MODE = "HRWSI_HARVESTER_RUN_MODE"
    
    HRWSI_HARVESTER_DATABASE_HOST = "HRWSI_HARVESTER_DATABASE_HOST"
    HRWSI_HARVESTER_DATABASE_PORT = "HRWSI_HARVESTER_DATABASE_PORT"
    HRWSI_HARVESTER_DATABASE_USER = "HRWSI_HARVESTER_DATABASE_USER"
    HRWSI_HARVESTER_DATABASE_PASSWORD = "HRWSI_HARVESTER_DATABASE_PASSWORD"
    HRWSI_HARVESTER_DATABASE_NAME = "HRWSI_HARVESTER_DATABASE_NAME"

    HRWSI_HARVESTER_ARCHIVE_START_DATE = "HRWSI_HARVESTER_ARCHIVE_START_DATE"
    HRWSI_HARVESTER_ARCHIVE_END_DATE = "HRWSI_HARVESTER_ARCHIVE_END_DATE"

    VAULT_URL = "VAULT_URL"
    VAULT_TOKEN = "VAULT_TOKEN"

    S3_CONFIGURATION_FILE_PATH = "S3_CONFIGURATION_FILE_PATH"




class CommandLineUserInterface(UserInterface):

    LOGGER = LoggerFactory.get_logger(__name__)

    def __init__(self):
        self.__manager: HarvesterProcessManager|None = None

    def start(self) -> None:
        if (self.__manager is None):
            self.LOGGER.info("Starting Command Line User Interface...")

            run_mode_value: str | None = environ.get(EnvironmentVariablesNames.HRWSI_HARVESTER_RUN_MODE.value)
            if (run_mode_value is None):
                raise ValueError("HRWSI_HARVESTER_RUN_MODE environment variable is not set")
            run_mode: RunMode = RunMode.from_string(run_mode_value)

            database_host_value: str | None = environ.get(EnvironmentVariablesNames.HRWSI_HARVESTER_DATABASE_HOST.value)
            if (database_host_value is None):
                raise ValueError("HRWSI_HARVESTER_DATABASE_HOST environment variable is not set")
            
            database_port_value: str | None = environ.get(EnvironmentVariablesNames.HRWSI_HARVESTER_DATABASE_PORT.value)
            if (database_port_value is None):
                raise ValueError("HRWSI_HARVESTER_DATABASE_PORT environment variable is not set")
            try:
                database_port_value = int(database_port_value)
            except ValueError:
                raise ValueError("HRWSI_HARVESTER_DATABASE_PORT environment variable must be an integer")
            if ((database_port_value < 1) or (database_port_value > 65534)):
                raise ValueError("HRWSI_HARVESTER_DATABASE_PORT environment variable must be between 1 and 65534")

            database_username_value: str | None = environ.get(EnvironmentVariablesNames.HRWSI_HARVESTER_DATABASE_USER.value)
            if (database_username_value is None):
                raise ValueError("HRWSI_HARVESTER_DATABASE_USER environment variable is not set")
            
            database_password_value: str | None = environ.get(EnvironmentVariablesNames.HRWSI_HARVESTER_DATABASE_PASSWORD.value)
            if (database_password_value is None):
                raise ValueError("HRWSI_HARVESTER_DATABASE_PASSWORD environment variable is not set")
            
            database_name_value: str | None = environ.get(EnvironmentVariablesNames.HRWSI_HARVESTER_DATABASE_NAME.value)
            if (database_name_value is None):
                raise ValueError("HRWSI_HARVESTER_DATABASE_NAME environment variable is not set")



            vault_url_value: str | None = environ.get(EnvironmentVariablesNames.VAULT_URL.value)
            if (vault_url_value is None):
                raise ValueError("VAULT_URL environment variable is not set")
            
            vault_token_value: str | None = environ.get(EnvironmentVariablesNames.VAULT_TOKEN.value)
            if (vault_token_value is None):
                raise ValueError("VAULT_TOKEN environment variable is not set")
            

            s3_configuration_file_path_value: str | None = environ.get(EnvironmentVariablesNames.S3_CONFIGURATION_FILE_PATH.value)
            if (s3_configuration_file_path_value is None):
                raise ValueError("S3_CONFIGURATION_FILE_PATH environment variable is not set")

            archive_start_date_value: str | None = environ.get(EnvironmentVariablesNames.HRWSI_HARVESTER_ARCHIVE_START_DATE.value)
            archive_end_date_value: str | None = environ.get(EnvironmentVariablesNames.HRWSI_HARVESTER_ARCHIVE_END_DATE.value)


            vault: VaultServiceProvider = HashcorpVaultClient(
                url=vault_url_value,
                token=vault_token_value
            )

            s3: S3ServiceProvider = S3Client(
                configuration_file=Path(s3_configuration_file_path_value)
            )

            repository: HarvesterRepository = PostgreSqlHarvesterRepository(
                host=database_host_value,
                port=int(database_port_value),
                username=database_username_value,
                password=database_password_value,
                database_name=database_name_value
            )

            service: HarvesterService = HarvesterServiceImpl(
                run_mode=run_mode,
                repository=repository,
                vault=vault,
                s3=s3
            )

            archive_start_date: DateTime|None = None
            archive_end_date: DateTime|None = None

            if (run_mode == RunMode.ARCHIVE):
                if (archive_start_date_value is not None):
                    try:
                        archive_start_date = DateTime.fromisoformat(archive_start_date_value)
                    except ValueError:
                        raise ValueError("HRWSI_HARVESTER_ARCHIVE_START_DATE environment variable must be a date in the format YYYY-MM-DD")

                if (archive_end_date_value is not None):
                    try:
                        archive_end_date = DateTime.fromisoformat(archive_end_date_value)
                    except ValueError:
                        raise ValueError("HRWSI_HARVESTER_ARCHIVE_END_DATE environment variable must be a date in the format YYYY-MM-DD")

                if (archive_start_date and archive_end_date and (archive_start_date > archive_end_date)):
                    raise ValueError("ARCHIVE start date must be earlier than end date")


            self.__manager = HarvesterProcessManager(
                service=service,
                harvest_from_date=archive_start_date,
                harvest_to_date=archive_end_date,
                vault=vault,
                s3=s3
            )
            self.__manager.start_harvesting()


    def stop(self) -> None:
        self.LOGGER.info("Stopping Command Line User Interface...")
        if (self.__manager is not None):
            self.__manager.stop_harvesting()