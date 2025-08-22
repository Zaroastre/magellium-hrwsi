#!/usr/bin/env python3
"""
The Launcher module is used to create and associate a Nomad job to each processing task.
"""
import argparse
import asyncio
import json
import os
import socket
from collections import namedtuple
from datetime import datetime
from pathlib import Path
from typing import List

import psycopg2
import psycopg2.extras
from dateutil.relativedelta import relativedelta

from magellium.hrwsi.system.apimanager.api_manager import ApiManager
from magellium.hrwsi.system.apimanager.hrwsi_database_api_manager import HRWSIDatabaseApiManager
from magellium.hrwsi.system.core.flavours import Flavour
from magellium.hrwsi.system.launcher.config_file_generation.cc_config_file_generation import CCConfigFileGeneration
from magellium.hrwsi.system.launcher.config_file_generation.fsc_config_file_generation import FSCConfigFileGeneration
from magellium.hrwsi.system.launcher.config_file_generation.gfsc_config_file_generation import GFSCConfigFileGeneration
from magellium.hrwsi.system.launcher.config_file_generation.sig0_config_file_generation import Sig0ConfigFileGeneration
from magellium.hrwsi.system.launcher.config_file_generation.sws_config_file_generation import SWSConfigFileGeneration
from magellium.hrwsi.system.launcher.config_file_generation.wds_config_file_generation import WDSConfigFileGeneration
from magellium.hrwsi.system.launcher.config_file_generation.wics1_config_file_generation import WICS1ConfigFileGeneration
from magellium.hrwsi.system.launcher.config_file_generation.wics1s2_config_file_generation import WICS1S2ConfigFileGeneration
from magellium.hrwsi.system.launcher.config_file_generation.wics2_config_file_generation import WICS2ConfigFileGeneration
from magellium.hrwsi.system.launcher.launcher import AbstractLauncher, Launcher
from magellium.hrwsi.system.settings.queries_and_constants import (
    GET_OLDEST_MEASUREMENT_DATE_FROM_UNPROCESSED_PT_REQUEST,
    GET_UNPROCESSED_ARCHIVE_PT_REQUEST,
)


class ArchiveLauncher(AbstractLauncher):
    """Launch processing task execution"""

    GET_UNPROCESSED_ARCHIVE_PT_REQUEST = GET_UNPROCESSED_ARCHIVE_PT_REQUEST
    GET_OLDEST_MEASUREMENT_DATE_FROM_UNPROCESSED_PT_REQUEST = GET_OLDEST_MEASUREMENT_DATE_FROM_UNPROCESSED_PT_REQUEST

    def __init__(self, flavour: Flavour, nomad_host: str, nomad_port: int, configuration_folder: Path):
        super().__init__(flavour, nomad_host, nomad_port, configuration_folder)

    def create_hcl_file(self, hcl_file_path: str, hcl_info: namedtuple) -> None:
        """
        Create the content of the nomad job
        """

        self.logger.info("Begin create hcl file")
        error_message = 'create_hcl_file error: {}'

        # Convert duration minutes to seconds
        nomad_job_timeout = str(hcl_info.duration * 2) + "s"

        content = self.HCL_FILE_TEMPLATE

        try:
            replacements = {
                "processing_task_group": "archive",
                "worker-group": "worker-archive",
                "flavour_content": hcl_info.flavour,
                "processing_task_name": f"processing_task_{hcl_info.processing_task_id}",
                "image_docker": hcl_info.docker_image,
                "name_of_processing_routine": hcl_info.processing_routine_name,
                "timeout_max": nomad_job_timeout,
                "ram": str(hcl_info.ram),
                "${NOMAD_TOKEN}": os.getenv("NOMAD_TOKEN"),

                # Usefull content for rabbit producer json
                "id_processing_task": str(hcl_info.processing_task_id),
                "id_trigger_validation": str(hcl_info.trigger_validation_id),
                "code_product_type": hcl_info.product_type_code
            }

            # Apply all substitutions in a single loop
            for placeholder, value in replacements.items():
                content = content.replace(placeholder, value)

            # Add:
            # - Parameters to the config file for accessing HRWSI/EODATA/CATALOGUE S3 buckets in the Docker container,
            # - Order to write the worker runner script,
            # - The config file for the processing routine.
            # TODO: update the .s3cfg_HRWSI and .s3cfg_EODATA config files.
            # TODO: copier le contenu du fichier de config pour la routine dans le fichier HCL
            replacement_tags = {
                "s3cfg_hrwsi": "s3cmd_hrwsi_config",
                "s3cfg_eodata": "s3cmd_eodata_config",
                "s3cfg_catalogue": "s3cmd_catalogue_config",
                "worker_script_path": "wait_script",
                "routine_config_file": "routine_config"
            }

            # Apply all replacements by dynamically accessing attributes
            for attr_name, tag in replacement_tags.items():
                file_path = getattr(self, attr_name)
                content = Launcher.replace_file_content(file_path, content=content, tag=tag)

            with open(hcl_file_path, 'w', encoding="utf-8") as hcl_file:
                hcl_file.write(content)

            self.logger.info("End create hcl file")

        except (NameError, FileNotFoundError, UnicodeDecodeError, Exception) as error:
            raise type(error)(error_message.format(error)) from error

    async def launch(self) -> None:  # pragma: no cover
        """
        Run Launcher workflow:
        - wait for raw2valid insertion notifications
        - create processing tasks
        """
        error_message = 'Archive Launcher error: {}'

        try:
            while True:
                # Init and start the tasks
                self.logger.info("Initializing the Archive Launcher")

                tasks = [
                    asyncio.create_task(self.handle_unprocessed_pt(), name="handle_unprocessed_pt_task"),
                    asyncio.create_task(self.handle_processing_task_input(), name="handle_pt_input_task")
                ]
                self.logger.info("Archive Launcher handle_unprocessed_pt_task and handle_pt_input_task started.")

                # Tasks are restarted at regular intervals
                await asyncio.sleep(self.interval)

                self.logger.info("Restarting Archive Launcher cycle...")

                # Stop and cancel the tasks cleanly
                await self.stop_tasks()
                [task.cancel() for task in tasks]
                await asyncio.gather(*tasks, return_exceptions=True)

                # The asyncio event is used to notify multiple asyncio tasks that some event has happened.
                # Reset the event for the next Archive launcher cycle
                self._stop_event = asyncio.Event()

        except (TypeError, RuntimeError, Exception) as error:
            self.logger.error(error_message.format(error))

    async def handle_unprocessed_pt(self) -> None:  # pragma no cover
        """
        This method periodically recovers all unprocessed processing tasks related to a machine flavour.
        """

        self.logger.info("Begin handle_unprocessed_pt")

        try:
            # When the Archive Launcher service is restarted, but also at regular intervals:
            # - the oldest measurement date of unprocessed archive processing tasks is retrieved.
            # - all the unprocessed archive processing tasks with a measurement date between the oldest date above
            #   and the oldest date + 1 month 1/2 (with deadline at 2025-01-14) are retrieved.
            with HRWSIDatabaseApiManager.database_connection() as (conn, cur):
                while not self._stop_event.is_set():
                    request = GET_OLDEST_MEASUREMENT_DATE_FROM_UNPROCESSED_PT_REQUEST.format(
                        self.flavour, self.formatted_eligible_tiles)
                    result = HRWSIDatabaseApiManager.execute_request_in_database(cur, request)
                    oldest_measurement_date = result.fetchone()[0]
                    if oldest_measurement_date:
                        date_str = str(oldest_measurement_date)
                        date = datetime.strptime(date_str, "%Y%m%d")

                        # Add a time interval to the oldest measurement date retrieved above
                        new_date = date + relativedelta(months=self.measurement_date_interval['months'],
                                                        days=self.measurement_date_interval['days'])

                        # Set the closing date to ensure that only the archive unprocessed processing tasks will be retrieved
                        measurement_closing_date = int(new_date.strftime("%Y%m%d")) if int(new_date.strftime("%Y%m%d")) < 20250115 else 20250114

                        # Retrieve unprocessed tasks
                        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as dict_cur:
                            request = GET_UNPROCESSED_ARCHIVE_PT_REQUEST.format(
                                self.flavour, self.formatted_eligible_tiles, oldest_measurement_date, measurement_closing_date)
                            dict_cur = HRWSIDatabaseApiManager.execute_request_in_database(dict_cur, request)
                            rows = dict_cur.fetchall()

                            for row in rows:
                                # Create a dict with column names as keys and values as values
                                unprocessed_pt = dict(row)
                                pt_id = unprocessed_pt.get('id')

                                # Ensure that no processing task id duplicates are processed.
                                if pt_id not in self.processing_tasks_set:
                                    self.processing_tasks_queue.put_nowait(json.dumps(unprocessed_pt))
                                    self.processing_tasks_set.add(pt_id)

                    # Waiting for the next check
                    await asyncio.sleep(self.pt_reprocessing_waiting_time)

        except (psycopg2.OperationalError, TypeError, Exception) as error:
            raise type(error)('Failed to recover unprocessed tasks: {}'.format(error)) from error

    def handle_gfsc(self, hcl_info: List[namedtuple], tile_id: str, reference: any, **kwargs):
        gfsc_previous_processing_date_str = str(reference.gfsc_previous_processing_date)
        gfsc_previous_processing_date = datetime.strptime(gfsc_previous_processing_date_str, '%Y-%m-%d %H:%M:%S')
        gfsc_processing_date_str = datetime.strftime(gfsc_previous_processing_date,'%Y-%m-%d')

        # Getting the input paths
        fsc_list = [hcl_info_el.input_path if hcl_info_el.input_path[-1] == "/" else hcl_info_el.input_path + "/"
                    for hcl_info_el in hcl_info
                    if "FSC" in os.path.basename(
                        hcl_info_el.input_path if hcl_info_el.input_path[-1] != "/"
                        else hcl_info_el.input_path[:-2])]
        sws_list = [hcl_info_el.input_path if hcl_info_el.input_path[-1] == "/" else hcl_info_el.input_path + "/"
                    for hcl_info_el in hcl_info
                    if "SWS" in os.path.basename(
                        hcl_info_el.input_path if hcl_info_el.input_path[-1] != "/"
                        else hcl_info_el.input_path[:-2])]
        # Config file generation
        return GFSCConfigFileGeneration(tile_id=tile_id,
                                        processing_date=gfsc_processing_date_str,
                                        sws_list=sws_list,
                                        fsc_list=fsc_list,
                                        aggregation_timespan=self.GFSC_AGGREGATION_TIMESPAN)

def main():  # pragma: no cover
    # Load config file

    parser = argparse.ArgumentParser(description="Launch a job with a specific flavour.")
    parser.add_argument(
        "-f", "--flavour",
        type=str,
        choices=[flavour.value for flavour in Flavour],
        dest="flavour",
        required=True,
        help="The flavour of the job (options: %(choices)s)."
    )

    parser.add_argument(
        "-c", "--configuration-folder-path",
        type=Path,
        dest="configuration_folder",
        required=True,
        help="The path of the configuration folder."
    )

    nomad_host: str = os.environ.get("NOMAD_HOST")
    nomad_port: str = os.environ.get("NOMAD_PORT")

    if (nomad_host is None):
        raise IOError("NOMAD_HOST environment variable is not set")

    if (nomad_port is None):
        raise IOError("NOMAD_PORT environment variable is not set")

    args = parser.parse_args()

    launcher: Launcher = ArchiveLauncher(
        flavour=Flavour.of(args.flavour), 
        nomad_host=socket.gethostbyname(nomad_host), 
        nomad_port=int(nomad_port),
        configuration_folder=args.configuration_folder
    )
    
    asyncio.run(launcher.launch())


if (__name__ == "__main__"):  # pragma: no cover
    main()