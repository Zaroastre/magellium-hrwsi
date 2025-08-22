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
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psycopg2
import requests

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
    GET_IN_ERROR_PT_REQUEST,
    GET_UNDISPATCHED_PT_REQUEST,
    GET_UNFINISHED_PT_REQUEST,
    GET_UNFINISHED_PT_WITH_EXIT_CODE,
    GET_UNFINISHED_WITH_CALLBACK_PT_REQUEST,
    INSERT_PROCESSING_STATUS_WORKFLOW,
    LISTEN_LAUNCHER_PT_REQUEST,
    SQL_NOTIFY_REQ,
)


class NRTLauncher(AbstractLauncher):
    """Launch processing task execution"""

    LISTEN_REQUEST = LISTEN_LAUNCHER_PT_REQUEST

    GET_UNDISPATCHED_PT_REQUEST = GET_UNDISPATCHED_PT_REQUEST
    SQL_NOTIFY_REQ = SQL_NOTIFY_REQ
    GET_UNFINISHED_PT_REQUEST = GET_UNFINISHED_PT_REQUEST
    GET_UNFINISHED_WITH_CALLBACK_PT_REQUEST = GET_UNFINISHED_WITH_CALLBACK_PT_REQUEST
    GET_IN_ERROR_PT_REQUEST = GET_IN_ERROR_PT_REQUEST
    GET_UNFINISHED_PT_WITH_EXIT_CODE = GET_UNFINISHED_PT_WITH_EXIT_CODE
    INSERT_PROCESSING_STATUS_WORKFLOW = INSERT_PROCESSING_STATUS_WORKFLOW

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
                "processing_task_group": "nrt-3h",
                "worker-group": "worker-nrt",
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

    def get_job_dispatch_duration(self, job_uuid: str,) -> Optional[float]:
        """
        Get the duration in seconds since a job has been dispatch on Nomad

        Args:
            job_uuid: Nomad job UUID

        Returns:
            Duration in seconds since dispatch, or None if the job does not exist
        """
        try:
            allocation = self.nomad_client.allocation.get_allocation(job_uuid)
            nomad_job_dispatch_time = allocation["Job"].get('DispatchTime', None)


            if nomad_job_dispatch_time is None:
                # If DispatchTime is None, the job was never dispatched to begin with
                # We check for the submit time
                nomad_job_submit_time = allocation["Job"].get('SubmitTime')
                if nomad_job_submit_time is None:
                    return None
                nomad_job_dispatch_time = nomad_job_submit_time

            # From ns to s
            dispatch_time_seconds = nomad_job_dispatch_time / 1_000_000_000
            dispatch_datetime = datetime.fromtimestamp(dispatch_time_seconds, tz=timezone.utc)

            # Duration computation
            current_time = datetime.now(timezone.utc)
            duration = (current_time - dispatch_datetime).total_seconds()

            return duration

        except requests.exceptions.RequestException as error:
            self.logger.error(f"Error when requesting the Nomad server Nomad: {error}")
            return None
        except (KeyError, ValueError, json.JSONDecodeError) as error:
            self.logger.error(f"Error when parsing the answer: {error}")
            return None
        except Exception as error:
            self.logger.error(f"Unexpected error: {error}")
            return None

    async def launch(self) -> None:
        """
        Run launcher workflow:
        - wait for new processing task insertion
        - create and associate a Nomad job to each processing task
        """

        error_message = 'NRT Launcher error: {}'

        try:
            while True:
                # Init and start the tasks
                self.logger.info("Initializing the NRT Launcher")

                tasks = [
                    asyncio.create_task(self.handle_notify(), name="handle_notify_task"),
                    asyncio.create_task(self.handle_processing_task_input(), name="handle_processing_task"),
                    asyncio.create_task(self.handle_undispatched_pt(), name="handle_undispatched_pt_task"),
                    asyncio.create_task(self.handle_lost_pt(), name="handle_lost_pt_task"),
                    asyncio.create_task(self.handle_in_error_pt(), name="handle_in_error_pt_task")
                ]
                self.logger.info("NRT Launcher handle_processing_task, handle_undispatched_pt_task and "
                                 "handle_notify_task started.")

                # Tasks are restarted at regular intervals
                await asyncio.sleep(self.interval)

                self.logger.info("Restarting NRT Launcher cycle...")

                # Stop and cancel the tasks cleanly
                await self.stop_tasks()
                [task.cancel() for task in tasks]
                await asyncio.gather(*tasks, return_exceptions=True)

                # The asyncio event is used to notify multiple asyncio tasks that some event has happened.
                # Reset the event for the next NRT launcher cycle
                self._stop_event = asyncio.Event()

        except (TypeError, RuntimeError, Exception) as error:
            self.logger.error(error_message.format(error))
    
    async def handle_notify(self) -> None:
        """
        When a processing task insertion notification pop, collect input
        and insert it into a queue.
        """

        self.logger.info("Begin handle_notify : Receive input insertion notification")
        error_message = "Error in handle_notify : {}"

        try:
            # Connect to Database
            with HRWSIDatabaseApiManager.database_connection() as (conn, cur):
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

                # Listen channel
                _ = HRWSIDatabaseApiManager.execute_request_in_database(cur, self.LISTEN_REQUEST)

                # If the NRT launcher service is restarted, all unprocessed processing tasks are retrieved.
                HRWSIDatabaseApiManager.get_statements_to_notify_again(GET_UNDISPATCHED_PT_REQUEST.format(self.flavour),
                                                                       SQL_NOTIFY_REQ, self.logger)

                while not self._stop_event.is_set():
                    # Prevents conn.poll from blocking the asyncio event loop
                    await asyncio.to_thread(conn.poll)

                    while conn.notifies:
                        notify = conn.notifies.pop(0)
                        flavour = json.loads(notify.payload).get('flavour')
                        pt_id = json.loads(notify.payload).get('id')
                        if flavour == self.flavour and pt_id not in self.processing_tasks_set:
                            self.processing_tasks_queue.put_nowait(notify.payload)
                            self.processing_tasks_set.add(pt_id)

        except (KeyError, psycopg2.OperationalError, TypeError) as error:
            self.logger.error(error_message.format(error))
        except Exception as error:
            self.logger.error(error_message.format(error))

    async def handle_undispatched_pt(self) -> None:  # pragma no cover
        """
        This method periodically recovers all undispatched processing tasks related to a machine flavour.
        """

        self.logger.info("Begin handle_undispatched_pt")

        try:
            # When the NRT Launcher service is restarted, but also at regular intervals:
            # - all the undispatched nrt processing tasks with a measurement date >= 2025-01-15 are retrieved.
            while not self._stop_event.is_set():
                request = GET_UNDISPATCHED_PT_REQUEST.format(self.flavour)
                HRWSIDatabaseApiManager.get_statements_to_notify_again(request, SQL_NOTIFY_REQ, self.logger)
                self.logger.info("Successfully recovered undispatched tasks")

                # ✅ Wait before recovering the undispatched processing tasks
                await asyncio.sleep(self.pt_reprocessing_waiting_time)

        except Exception as error:
            self.logger.error(f"Error in handle_undispatched_pt: {error}")

    async def handle_in_error_pt(self) -> None:  # pragma no cover
        """
        This method periodically recovers all processing tasks in error related to a machine flavour.
        """

        self.logger.info("Begin handle_in_error_pt")

        try:
            # When the NRT Launcher service is restarted, but also at regular intervals:
            # - all the undispatched nrt processing tasks with a measurement date >= 2025-01-15 are retrieved.
            while not self._stop_event.is_set():
                request = GET_IN_ERROR_PT_REQUEST.format(self.flavour)
                HRWSIDatabaseApiManager.get_statements_to_notify_again(request, SQL_NOTIFY_REQ, self.logger)
                self.logger.info("Successfully recovered in error tasks")

                # ✅ Wait before recovering the undispatched processing tasks
                await asyncio.sleep(self.pt_reprocessing_waiting_time)

        except Exception as error:
            self.logger.error(f"Error in handle_in_error_pt: {error}")

    async def handle_lost_pt(self) -> None:  # pragma no cover
        """
        This method periodically recovers all dispatched yet unsuccessful processing tasks related to a machine flavour.
        It then either terminate them or program them for relaunch.
        """

        self.logger.info("Begin handle_lost_pt")

        try:
            with HRWSIDatabaseApiManager.database_connection() as (conn, cur):
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                internal_error_code_id_request = "SELECT id FROM hrwsi.processing_status WHERE name = 'internal_error'"
                cur = HRWSIDatabaseApiManager.execute_request_in_database(cur, internal_error_code_id_request)
                results = cur.fetchall()
                for result in results:
                    internal_error_status_id = result[0]
                conn.commit()
            
            # When the NRT Launcher service is restarted, but also at regular intervals:
            # - all the unsuccessful nrt processing tasks with a measurement date >= 2025-01-15 are retrieved.
            # - all the unsuccessful nrt processing tasks with a measurement date >= 2025-01-15 without callback are retrieved.
            # - all the unsuccessful nrt processing tasks with a measurement date >= 2025-01-15 without exit code are retrieved.
            while not self._stop_event.is_set():
                with HRWSIDatabaseApiManager.database_connection() as (conn, cur):
                    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                    request = self.GET_UNFINISHED_PT_REQUEST.format(self.flavour)
                    result = HRWSIDatabaseApiManager.execute_request_in_database(cur, request)
                    unfinished_pt_list = result.fetchall()
                    # First we get the jobs that never even answered with a pending status
                    request = self.GET_UNFINISHED_WITH_CALLBACK_PT_REQUEST.format(self.flavour)
                    result = HRWSIDatabaseApiManager.execute_request_in_database(cur, request)
                    unfinished_pt_with_callback_list = result.fetchall()
                    unfinished_pt_without_callback_list = set(unfinished_pt_list) - set(unfinished_pt_with_callback_list)
                    # Then we get the jobs that are stuck in pending or started
                    request = self.GET_UNFINISHED_PT_WITH_EXIT_CODE.format(self.flavour)
                    result = HRWSIDatabaseApiManager.execute_request_in_database(cur, request)
                    unfinished_pt_with_exit_code_list = result.fetchall()
                    unfinished_pt_without_exit_code_list = set(unfinished_pt_list) - set(unfinished_pt_with_exit_code_list)

                    # For all those processing tasks, if they have been in this state for longer than
                    #  - max(21 minutes, 3*<typical processing routine duration>) for jobs without exit code
                    #  - 1h for jobs without callback
                    # we relaunch them.
                    for lost_pt in unfinished_pt_without_exit_code_list | unfinished_pt_without_callback_list:
                        njd_id = lost_pt[-2]
                        pr_duration = lost_pt[-1]
                        duration_since_dispatching = self.get_job_dispatch_duration(njd_id)
                        # If this is None, this means that the Nomad server either has discarded the job (been inactive for too long) or has never have
                        # it in the first place. The processing task is to be relaunched.
                        pr_duration_in_minutes = max(7,pr_duration)
                        # Discrimination of the cases with priority to the jobs without callback
                        if lost_pt in unfinished_pt_without_exit_code_list:
                            relaunch_flag = not duration_since_dispatching or duration_since_dispatching > 3*60*pr_duration_in_minutes
                        else:
                            relaunch_flag = not duration_since_dispatching or duration_since_dispatching > 3600
                        # If the job is eligible, we put it in 404 internal error.
                        if relaunch_flag:
                            processing_status_workflow = tuple((njd_id, int(internal_error_status_id), datetime.now(), str(404)), )
                            cur = HRWSIDatabaseApiManager.execute_request_in_database(cur,
                                                                                      self.INSERT_PROCESSING_STATUS_WORKFLOW,
                                                                                      (processing_status_workflow,))
                    conn.commit()
                
                
                self.logger.info(f"Successfully put in error {len(set(unfinished_pt_without_exit_code_list | unfinished_pt_without_callback_list))} lost jobs tasks")


                # ✅ Wait before recovering the undispatched processing tasks
                await asyncio.sleep(self.pt_reprocessing_waiting_time)

        except Exception as error:
            self.logger.error(f"Error in handle_lost_pt: {error}")

def main():  # pragma: no cover
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

    launcher: Launcher = NRTLauncher(
        flavour=Flavour.of(args.flavour), 
        nomad_host=socket.gethostbyname(nomad_host), 
        nomad_port=int(nomad_port),
        configuration_folder=args.configuration_folder
    )

    asyncio.run(launcher.launch())


if (__name__ == "__main__"):  # pragma: no cover
    main()