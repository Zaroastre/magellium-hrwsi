import asyncio
import json
import logging
import os
import socket
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from collections import namedtuple
from datetime import datetime, timezone
from typing import List, Optional
from pathlib import Path

import hvac
import nomad
import psycopg2

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
from magellium.hrwsi.system.settings.queries_and_constants import (
    GET_CONFIG_PT_REPROCESSING_WAITING_TIME_PARAM,
    HCL_INFO_REQUEST,
    HCL_TEMPLATE,
    IS_THIS_PT_CURRENTLY_DEPLOYED,
    NOMAD_JOB_DISPATCH_REQUEST,
    PROCESSING_STATUS_WORKFLOW_REQUEST,
    PT_2_NOMAD_REQUEST,
    WORKER_SCRIPT_PATH,
)
from magellium.hrwsi.utils.file import FileUtil
from magellium.hrwsi.utils.logger import LogUtil
from magellium.hrwsi.utils.s3_client import S3Client
from magellium.hrwsi.utils.vault_client import VaultClient


class Launcher(ABC):
    # def __init__(self):
    #     raise NotImplementedError()

    @abstractmethod
    async def launch(self) -> None:
        pass

class AbstractLauncher(Launcher, ABC):

    LOGGER_LEVEL = logging.DEBUG
    HCL_FILE_TEMPLATE = HCL_TEMPLATE
    HCL_INFO_REQUEST = HCL_INFO_REQUEST
    IS_THIS_PT_CURRENTLY_DEPLOYED = IS_THIS_PT_CURRENTLY_DEPLOYED
    NOMAD_JOB_DISPATCH_REQUEST = NOMAD_JOB_DISPATCH_REQUEST
    PT_2_NOMAD_REQUEST = PT_2_NOMAD_REQUEST
    PROCESSING_STATUS_WORKFLOW_REQUEST = PROCESSING_STATUS_WORKFLOW_REQUEST
    GET_CONFIG_PT_REPROCESSING_WAITING_TIME_PARAM = GET_CONFIG_PT_REPROCESSING_WAITING_TIME_PARAM

    # TODO Add this parameter to the system params table and fetch if with the HCL_INFO request
    GFSC_AGGREGATION_TIMESPAN="7"

    def __init__(self, flavour: Flavour, nomad_host: str, nomad_port: int, configuration_folder: Path):
        self._flavour: Flavour = flavour
        self.s3cfg_hrwsi = ".s3cfg_HRWSI"
        self.s3cfg_eodata = ".s3cfg_EODATA"
        self.s3cfg_catalogue = ".s3cfg_CATALOGUE"
        self.logger = LogUtil.get_logger(f'Log_launcher_{self.flavour}', self.LOGGER_LEVEL, f"log_launcher/logs_{self.flavour}.log")
        self.format_date = '%Y-%m-%d %H:%M:%S'
        self.hcl_file = HCL_TEMPLATE
        self.worker_script_path = WORKER_SCRIPT_PATH
        self.default_hcl_file_path = configuration_folder.joinpath("/launcher/pt_id_{}.hcl")
        self.routine_config_file = "/tmp/configuration_file.yml"
        self.s3_cfg_path = f"/home/nrt_production_system/{self.s3cfg_eodata}"
        self.inspire_file_name = "INSPIRE.xml"
        self.tmp_inspire_path = "/tmp/tmp_L1C_inspire"
        self.processing_tasks_queue = asyncio.Queue()
        self.processing_tasks_set = set()
        with HRWSIDatabaseApiManager.database_connection() as (conn, cur):
            cur = HRWSIDatabaseApiManager.execute_request_in_database(cur, GET_CONFIG_PT_REPROCESSING_WAITING_TIME_PARAM)
            rows = cur.fetchall()
            config_params = {param: value for param, value in rows}
        self.pt_reprocessing_waiting_time = int(config_params['pt_reprocessing_waiting_time'])
        self._stop_event = asyncio.Event()
        config_data = ApiManager.read_config_file(configuration_folder)
        self.interval = config_data["async_loop"]["interval"]

        # Get IP address
        ip_address = socket.gethostbyname(socket.gethostname())
        self.logger.info("ip_address : %s", ip_address)

        # Nomad Client
        self.nomad_client = nomad.Nomad(host=nomad_host, port=nomad_port)

        # format the worker script file
        self._format_worker_script()

    @property
    def flavour(self) -> Flavour:
        return self._flavour

    def _format_worker_script(self) -> None: # pragma no cover
        """
        This method allows you to replace certain keywords in the worker script file with gitlab credentials,
        which are mandatory to download docker images of the processing routines.
        """

        try:
            vault_client = VaultClient()
            if vault_client.is_authenticated:
                secret = vault_client.read_secret('gitlab')

                container_registry_username = secret["container_registry_username"]
                container_registry_address = secret["container_registry_address"] + '/'
                container_registry_token = secret["container_registry_token"]

                with open(self.worker_script_path, 'r', encoding="utf-8") as worker_script_file:
                    content = worker_script_file.read()

                updated_content = content.replace("{container_registry_username}", container_registry_username)
                updated_content = updated_content.replace("{container_registry_address}", container_registry_address)
                updated_content = updated_content.replace("{container_registry_token}", container_registry_token)

                with open(self.worker_script_path, 'w', encoding="utf-8") as worker_script_file:
                    worker_script_file.write(updated_content)

                self.logger.info("Gitlab container registry credentials updated to the worker_script.sh file.")

        except hvac.exceptions.VaultError as error:
            self.logger.critical(error)
        except Exception as error:
            self.logger.critical(error)

    @abstractmethod
    def create_hcl_file(self, hcl_file_path: str, hcl_info: namedtuple) -> None:
        raise NotImplementedError()
    
    @staticmethod
    def _replace_file_content(file_path: str, content, tag: str,
                             access_mode: str = 'r', encoding_format: str = "utf-8") -> str:
        """
        Method to replace a tag in a file with its content.

        :param file_path: The path of the file.
        :param content: The content to replace the tag.
        :param tag: The tag to replace.
        :param access_mode: The mode in which to open the file (read, write, etc.).
        :param encoding_format: The encoding format of the file.

        return: The updated content.
        """
        error_message = 'Error replacing tag in {}: {}'

        try:
            with open(file_path, access_mode, encoding=encoding_format) as config_file:
                file_data = config_file.read()
            updated_content = content.replace(tag, file_data)

            return updated_content

        except (NameError, FileNotFoundError, UnicodeDecodeError) as error:
            raise type(error)(error_message.format(file_path, error)) from error
    

    def convert_ns_create_time(self, time_ns: int) -> str:
        """
        This function converts nanoseconds to a human-readable date and time.

        :param time_ns: The nanoseconds to convert.

        return: A string in the specified format.
        """
        from datetime import datetime

        # Convert nanoseconds to seconds
        time_s = time_ns / 1e9

        # Convert seconds into a datetime object
        formatted_date = datetime.fromtimestamp(time_s, tz=timezone.utc)

        # Format the date accordingly
        formatted_date = formatted_date.strftime(self.format_date)

        return formatted_date

    def send_hcl_file_to_nomad_server(self, hcl_file_name: str) -> dict:
        """
        Send the hcl to the nomad server to register the new job.

        :param hcl_file_name: The path of the hcl file

        return: The response dict from nomad server
        """

        self.logger.info("Begin send_hcl_file_to_nomad_server")

        # Parse hcl file
        try:
            with open(hcl_file_name, "r", encoding="utf-8") as fh:
                job_raw_nomad = fh.read()
                job_dict = self.nomad_client.jobs.parse(job_raw_nomad)

        except FileNotFoundError as error:
            raise type(error)('Hcl file not found: {}'.format(error)) from error
        # Due to a TypeError: exceptions must derive from BaseException, the following nomad exception cannot be tested
        # with pytest
        except nomad.api.exceptions.BadRequestNomadException as error:  # pragma: no cover
            raise type(error)('Failed to parse the nomad hcl file: {} \n {}'.format(
                error.nomad_resp.reason, error.nomad_resp.text)) from error

        # Register job
        job_dict = {"Job": job_dict}

        try:
            response = self.nomad_client.jobs.register_job(job_dict)
            self.logger.info("End send_hcl_file_to_nomad_server")

            return response

        # Due to a TypeError: exceptions must derive from BaseException, the following nomad exception cannot be tested
        # with pytest
        except nomad.api.exceptions.BadRequestNomadException as error:  # pragma: no cover
            raise type(error)("Failed to register nomad job: {} \n {}".format(
                error.nomad_resp.reason, error.nomad_resp.text)) from error
    
    def collect_uuid(self, processing_task_id: int) -> str: # pragma no cover
        """
        Collect the processing task's Nomad job UUID.
        """

        self.logger.info("Begin collect nomad job uuid")
        error_message = "Failed to collect nomad job UUID: "
        # to be sure that the job is allocated

        # Collect uuid
        try:
            nomad_uuid = None
            job_id = f"processing_task_{processing_task_id}"
            while not nomad_uuid:
                allocations = self.nomad_client.job.get_allocations(job_id)
                # if there are several allocations
                for alloc in allocations:
                    if alloc["ClientStatus"] in ("running", "pending"):
                        nomad_uuid = alloc['ID']

            self.logger.info(f"End collect nomad job uuid for processing_task processing_task_{processing_task_id}")
            return nomad_uuid


        except IndexError as error:
            raise Exception(error_message.format(error)) from error
        # Due to a TypeError: exceptions must derive from BaseException, the following nomad exception cannot be tested
        # with pytest
        except nomad.api.exceptions.BadRequestNomadException as error:  # pragma: no cover
            raise type(error)('Failed to parse the nomad hcl file: '.format(
                f"\n{error.nomad_resp.reason}\n{error.nomad_resp.text}")) from error
    
    def create_nomad_job_dispatch(self, cur: psycopg2.extensions.cursor, nomad_job_uuid: str) -> None:
        """
        Once the nomad job has been dispatched, create a new entry in the 'hrwsi.nomad_job_dispatch' table.
        """

        self.logger.info("Begin create Processing status workflow")
        error_message = "Failed to insert new nomad job dispatch into database: "

        try:
            # TODO : currently, can't find correct values to handle the log_path and nomad_job_dispatch content.
            registered_nomad_job = ((nomad_job_uuid,),)
            self.logger.debug("Processing nomad job dispatch tuple : %s", registered_nomad_job)

            # Insert in database
            _ = HRWSIDatabaseApiManager.execute_request_in_database(cur, self.NOMAD_JOB_DISPATCH_REQUEST,
                                                                    registered_nomad_job)

            self.logger.info("End create Processing nomad_job_dispatch")

        except (psycopg2.OperationalError, TypeError, Exception) as error:
            raise type(error)(error_message.format(error)) from error
    
    def create_processing_task_to_nomad(self, cur: psycopg2.extensions.cursor, nomad_job_uuid: str,
                                        processing_task_id: str) -> None:
        """
        Once the nomad job has been dispatched, create a new entry in the 'hrwsi.processingtask2nomad' table.
        """

        self.logger.info("Begin create processing task to nomad")
        error_message = "Failed to insert new processing task to nomad into database: "

        # Insert in database
        try:
            data_to_insert = ((nomad_job_uuid, processing_task_id),)
            _ = HRWSIDatabaseApiManager.execute_request_in_database(cur, self.PT_2_NOMAD_REQUEST, data_to_insert)

            self.logger.info("End create processing task to nomad")

        except (psycopg2.OperationalError, TypeError, Exception) as error:
            raise type(error)(error_message.format(error)) from error
    
    def create_processing_status_workflow_for_new_nomad_job(self, cur: psycopg2.extensions.cursor,
                                                            nomad_job_uuid: str) -> None:
        """
        Once the nomad job dispatch has been filled, create a new entry in the 'hrwsi.processing_status_workflow' table
        which details the workflow status of the processing task.
        """

        self.logger.info("Begin create Processing status workflow")
        error_message = "Failed to insert new processing status workflow into database: "

        nomad_job_existing_status = {'running': 'started', 'pending': 'pending',
                                     'dead': 'internal_error', 'complete': 'processed'}

        # Get nomad job status and submit time
        try:
            allocation = self.nomad_client.allocation.get_allocation(nomad_job_uuid)
            nomad_job_status = allocation["Job"].get("Status", "Unknown")
            nomad_job_submit_time = allocation["Job"].get('SubmitTime', None)
            if nomad_job_submit_time:
                nomad_job_submit_time = self.convert_ns_create_time(nomad_job_submit_time)
            else:
                nomad_job_submit_time = datetime.now().strftime(self.format_date)

            # Insert data into database
            data_to_insert = (
                (nomad_job_uuid, nomad_job_existing_status[nomad_job_status], nomad_job_submit_time),)
            _ = HRWSIDatabaseApiManager.execute_request_in_database(cur, self.PROCESSING_STATUS_WORKFLOW_REQUEST,
                                                                    data_to_insert)

            self.logger.info("End create Processing status workflow")

        except (psycopg2.OperationalError, TypeError, Exception) as error:
            raise type(error)(error_message.format(error)) from error
        # Due to a TypeError: exceptions must derive from BaseException, the following nomad exception cannot be tested
        # with pytest
        except nomad.api.exceptions.BadRequestNomadException as error:  # pragma: no cover
            raise type(error)('Failed to parse the nomad hcl file: '.format(
                f"\n{error.nomad_resp.reason}\n{error.nomad_resp.text}")) from error
    
    async def stop_tasks(self):
        """Stop cleanly the running task."""

        self.logger.info("Stopping current tasks...")
        self._stop_event.set()

    async def handle_processing_task_input(self) -> None:
        """
        Collect processing task input and create the nomad job.
        """

        error_message = 'handle_processing_task_input error: {}'

        try:
            with HRWSIDatabaseApiManager.database_connection() as (conn, cur):
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

                while not self._stop_event.is_set():
                    queue_item = await self.processing_tasks_queue.get()
                    await self.create_nomad_job(cur, queue_item)
                    pt_id = json.loads(queue_item).get('id')
                    self.processing_tasks_set.remove(pt_id)

        except (psycopg2.OperationalError, KeyError) as error:
            raise type(error)(error_message.format(error)) from error
        except Exception as error:
            raise Exception(error_message.format(error)) from error

    def calculate_l1c_product_measurement_date(self, inspire_file_path: str, l1c_measurement_date: str) -> str:
        """
        In the case of a partial L1C, 2 L1Cs have the same measurement date.
        To avoid producing 2 CCs with the same product path, we look at the beginPosition
        and endPosition dates in the INSPIRE.xml file.
        If beginPosition < l1c_measurement_date < endPosition:
          - Keep l1c_measurement_date as the product_measurement_date
        Else:
          - beginPosition is the new product_measurement_date
        """

        # Load xml file
        tree = ET.parse(inspire_file_path)
        root = tree.getroot()

        # Defining namespaces
        namespaces = {
            "gco": "http://www.isotc211.org/2005/gco",
            "gmd": "http://www.isotc211.org/2005/gmd",
            "gml": "http://www.opengis.net/gml",
            "xlink": "http://www.w3.org/1999/xlink",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance"
        }

        begin_position = root.find(".//gml:beginPosition", namespaces).text
        end_position = root.find(".//gml:endPosition", namespaces).text

        # Definition of corresponding formats
        format1 = "%Y-%m-%dT%H:%M:%S"
        format2 = "%Y%m%dT%H%M%S"

        # Convert to datetime
        begin_position_date = datetime.strptime(begin_position, format1)
        end_position_date = datetime.strptime(end_position, format1)
        measurement_date = datetime.strptime(l1c_measurement_date, format2)

        # Check if the measurement date in the L1C name is between beginPosition and endPosition
        if begin_position_date < measurement_date < end_position_date:
            return l1c_measurement_date
        # Else return the beginPosition as the product_measurement_date
        return begin_position_date.strftime("%Y%m%dT%H%M%S")

    async def create_nomad_job(self, cur: psycopg2.extensions.cursor, queue_item: str) -> None:
        """
        Create a nomad job for a processing task.
        """

        self.logger.info("Begin create_nomad_job")
        error_message = 'create_nomad_job error: {}'

        try:
            pt_input = json.loads(queue_item)
            self.logger.info("pt_input %s", pt_input)
            # TODO: remove this condition as <"processing_task" in pt_input> never seems to be met
            if "processing_task" in pt_input:
                trigger_validation_id = pt_input["processing_task"]['trigger_validation_fk_id']
                processing_task_id = pt_input["processing_task"]['id']
            else:
                trigger_validation_id = pt_input['trigger_validation_fk_id']
                processing_task_id = pt_input['id']
            self.logger.info("trigger_validation_id %s", trigger_validation_id)

            # Checking that this processing task has not already been processed while this item was in the queue
            has_pt_already_been_deployed_request = self.IS_THIS_PT_CURRENTLY_DEPLOYED.format(processing_task_id)
            result = HRWSIDatabaseApiManager.execute_request_in_database(cur, has_pt_already_been_deployed_request)
            has_pt_already_been_deployed = result.fetchone()[0]
            self.logger.info("has_pt_already_been_deployed %s", has_pt_already_been_deployed)
            if has_pt_already_been_deployed:
                self.logger.info("Processing task with ID %i has already been deployed, passing.", processing_task_id)
                return

            hcl_query = self.HCL_INFO_REQUEST % trigger_validation_id
            self.logger.info("hcl_query %s", hcl_query)
            
            # Get the necessary resources for the processing task
            result = HRWSIDatabaseApiManager.execute_request_in_database(cur, hcl_query)
            self.logger.info("result %s", result)
            col_names = [desc[0] for desc in result.description]
            self.logger.info("col_names %s", col_names)

            # Create a config file for processing routine
            result_data_list = result.fetchall()
            self.logger.info("result_data_list %s", result_data_list)
            hcl_data_list = []
            for result_data in result_data_list:
                Hcl = namedtuple('Hcl', col_names)
                hcl_data_list.append(Hcl(*result_data))
            self.logger.info("hcl_data_list %s", hcl_data_list)
            status = self.create_config_file_for_routine(hcl_info=hcl_data_list)
            self.logger.info("status %s", status)
            if status:
                return
            else:
                # Convert hcl_data to a named tuple
                # TODO might be wrong when processing_routine is re-launched due to an error.
                #  To be sure we get the last one chronologically
                Hcl = namedtuple('Hcl', col_names)
                self.logger.info("Hcl %s", Hcl)
                hcl_data = Hcl(*result_data_list[-1])
                self.logger.info("hcl_data %s", hcl_data)

                # Create the hcl file
                FileUtil.create_dir('nomad_job')
                processing_task_id = hcl_data.processing_task_id
                self.logger.info("processing_task_id %s", processing_task_id)
                hcl_file_path = self.default_hcl_file_path.format(processing_task_id)
                self.logger.info("hcl_file_path %s", hcl_file_path)
                self.create_hcl_file(hcl_file_path, hcl_data)

                # Send hcl file to the nomad server for the current job creation
                response = self.send_hcl_file_to_nomad_server(hcl_file_path)
                self.logger.info("response %s", response)
                if response.get("EvalID"):
                    # Delete the local hcl file
                    FileUtil.delete_file_if_exists(hcl_file_path)

                    # Get the nomad job uuid
                    nomad_job_uuid = self.collect_uuid(processing_task_id)

                    # Insert nomad job info to database
                    self.create_nomad_job_dispatch(cur, nomad_job_uuid)
                    self.create_processing_task_to_nomad(cur, nomad_job_uuid, processing_task_id)
                    self.create_processing_status_workflow_for_new_nomad_job(cur, nomad_job_uuid)

                else:
                    self.logger.error("Failed to send hcl file to nomad server for the processing task %s",
                                      processing_task_id)

        except psycopg2.errors.UniqueViolation: # pragma no cover
            self.logger.error("Trying to re-index nomad job with uuid %s, pass.", nomad_job_uuid)
        except KeyError as error: # pragma no cover
            self.logger.error("The json load has been corrupted. Discarding it : %s", error)
        except (psycopg2.OperationalError, TypeError, NameError, FileNotFoundError, UnicodeDecodeError,
                nomad.api.exceptions.BadRequestNomadException) as error:
            raise type(error)(error_message.format(error)) from error

    def create_config_file_for_routine(self, hcl_info: List[namedtuple], **kwargs) -> Optional[bool]:  # pragma: no cover
        """
        Create the content of the configuration file for the current processing routine.
        Return an object if a WICS1 or a CC processing routine haven't the necessary auxiliaries data to run.
        """
        if not hcl_info:
            self.logger.error("No HCL info provided.")
            return None

        reference = hcl_info[0]
        routine = reference.processing_routine_name
        tile_id = reference.tile

         # ---- Dispatcher ----
        handlers = {
            "SWS": self.handle_sws,
            "FSC": self.handle_fsc,
            "WICS1": self.handle_wics1,
            "WICS2": self.handle_wics2,
            "CC": self.handle_cc,
            "WDS": self.handle_wds,
            "SIG0": self.handle_sig0,
            "WICS1S2": self.handle_wics1s2,
            "GFSC": self.handle_gfsc,
        }

        handler = handlers.get(routine)
        if not handler:
            self.logger.error("Failed to create config file: unknown routine %s", routine)
            return None

        cfg = handler(hcl_info, **kwargs)
        if cfg is None:
            return None

        status = cfg.fill_conf_yaml()
        return status


    def format_measurement_date(date: str) -> str:
        """Format YYYYMMDD into YYYY-MM-DD"""
        return f"{date[:4]}-{date[4:6]}-{date[6:]}"

    def get_basename(path: str) -> str:
        return os.path.basename(path)

    # ---- Routines handlers ----
    def handle_sws(self, hcl_info: List[namedtuple], tile_id: str, reference: any ,**kwargs):
        return SWSConfigFileGeneration(
            tile_id=tile_id,
            sigma0_name=self.get_basename(reference.input_path),
            measurement_date=self.format_measurement_date(str(reference.measurement_day))
        )

    def handle_fsc(self, hcl_info: List[namedtuple], tile_id: str, reference: any ,**kwargs):
        return FSCConfigFileGeneration(
            tile_id=tile_id,
            l2a_name=self.get_basename(reference.input_path),
            measurement_date=self.format_measurement_date(str(reference.measurement_day))
        )

    def handle_wics1(self, hcl_info: List[namedtuple], tile_id: str, reference: any ,**kwargs):
        cfg = WICS1ConfigFileGeneration(
            tile_id=tile_id,
            sigma0_name=self.get_basename(reference.input_path),
            measurement_date=self.format_measurement_date(str(reference.measurement_day))
        )
        return cfg

    def handle_wics2(self, hcl_info: List[namedtuple], tile_id: str, reference: any ,**kwargs):
        return WICS2ConfigFileGeneration(
            tile_id=tile_id,
            l2a_name=self.get_basename(reference.input_path),
            measurement_date=self.format_measurement_date(str(reference.measurement_day))
        )

    def handle_cc(self, hcl_info: List[namedtuple], tile_id: str, reference: any ,**kwargs):
        # Sélection L1C
        l1c_info = next(el for el in hcl_info if "MSIL1C" in self.get_basename(el.input_path))
        measurement_date = self.format_measurement_date(str(l1c_info.measurement_day))
        l1c_path = l1c_info.input_path

        # Téléchargement inspire.xml
        bucket = l1c_path.split('/')[2]
        file_path = f"{l1c_path[12:]}/{self.inspire_file_name}"
        local_file_path = f"{self.tmp_inspire_path}/{self.inspire_file_name}"
        try:
            creds = VaultClient().read_secret('s3cfg_EODATA')
            s3 = S3Client(creds['access_key'], creds['secret_key'], creds['endpoint_url'], creds['region_name'])
            s3.download_file_from_s3(bucket, file_path, local_file_path)
            self.logger.info("File %s successfully downloaded to %s", file_path, local_file_path)
        except RuntimeError as e:
            self.logger.critical("Download failed for %s/%s: %s", l1c_path, self.inspire_file_name, e)
            return None

        # Dates
        product_measurement_date = self.calculate_l1c_product_measurement_date(
            inspire_file_path=local_file_path,
            l1c_measurement_date=l1c_path.split('_', maxsplit=3)[2]
        )

        if len(hcl_info) == 2:
            l1c_name = self.get_basename(l1c_path)
            l2a_name = self.get_basename(next(el.input_path for el in hcl_info if "L2A" in self.get_basename(el.input_path)))
            maja_mode = "L2NOMINAL"
        else:
            l1c_name, l2a_name, maja_mode = self.get_basename(reference.input_path), None, "L2INIT"

        return CCConfigFileGeneration(maja_mode, tile_id, measurement_date, product_measurement_date, l1c_name, l2a_name)

    def handle_wds(self, hcl_info: List[namedtuple], tile_id: str, reference: any ,**kwargs):
        measurement_date = self.format_measurement_date(str(reference.measurement_day))
        sigma0_name = self.get_basename(next(el.input_path for el in hcl_info if "SIG0" in self.get_basename(el.input_path)))
        fsc_list = [
            el.input_path.split("/")[-1] if el.input_path.split("/")[-1] else el.input_path.split("/")[-2]
            for el in hcl_info if "CLMS_WSI_FSC" in el.input_path
        ]
        return WDSConfigFileGeneration(tile_id=tile_id,
                                    sigma0_name=sigma0_name,
                                    measurement_date=measurement_date,
                                    fsc_list=fsc_list)

    def handle_sig0(self, hcl_info: List[namedtuple], tile_id: str, reference: any ,**kwargs):
        measurement_date = self.format_measurement_date(str(reference.measurement_day))
        relative_orbit = str(reference.relative_orbit_number)
        grd_list = [self.get_basename(el.input_path) for el in hcl_info if "GRD" in el.input_path]
        return Sig0ConfigFileGeneration(tile_id=tile_id,
                                        measurement_date=measurement_date,
                                        grd_list=grd_list,
                                        relative_orbit=relative_orbit)

    def handle_wics1s2(self, hcl_info: List[namedtuple], tile_id: str, reference: any ,**kwargs):
        measurement_date = self.format_measurement_date(str(reference.measurement_day))
        wic_s1_list = [el.input_path for el in hcl_info if "S1" in el.input_path]
        wic_s2_list = [el.input_path for el in hcl_info if "S2" in el.input_path]
        wic_s1_filename = wic_s1_list[0].split('/')[-2]
        hour = wic_s1_filename.split('_')[5].split('T')[1][:2]
        return WICS1S2ConfigFileGeneration(tile_id=tile_id,
                                        measurement_date=measurement_date,
                                        wic_s1_list=wic_s1_list,
                                        wic_s2_list=wic_s2_list,
                                        hour=hour)

    def handle_gfsc(self, hcl_info: List[namedtuple], tile_id: str, reference: any, **kwargs):
        prev_date = datetime.strptime(str(reference.gfsc_previous_processing_date), '%Y-%m-%d %H:%M:%S')
        gfsc_processing_date = datetime.strftime(prev_date, '%Y-%m-%d')

        def normalize_path(p: str) -> str:
            return p if p.endswith("/") else p + "/"

        fsc_list = [normalize_path(el.input_path) for el in hcl_info if "FSC" in self.get_basename(el.input_path)]
        sws_list = [normalize_path(el.input_path) for el in hcl_info if "SWS" in self.get_basename(el.input_path)]

        return GFSCConfigFileGeneration(tile_id=tile_id,
                                        processing_date=gfsc_processing_date,
                                        sws_list=sws_list,
                                        fsc_list=fsc_list,
                                        aggregation_timespan=self.GFSC_AGGREGATION_TIMESPAN)



       