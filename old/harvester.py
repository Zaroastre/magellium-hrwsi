#!/usr/bin/env python3
"""
Harvester module is used to extract candidate input, indentify new input and update database.
"""
import asyncio
import copy
import datetime
import json
import logging
import time
from datetime import timedelta
from os import environ
import argparse
from pathlib import Path

import hvac
import nest_asyncio
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import pytz

from magellium.hrwsi.system.apimanager.api_manager import ApiManager
from magellium.hrwsi.system.apimanager.hrwsi_database_api_manager import HRWSIDatabaseApiManager
from magellium.hrwsi.system.apimanager.wekeo_api_manager import WekeoApiManager
from magellium.hrwsi.system.core.run_modes import RunMode
from magellium.hrwsi.system.settings.queries_and_constants import (
    CANDIDATE_ALREADY_IN_DATABASE_REQUEST,
    ELIGIBLE_PRODUCT_LIST,
    GET_LAST_PUBLISHING_DATE_INPUT,
    GET_UNPROCESSED_PRODUCTS_REQUEST,
    GET_WEKEO_API_MANAGER_PARAMS,
    GRD_CANDIDATE_ALREADY_IN_DATABASE_REQUEST,
    INSERT_CANDIDATE_REQUEST,
    LISTEN_PRODUCTS_REQUEST,
    UNSET_HARVEST_START_DATES,
)
from magellium.hrwsi.utils.logger import LogUtil


class Harvester:
    """Define a harvester"""

    LOGGER_LEVEL = logging.DEBUG
    INSERT_CANDIDATE_REQUEST = INSERT_CANDIDATE_REQUEST
    LISTEN_PRODUCTS_REQUEST = LISTEN_PRODUCTS_REQUEST
    CANDIDATE_ALREADY_IN_DATABASE_REQUEST = CANDIDATE_ALREADY_IN_DATABASE_REQUEST
    GRD_CANDIDATE_ALREADY_IN_DATABASE_REQUEST = GRD_CANDIDATE_ALREADY_IN_DATABASE_REQUEST
    UNSET_HARVEST_START_DATES = UNSET_HARVEST_START_DATES
    GET_UNPROCESSED_PRODUCTS_REQUEST = GET_UNPROCESSED_PRODUCTS_REQUEST
    GET_LAST_PUBLISHING_DATE_INPUT = GET_LAST_PUBLISHING_DATE_INPUT

    HARVESTER_WAITING_TIME: str = "harvester_waiting_time"
    START_DATE: str = "start_date"
    END_DATE: str = "end_date"
    TRIGGERING_CONDITION_NAME: str = "triggering_condition_name"
    INPUT_TYPE: str = "input_type"

    def __init__(self, harvesting_run_mode: RunMode, configuration_folder: Path, request_list: list[ApiManager] = None):

        self.request_list = request_list
        self.logger = LogUtil.get_logger('Log_harvester', self.LOGGER_LEVEL, "log_harvester/logs.log")

        # Load config file
        self.__configuration_folder: Path = configuration_folder
        config_data = ApiManager.read_config_file(self.__configuration_folder)

        self.delta_time_notification_max = datetime.timedelta(
            seconds=config_data[Harvester.HARVESTER_WAITING_TIME]["delta_seconds_max_between_notifications"])
        self.nb_of_second_between_each_notify_verification = config_data[Harvester.HARVESTER_WAITING_TIME][
            "nb_of_second_between_each_notify_verification"]
        self.day_since_creation_of_processing_task = config_data["orchestration_parameters"][
            "day_since_creation_of_processing_task"]
        self.sleep_time_before_harvest_product = config_data[Harvester.HARVESTER_WAITING_TIME][
            "sleep_time_before_harvest_product"]
        self.sleep_time_to_harvest_input = config_data[Harvester.HARVESTER_WAITING_TIME]["sleep_time_before_harvest_raw_input"]

        self.eligible_product_list: list = ELIGIBLE_PRODUCT_LIST
        self.products_queue = asyncio.Queue()

        self.continuous_harvester = True
        self.harvesting_run_mode: RunMode = harvesting_run_mode
        self.past_recovery = True

    def harvest_input(self) -> None:
        """
        Init request params for Wekeo, find candidate input, identify new input and add in database.
        When wekeo resto data is retrieved further back in time by the harvester, the database 'harvest_start_date'
        value is the date reference from which the triggerer can identify nrt data as soon as new data is inserted
        in the raw_input table.
        """

        self.logger.info("Begin harvest input")
        finished_harvesting = False
        today = datetime.date.today()
        now = datetime.datetime.now(datetime.timezone.utc)
        
        # Interrupt the WEkEO harvesting loop in case this session takes more than the sleep time
        self.continuous_harvester = False

        with HRWSIDatabaseApiManager.database_connection() as (conn, cur):
            # Convert cur in a dict and activate autocommit
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
            # Get wekeo config params
            res_config_params = HRWSIDatabaseApiManager.execute_request_in_database(cur, GET_WEKEO_API_MANAGER_PARAMS)
            config_data = res_config_params.fetchall()

            # Recovering the date from which to harvest wekeo data
            harvest_dates = self.extract_harvest_dates(config_data, self.harvesting_run_mode)
            harvest_dates_deep_copy = copy.deepcopy(harvest_dates)

            # The 'past_recovery' class parameter boolean is set to True if at least one harvest_start_date exists
            # which means that the system will harvest data from the past.
            self.past_recovery = any(sub_dict[Harvester.START_DATE] is not None for sub_dict in harvest_dates.values())

            deltaday_furthest_date = 1
            while not finished_harvesting:

                # Create request_list for wekeo
                for pc in config_data:
                    # Set next harvest start date
                    if pc['timeliness']:
                        tc_name = f"{pc[Harvester.TRIGGERING_CONDITION_NAME]}/{pc['timeliness']}" 
                    else:
                        tc_name = pc[Harvester.TRIGGERING_CONDITION_NAME]

                    closing_date = today if self.harvesting_run_mode == RunMode.NRT else harvest_dates[tc_name][Harvester.END_DATE]
                    
                    # Restore the database cursor as default tuple
                    cur = conn.cursor()
                    
                    if self.past_recovery:
                        current_harvest_start_date = harvest_dates[tc_name][Harvester.START_DATE]
                        if current_harvest_start_date and current_harvest_start_date <= closing_date:
                            next_harvest_start_date = current_harvest_start_date + timedelta(days=deltaday_furthest_date)
                            current_harvest_end_date = next_harvest_start_date
                        else:
                            harvest_dates[tc_name][Harvester.START_DATE] = None
                            continue
                        harvest_dates[tc_name][Harvester.START_DATE] = current_harvest_end_date

                        request = WekeoApiManager(
                                        triggering_condition_name=tc_name,
                                        input_type=pc[Harvester.INPUT_TYPE],
                                        collection=pc["collection"],
                                        harvest_start_date=harvest_dates_deep_copy[tc_name][Harvester.START_DATE],
                                        tile_list_file=pc["tile_list_file"] if "tile_list_file" in pc else None,
                                        geometry_file=pc["geometry_file"] if "geometry_file" in pc else None,
                                        polarisation=pc["polarisation"],
                                        timeliness=pc["timeliness"],
                                        configuration_folder=self.__configuration_folder,
                                        min_publication_date=current_harvest_start_date,
                                        max_publication_date=current_harvest_end_date)

                    else :
                        min_measurement_date = now - timedelta(days=pc["max_day_since_measurement_date"])
                        
                        request_last_publishing_date = GET_LAST_PUBLISHING_DATE_INPUT.format(pc[Harvester.INPUT_TYPE])
                        cur = HRWSIDatabaseApiManager.execute_request_in_database(cur, request_last_publishing_date)
                        last_publishing_date = cur.fetchone()
                                                
                        # The GRDs having two timelinesses, we do not to miss the Fast-24h or the NRT-3h if
                        # one is published just before the one we just harvested.
                        if last_publishing_date and not pc['timeliness']:
                            last_publishing_date = pytz.utc.localize(last_publishing_date[0])
                        else:
                            last_publishing_date = now - timedelta(days=pc["max_day_since_publication_date"])
                        
                        if now - timedelta(days=pc["max_day_since_publication_date"]) > last_publishing_date:
                            self.logger.error("Trying to harvest data too far in the past, going from %s to %s for the publishing date.",
                                              last_publishing_date,
                                              now - timedelta(days=pc['max_day_since_publication_date']))

                            last_publishing_date = now - timedelta(days=pc["max_day_since_publication_date"])
                        
                        request = WekeoApiManager(
                                        triggering_condition_name=tc_name,
                                        input_type=pc[Harvester.INPUT_TYPE],
                                        collection=pc["collection"],
                                        harvest_start_date=min_measurement_date,
                                        tile_list_file=pc["tile_list_file"] if "tile_list_file" in pc else None,
                                        geometry_file=pc["geometry_file"] if "geometry_file" in pc else None,
                                        polarisation=pc["polarisation"],
                                        timeliness=pc["timeliness"],
                                        min_publication_date=last_publishing_date,
                                        max_publication_date=now)

                    # Find candidate input
                    all_candidates_tuple = request.get_candidate_inputs()

                    if all_candidates_tuple:

                        # Identify new candidate
                        self.logger.info("Begin identify_new_candidate")
                        if request.input_type == 'S2MSI1C':
                            if self.past_recovery:
                                current_harvest_start_day = current_harvest_start_date.strftime('%Y-%m-%d')
                                candidate_already_in_database_request = self.CANDIDATE_ALREADY_IN_DATABASE_REQUEST.format(
                                        current_harvest_start_day, request.input_type)
                            else:
                                min_measurement_day = min_measurement_date.strftime('%Y-%m-%d')
                                candidate_already_in_database_request = self.CANDIDATE_ALREADY_IN_DATABASE_REQUEST.format(
                                        min_measurement_day, request.input_type)

                            new_input_tuple = Harvester.identify_new_candidate(candidate_already_in_database_request,
                                                                               all_candidates_tuple,
                                                                               7)
                        else:
                            if self.past_recovery:
                                current_harvest_start_day = current_harvest_start_date.strftime('%Y-%m-%d')
                                candidate_already_in_database_request = self.GRD_CANDIDATE_ALREADY_IN_DATABASE_REQUEST.format(
                                    current_harvest_start_day, request.input_type)
                            else:
                                min_measurement_day = min_measurement_date.strftime('%Y-%m-%d')
                                candidate_already_in_database_request = self.GRD_CANDIDATE_ALREADY_IN_DATABASE_REQUEST.format(
                                        min_measurement_day, request.input_type)

                            new_input_tuple = Harvester.identify_new_candidate(candidate_already_in_database_request,
                                                                               all_candidates_tuple,
                                                                               timeliness=True)
                        self.logger.info("End identify_new_candidate")

                        # Add new input in database
                        if new_input_tuple:
                            self.logger.info("idenfied %i new inputs to insert in the database", len(new_input_tuple))
                            _ = HRWSIDatabaseApiManager.execute_request_in_database(cur, self.INSERT_CANDIDATE_REQUEST,
                                                                                    new_input_tuple)
                            conn.commit()

                # Update the finished_harvesting status
                finished_harvesting = all(sub_dict[Harvester.START_DATE] is None for sub_dict in harvest_dates.values())

            # If the system got wekeo data from the past, wait 5 minutes before to set the nrt/archive_harvest_start_date
            # and harvest_end_date by input_type to NULL into the database, to give the Triggerer time to check that
            # all recent raw input insertions are of type nrt with the help of the nrt/archive_harvest_start_date.
            if self.past_recovery:
                time.sleep(self.sleep_time_to_harvest_input)
                unset_harvest_date_map = {
                    RunMode.NRT: 'nrt_harvest_start_date=NULL',
                    RunMode.ARCHIVE: 'archive_harvest_start_date=NULL, archive_harvest_end_date=NULL'
                }
                for key in harvest_dates_deep_copy.keys():
                    req = UNSET_HARVEST_START_DATES.format(unset_harvest_date_map[self.harvesting_run_mode], key.replace('/', ''))
                    _ = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
                conn.commit()

            # Reactivate the continuous harvester input process
            self.continuous_harvester = True

        self.logger.info("End harvest input")

    @staticmethod
    def extract_harvest_dates(config_data: list,
                              harvesting_run_mode: RunMode) -> dict:
        """
        According to the hrwsi run mode, get harvesting start/end date by input_type
        and convert it to a datetime.date object.
        - For the 'nrt' operating mode, only the start date from which to retrieve wekeo data is used.
        This mode is only used for the initial launch of the hrwsi application in production,
        in order to retrieve data from a date defined as required.
        - The 'archive' operating mode, on the other hand, is based on a time interval
        defined by a start date and an end date.

        @param: config_data: the wekeo api manager configuration data
        @param: today: the current date
        @param: harvesting_run_mode: the current harvester run mode defined by the parameter class 'harvesting_run_mode'
        """

        # Mapping dict
        date_map = {RunMode.NRT: {'start': 'nrt_harvest_start_date', 'end': 'nrt_harvest_end_date'},
                    RunMode.ARCHIVE: {'start': 'archive_harvest_start_date',
                                'end': 'archive_harvest_end_date'}
                    }

        harvest_dates = {
            f"{d[Harvester.TRIGGERING_CONDITION_NAME]}/{d['timeliness']}" if d['timeliness'] else d[
                Harvester.TRIGGERING_CONDITION_NAME]: {
                Harvester.START_DATE: (
                    datetime.date(int(d[date_map[harvesting_run_mode]['start']][:4]),
                                  int(d[date_map[harvesting_run_mode]['start']][4:6]),
                                  int(d[date_map[harvesting_run_mode]['start']][6:8])) if d[
                        date_map[harvesting_run_mode]['start']] else None),
                Harvester.END_DATE: (
                    datetime.date(int(d[date_map[harvesting_run_mode]['end']][:4]),
                                  int(d[date_map[harvesting_run_mode]['end']][4:6]),
                                  int(d[date_map[harvesting_run_mode]['end']][6:8])) if (
                            date_map[harvesting_run_mode]['end'] in d
                            and d[date_map[harvesting_run_mode]['end']])
                                  else None)
            }
            for d in config_data}

        return harvest_dates

    async def harvest(self) -> None: # pragma: no cover
        """
        Run harvester workflow
        """

        # Create Listening loop
        await self.create_loop()

    async def create_loop(self) -> None:  # pragma: no cover
        """
        Create a loop to wait and listen notifications
        """

        self.logger.info("Initializing event loop")
        nest_asyncio.apply()  # Correct that : by design asyncio does not allow its event loop to be nested
        error_message = 'create_loop error: {}'
        conn, cur, conn_insert, cur_insert = None, None, None, None

        try:
            # Connect to Database
            conn, cur = HRWSIDatabaseApiManager.connect_to_database()
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            # Convert cur in a dict
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Listen channel
            _ = HRWSIDatabaseApiManager.execute_request_in_database(cur, self.LISTEN_PRODUCTS_REQUEST)

            # If the Harvester service is restarted, get all unprocessed products.
            notify_req = "NOTIFY product_insertion, '{}';"
            HRWSIDatabaseApiManager.get_statements_to_notify_again(GET_UNPROCESSED_PRODUCTS_REQUEST, notify_req, self.logger)

            try:
                # Init and run the event loop
                notify_task = asyncio.create_task(self.handle_notify(conn, cur))
                harvest_task = asyncio.create_task(self.continuous_harvest_input())

                await asyncio.gather(notify_task, harvest_task)
                self.logger.info("Event loop running")

            except asyncio.CancelledError:
                notify_task.cancel()
                harvest_task.cancel()
                await asyncio.gather(notify_task, harvest_task, return_exceptions=True)

        except KeyboardInterrupt:
            pass
        except (OSError, TypeError, AttributeError, ValueError, RuntimeError,
                psycopg2.OperationalError) as error:
            self.logger.error(error_message.format(error))
        except Exception as error:
            self.logger.error(error_message.format(error))

        finally:
            # Close the database connector and cursor, and shutdown the loop
            if cur and conn:
                cur.close()
                conn.close()
            if cur_insert and conn_insert:
                cur_insert.close()
                conn_insert.close()

    async def continuous_harvest_input(self) -> None:  # pragma: no cover
        """
        Every 5 minutes (period of time defined by the self.sleep_time_to_harvest_input value),
        we check for new data before collecting it.
        """

        error_message = "Error in continuous_harvest_input : {}"

        try:
            while True:
                # Wait 5 minutes before to harvest new inputs
                await asyncio.sleep(self.sleep_time_to_harvest_input)
                if self.continuous_harvester:
                    self.harvest_input()


        except (KeyError, psycopg2.OperationalError, TypeError, hvac.exceptions.VaultError) as error:
            self.logger.error("%s",error_message.format(error))
        except Exception as error:
            self.logger.error("%s",error_message.format(error))

    def handle_product_notification(self, data_input: dict) -> None:
        """
        Based on the notification payload, product data formatting by the transform_data function before being inserted
        into the raw_inputs table using the XXX function.
        """

        error_message = 'handle_product_notification error: {}'

        try:
            formatted_product_data = Harvester.transform_product_data(data_input)

            # Insert formatted product data into raw_inputs table
            if formatted_product_data:
                with HRWSIDatabaseApiManager.database_connection() as (conn, cur):
                    # Convert cur in a dict and activate autocommit
                    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    HRWSIDatabaseApiManager.execute_request_in_database(cur,
                                                                        self.INSERT_CANDIDATE_REQUEST,
                                                                        formatted_product_data)

        except (KeyError, psycopg2.OperationalError, TypeError, ValueError) as error:
            raise type(error)(error_message.format(error)) from error
        except Exception as error:
            raise Exception(error_message.format(error)) from error

    @staticmethod
    def transform_product_data(data: dict) -> tuple:
        """
        In order to create a new entry in the raw_inputs table, the product data is formatted as follows:
        * S2_WICS2_L2B, S2_FSC_L2B, S1_WDS_L2B, S1_SWS_L2B, S1_WICS1_L2B, S2_CC_L2B & COMB_WICS1S2
            - measurement_day is deducted from the sixth part of the id.
              example: for the CLMS_WSI_FSC_020m_<TILEID>_<YYYYMMDDTHHmmSS>_<PLATFORM>_V102_FSCOG.<EXTENSION>,
                       the YYYYMMDDTHHmmSS part gives the UTC date and time of the observation.
                       Since the data is of type string, we transform it into datetime.
            - the tile is deducted from the fifth part of the id and the first character is ignored.
              example: T31TCH become 31TCH
            - the product does not contain is_partial and relative_orbit_number information, so the default values are False
              and None respectively. However, for the sake of genericity, if this information exists, it is used.

        * S2_MAJA_L2A:
            <SENTINEL2X>_<MEASUREMENT-DATE-YYYMMDD-HHMMSS-MS>_L2A_T<TILE>_C_V1-0_CLM_R2
            example: SENTINEL2B_20210202-124336-333_L2A_T28WET_C_V1-0_FRE_B11

        * S1_NRB_L2A (Backscatter_10m):
            SIG0_<START_DATETIME>_<STOP_DATETIME>_<MISSION_TAKE_ID>_<RELATIVE_ORBIT>_<TILE>_<PIXEL_SIZE>_<PLATFORM>IWGRDH_ENVEO
            example: SIG0_20210102T074131_20210102T074200_043635_009_T28WET_10m_S1AIWGRDH_ENVEO

        * GFSC_L2C:
            CLMS_WSI_GFSC_060m_<TILEID>_<YYYYMMDDP7D>_COMB_<VERSION>_<LAYER>
            example: CLMS_WSI_GFSC_060m_T28WET_202008017D_COMB_V102_GF-QA
        """
        error_message = 'transform_product_data error: {}'
        start_date_format = '%Y-%m-%dT%H:%M:%S.'

        try:
            raw_input_id = data['id']
            product_type_code = data['product_type_code']
            input_path = data['product_path']
            publishing_date = data['catalogue_date']
            is_partial = data.get('is_partial', False)
            relative_orbit_number = None

            match product_type_code:
                case 'S2_MAJA_L2A':
                    tile = data['id'].split('_')[3][1:]
                    measurement_day = int(data['id'].split('_')[1][:8])
                    original_start_date = raw_input_id.split('_')[1]
                    formatted_start_date = datetime.datetime.strptime(original_start_date, '%Y%m%d-%H%M%S-%f')
                    start_date = formatted_start_date.strftime(
                        start_date_format) + f'{formatted_start_date.microsecond:06d}'

                case 'S1_NRB_L2A':
                    tile = data['id'].split('_')[5][1:]
                    measurement_day = int(data['id'].split('_')[1].split("T")[0])
                    raw_input_split = raw_input_id.split('_')
                    relative_orbit_number = int(raw_input_split[4])
                    original_start_date = raw_input_id.split('_')[1]
                    formatted_start_date = datetime.datetime.strptime(original_start_date, '%Y%m%dT%H%M%S')
                    start_date = formatted_start_date.strftime(
                        start_date_format) + f'{formatted_start_date.microsecond:06d}'

                case 'S2_WICS2_L2B' | 'S2_FSC_L2B' | 'S1_WDS_L2B' | 'S1_SWS_L2B' | 'S1_WICS1_L2B' | 'S2_CC_L2B':
                    tile = data['id'].split('_')[4][1:]
                    measurement_day = int(data['id'].split('_')[5].split("T")[0])
                    original_start_date = raw_input_id.split('_')[5]
                    formatted_start_date = datetime.datetime.strptime(original_start_date, '%Y%m%dT%H%M%S')
                    start_date = formatted_start_date.strftime(
                        start_date_format) + f'{formatted_start_date.microsecond:06d}'

                case 'COMB_WICS1S2':
                    tile = data['id'].split('_')[4][1:]
                    measurement_day = int(data['id'].split('_')[5].split("T")[0])
                    original_start_date = raw_input_id.split('_')[5][:-4]
                    formatted_start_date = datetime.datetime.strptime(original_start_date, '%Y%m%dT%H%M%S')
                    start_date = formatted_start_date.strftime(
                        start_date_format) + f'{formatted_start_date.microsecond:06d}'

                case 'GFSC_L2C':
                    tile = data['id'].split('_')[4][1:]
                    measurement_day = int(data['id'].split('_')[5][:8])
                    original_start_date = raw_input_id.split('_')[5][:-3]
                    formatted_start_date = datetime.datetime.strptime(original_start_date, '%Y%m%d')
                    start_date = formatted_start_date.strftime(
                        start_date_format) + f'{formatted_start_date.microsecond:06d}'

                case _:
                    raise KeyError('Unknown product.')

            return ((raw_input_id, product_type_code, start_date, publishing_date, tile, measurement_day,
                     relative_orbit_number, input_path, is_partial),)

        except (KeyError, TypeError) as error:
            raise type(error)(error_message.format(error)) from error

    async def handle_notify(self, conn: psycopg2.extensions.connection, cur: psycopg2.extensions.cursor) -> None: # pragma: no cover
        """
        When a processing task processed notification pop, if all processing task are processed,
        create input for all L2A products and clear notification
        """

        self.logger.info("Begin handle_notify : Receive processing task processed notification")
        error_message = "Error in handle_notify : {}"

        try:
            while True:
                # Wait a little to avoid a fast loop
                await asyncio.sleep(0.1)
                conn.poll()

                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    self.logger.debug("Payload : %s", notify.payload)
                    # Data filtering for new product recovery.
                    # Each product is inserted in the raw_inputs table if it is eligible.
                    raw_input = json.loads(notify.payload)
                    product_type = raw_input['product_type_code']
                    if product_type in self.eligible_product_list:
                        self.handle_product_notification(raw_input)

        except (KeyError, psycopg2.OperationalError, TypeError) as error:
            self.logger.error("%s",error_message.format(error))
        except Exception as error:
            self.logger.error("%s",error_message.format(error))
        finally:
            await self.handle_notify(conn, cur)


    @staticmethod
    def identify_new_candidate(request: str,
                               candidates_tuple: tuple,
                               col_index_in_candidate: int = None,
                               timeliness: bool = False) -> tuple:
        """
        Verify that tuple are not already in input table and return only new input tuple.
        """

        date_format = '%Y-%m-%dT%H:%M:%S.%fZ'
        with HRWSIDatabaseApiManager.database_connection() as (conn, cur):
            # Convert cur in a dict and activate autocommit
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            res = HRWSIDatabaseApiManager.execute_request_in_database(cur, request)

            if not timeliness:
                candidates_in_database = [result['input_path'] for result in res.fetchall()]
                new_tuple = tuple(
                    tuples for tuples in candidates_tuple
                    if tuples[col_index_in_candidate] not in candidates_in_database)
            else:
                candidates_in_database = [result for result in res.fetchall()]
                new_tuple = tuple(candidate for candidate in candidates_tuple
                                  if not any(
                                      candidate[4] == db_candidate['tile'] and
                                      candidate[2] == db_candidate[Harvester.START_DATE].strftime(date_format)
                                      for db_candidate in candidates_in_database))
        return new_tuple

def main():  # pragma: no cover
    running_mode: RunMode | None = RunMode.of(environ.get("HRWSI_HARVESTER_RUNNING_MODE"))
    vault_token: str | None = environ.get("VAULT_TOKEN")
    vault_addr: str | None = environ.get("VAULT_ADDR")

    if (running_mode is None):
        running_mode = RunMode.NRT

    if (vault_token is None):
        raise ValueError("VAULT_TOKEN environment variable is not set.")

    if (vault_addr is None):
        raise ValueError("VAULT_ADDR environment variable is not set.")

    parser = argparse.ArgumentParser(description="Launch a job with a specific flavour.")

    parser.add_argument(
        "-c", "--configuration-folder-path",
        type=Path,
        dest="configuration_folder",
        required=True,
        help="The path of the configuration folder."
    )

    args = parser.parse_args()


    harvester = Harvester(
        running_mode,
        args.configuration_folder
    )
    asyncio.run(harvester.harvest())

if (__name__ == "__main__"):  # pragma: no cover
    main()