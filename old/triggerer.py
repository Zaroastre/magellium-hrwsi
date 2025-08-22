#!/usr/bin/env python3
"""
Triggerer module is used to create processing tasks (PT) according triggering Conditions (TC), a bunch of logical
equations using terms on data availability to assess what Processing Tasks (PT) are to be created
"""
import argparse
import asyncio
import datetime
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Tuple
from zoneinfo import ZoneInfo

import hvac
import nest_asyncio
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from magellium.hrwsi.system.apimanager.api_manager import ApiManager
from magellium.hrwsi.system.apimanager.hrwsi_database_api_manager import HRWSIDatabaseApiManager
from magellium.hrwsi.system.settings.queries_and_constants import (
    COUNT_TO_BE_CREATED_CC_PT_ON_TILE_AND_DATE_INTERVAL,
    COUNT_UNDISPATCHED_CC_PT_ON_TILE_AND_DATE_INTERVAL,
    COUNT_UNFINISHED_CC_PT_ON_TILE_AND_DATE_INTERVAL,
    GET_ALL_FSC_AND_SWS_IN_THE_LAST_7_DAYS,
    GET_GRDH_UNPROCESSED,
    GET_L1C_UNPROCESSED,
    GET_LAST_PROCESSING_DATE,
    GET_PAST_HARVEST_DATE_REQ,
    GET_UNPROCESSED_PRODUCT,
    GET_UNPROCESSED_RAW_INPUTS_REQUEST,
    GET_WICS1S2_PAIRS_REQUEST,
    GFSC_TC_ALREADY_EXIST,
    INSERT_RAW2VALID,
    INSERT_TRIGGER_VALIDATION,
    IS_INPUT_SHARE_SAME_TILE_AND_MEASUREMENT_DAY,
    IS_L2A_EXISTS_REQUEST,
    IS_NRT_FROM_NOW_HARVEST_DATE_REQ,
    IS_NRT_FROM_PAST_HARVEST_DATE_REQ,
    IS_ONE_PROCESSING_TASK_EXISTS_FOR_THIS_TRIGGERING_CONDITION_TODAY_ON_SAME_TILE_AND_MEASUREMENT_DAY,
    IS_ONE_TRIGGER_VALIDATION_EXISTS_FOR_AN_INPUT,
    IS_PREVIOUS_L2A_EXISTS_FOR_THIS_TC_TILE_AND_MEASUREMENT_DAY_INTERVAL,
    LISTEN_INPUT_INSERTION_REQUEST,
    NB_OF_NOT_SUCCESSFULLY_PROCESSED_TASK_FOR_A_DAY_AND_SPECIFICS_ROUTINES,
    NRT_PRODUCT_LIST,
    SET_LAST_GFSC_PROCESSING_DATE,
    UNPROCESSED_TV_EXISTS_FOR_THIS_TC_TILE_AND_MEASUREMENT_DAY_INTERVAL,
)
from magellium.hrwsi.utils.logger import LogUtil
from magellium.hrwsi.utils.loop import init_loop, shutdown


class Triggerer:
    """Define a triggerer"""

    LOGGER_LEVEL = logging.INFO
    INSERT_TRIGGER_VALIDATION = INSERT_TRIGGER_VALIDATION
    INSERT_RAW2VALID = INSERT_RAW2VALID
    LISTEN_INPUT_INSERTION_REQUEST = LISTEN_INPUT_INSERTION_REQUEST
    IS_ONE_TRIGGER_VALIDATION_EXISTS_FOR_AN_INPUT = IS_ONE_TRIGGER_VALIDATION_EXISTS_FOR_AN_INPUT
    IS_ONE_PROCESSING_TASK_EXISTS_FOR_THIS_TRIGGERING_CONDITION_TODAY_ON_SAME_TILE_AND_MEASUREMENT_DAY = IS_ONE_PROCESSING_TASK_EXISTS_FOR_THIS_TRIGGERING_CONDITION_TODAY_ON_SAME_TILE_AND_MEASUREMENT_DAY
    IS_INPUT_SHARE_SAME_TILE_AND_MEASUREMENT_DAY = IS_INPUT_SHARE_SAME_TILE_AND_MEASUREMENT_DAY
    GET_GRDH_UNPROCESSED = GET_GRDH_UNPROCESSED
    NRT_PRODUCT_LIST = NRT_PRODUCT_LIST
    IS_NRT_FROM_NOW_HARVEST_DATE_REQ = IS_NRT_FROM_NOW_HARVEST_DATE_REQ
    IS_NRT_FROM_PAST_HARVEST_DATE_REQ = IS_NRT_FROM_PAST_HARVEST_DATE_REQ
    GET_PAST_HARVEST_DATE_REQ = GET_PAST_HARVEST_DATE_REQ
    GET_UNPROCESSED_PRODUCT = GET_UNPROCESSED_PRODUCT
    GET_ALL_GFSC_AND_SWS_IN_THE_LAST_7_DAYS = GET_ALL_FSC_AND_SWS_IN_THE_LAST_7_DAYS
    GET_LAST_PROCESSING_DATE = GET_LAST_PROCESSING_DATE
    SET_LAST_GFSC_PROCESSING_DATE = SET_LAST_GFSC_PROCESSING_DATE
    IS_L2A_EXISTS_REQUEST = IS_L2A_EXISTS_REQUEST
    IS_PREVIOUS_L2A_EXISTS_FOR_THIS_TC_TILE_AND_MEASUREMENT_DAY_INTERVAL = IS_PREVIOUS_L2A_EXISTS_FOR_THIS_TC_TILE_AND_MEASUREMENT_DAY_INTERVAL
    GET_L1C_UNPROCESSED = GET_L1C_UNPROCESSED
    COUNT_UNFINISHED_CC_PT_ON_TILE_AND_DATE_INTERVAL = COUNT_UNFINISHED_CC_PT_ON_TILE_AND_DATE_INTERVAL
    GET_UNPROCESSED_RAW_INPUTS_REQUEST = GET_UNPROCESSED_RAW_INPUTS_REQUEST
    COUNT_TO_BE_CREATED_CC_PT_ON_TILE_AND_DATE_INTERVAL = COUNT_TO_BE_CREATED_CC_PT_ON_TILE_AND_DATE_INTERVAL
    COUNT_UNDISPATCHED_CC_PT_ON_TILE_AND_DATE_INTERVAL = COUNT_UNDISPATCHED_CC_PT_ON_TILE_AND_DATE_INTERVAL
    IS_PREVIOUS_L2A_EXISTS_FOR_THIS_TC_TILE_AND_MEASUREMENT_DAY_INTERVAL = IS_PREVIOUS_L2A_EXISTS_FOR_THIS_TC_TILE_AND_MEASUREMENT_DAY_INTERVAL
    UNPROCESSED_TV_EXISTS_FOR_THIS_TC_TILE_AND_MEASUREMENT_DAY_INTERVAL = UNPROCESSED_TV_EXISTS_FOR_THIS_TC_TILE_AND_MEASUREMENT_DAY_INTERVAL
    NB_OF_NOT_SUCCESSFULLY_PROCESSED_TASK_FOR_A_DAY_AND_SPECIFICS_ROUTINES = NB_OF_NOT_SUCCESSFULLY_PROCESSED_TASK_FOR_A_DAY_AND_SPECIFICS_ROUTINES
    GFSC_TC_ALREADY_EXIST = GFSC_TC_ALREADY_EXIST

    NRT_TC_INPUTS_PAIRS={
        "Backscatter_10m_TC":("'IW_GRDH_1S'"),
        "CC_TC":("'S2MSI1C'"),
        "FSC_TC":("'S2_MAJA_L2A'"),
        "WICS2_TC":("'S2_MAJA_L2A'"),
        "SWS_TC":("'S1_NRB_L2A'"),
        "WICS1_TC":("'S1_NRB_L2A'"),
        "WDS_TC":("'S2_FSC_L2B'"),
        }

    def __init__(self, configuration_folder: Path): # pragma no cover

        error_message = 'Triggerer init error: {}'

        def format_data(yaml_data: dict): # pragma: no cover
            """
            When a yaml file is loaded with this data structure > T26WPS: 016 155
            We need to transform the data to have a valid dict.
            """
            result = {}
            for key, value in yaml_data.items():
                # integer value
                if isinstance(value, int):
                    result[key] = [value]
                # String value with whitespace
                elif isinstance(value, str) and ' ' in value:
                    result[key] = [int(num) for num in value.split()]
                # String without whitespace
                elif isinstance(value, str):
                    result[key] = [int(value)]
                # List
                else:
                    result[key] = [int(str(num).strip()) for num in value]
            return result

        try:
            self.logger = LogUtil.get_logger('Log_triggerer', self.LOGGER_LEVEL, "log_triggerer/logs.log")

            # Load config file
            config_data = ApiManager.read_config_file(configuration_folder)

            # Load valid tile track files
            with open(configuration_folder.joinpath(config_data['valid_tile_track_sws']), 'r', encoding="utf-8") as file:
                data = yaml.safe_load(file)
                self.valid_tile_track_sws = format_data(data)
            with (open(configuration_folder.joinpath(config_data['valid_tile_track_wds']), 'r', encoding="utf-8") as file):
                data = yaml.safe_load(file)
                self.valid_tile_track_wds = format_data(data)
            with open(configuration_folder.joinpath(config_data['valid_tile_track_wics1']), 'r', encoding="utf-8") as file:
                data = yaml.safe_load(file)
                self.valid_tile_track_wics1 = format_data(data)

            # Create triggering conditions dict
            triggering_conditions_temp = config_data['hrwsi_database_api_manager']
            self.triggering_conditions = {
                item['triggering_condition_name']: {k: v for k, v in item.items() if
                                                    k != 'triggering_condition_name'}
                for item in triggering_conditions_temp
            }

            with open(configuration_folder.joinpath(config_data['tile_list_file_path']), 'r', encoding="utf-8") as file:
                self.tile_list = yaml.safe_load(file)

            self.inputs_queue = asyncio.Queue()
            self.iw_grdh_1s_list = defaultdict(list)
            self.waiting_seconds = config_data['triggerer_waiting_time']['waiting_seconds']
            self.periodic_grdh_processing = config_data['triggerer_waiting_time']['waiting_grdh_seconds']
            self.periodic_l1c_processing = config_data['triggerer_waiting_time']['waiting_l1c_seconds']
            self.production_date_format = "%Y-%m-%d"
            self.orphan_ref = None

        except (FileNotFoundError, KeyError) as error:
            self.logger.error(error_message.format(error))

    def run(self) -> None:  # pragma: no cover
        """
        Run the triggerer workflow :
        - collect new harvest raw input id
        - check triggering conditions based on the product type
        - if the required conditions are met, create a trigger_validation and a raw2valid row
        """

        # Create waiting loop
        self.create_loop()

    def create_loop(self) -> None:  # pragma: no cover
        """
        Create a loop to handle notifications from the postgresql channel.
        Each notification indicates that a new row has been inserted in the raw_inputs table.
        UT: no unit test created because init_loop and shutdown methods are handled with test_loop.py
        """

        self.logger.info("Initializing event loop")
        nest_asyncio.apply()
        error_message = 'create_loop error: {}'
        loop = None
        conn, cur, conn_insert, cur_insert = None, None, None, None
        scheduler = None

        try:
            # Explicitly create the event loop
            loop_scheduler = asyncio.get_event_loop()

            # Initializes the scheduler with UTC zone info.
            # See documentation to https://apscheduler.readthedocs.io/en/3.x/index.html
            scheduler = AsyncIOScheduler(timezone=ZoneInfo("UTC"), event_loop=loop_scheduler)

            # Scheduling GFSC daily tasks every 10 minutes
            scheduler.add_job(
                self.gfsc_daily_tasks,
                'interval',
                hours=6
            )

            # Scheduling WICS1S2 every 10 minutes
            scheduler.add_job(
                self.wics1s2_daily_tasks,
                'interval',
                minutes=10
            )

            # Launch the AsyncIO Scheduler
            scheduler.start()

            # Connect to database to strictly listen for Postgres NOTIFY events.
            conn, cur = HRWSIDatabaseApiManager.connect_to_database()
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            # Start listening for notifications on the users_notification channel.
            _ = HRWSIDatabaseApiManager.execute_request_in_database(cur, self.LISTEN_INPUT_INSERTION_REQUEST)

            # Connect to Database to strictly do insert because if a NOTIFY is triggered while we’re INSERTING data,
            # we would miss it.
            conn_insert, cur_insert = HRWSIDatabaseApiManager.connect_to_database()
            conn_insert.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            # Convert cur in a dict
            cur_insert = conn_insert.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # If the Triggerer service is restarted, all unprocessed raw inputs are retrieved.
            for tc_name, input_type_code_list in self.NRT_TC_INPUTS_PAIRS.items():
                req = GET_UNPROCESSED_RAW_INPUTS_REQUEST.format(input_type_code_list, tc_name)
                HRWSIDatabaseApiManager.get_statements_to_notify_again(
                    req,
                    "NOTIFY input_insertion, '{}';",
                    self.logger)

            # List tasks to be added to the loop workflow and init the event loop
            tasks = [
                self.handle_raw_input(cur_insert),
                self.handle_grdh_raw_inputs(cur_insert),
                self.handle_l1c_raw_inputs(cur_insert)
            ]
            loop = init_loop(conn, self.handle_notify, tasks)

            # Run the loop
            loop.run_forever()
            self.logger.info("Event loop initialized and running")

        except KeyboardInterrupt:
            pass
        except (OSError, TypeError, AttributeError, ValueError, RuntimeError, psycopg2.OperationalError,
                hvac.exceptions.VaultError) as error:
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
            if loop:
                shutdown(loop)
                self.logger.info("Event loop shut down")
            if scheduler:
                scheduler.shutdown()
                self.logger.info("Scheduler shut down")

    def handle_notify(self, conn) -> None:
        """
        Process incoming 'input_insertion' channel notifications, and then we loop over any notifications
        that have arrived and add them to an asyncio queue.
        """

        self.logger.info("Begin handle_notify : Receive input insertion notification")

        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            self.inputs_queue.put_nowait(notify.payload)

    async def handle_raw_input(self, cur: psycopg2.extras.RealDictCursor) -> None:
        """
        This function is used to insert a new row into the trigger_validation and raw2valid table
        if the triggering conditions are met through the handle_triggering_condition function, for each new raw_input.
        """

        error_message = 'handle_raw_input error: {}'

        try:
            while True:
                queue_item = await self.inputs_queue.get()
                await self.handle_triggering_condition(cur, queue_item)

        except (psycopg2.OperationalError, TypeError) as error:
            self.logger.error(error_message.format(error))
        except Exception as error:
            self.logger.error(error_message.format(error))

    async def handle_triggering_condition(self, cur: psycopg2.extras.RealDictCursor, queue_item: str) -> None:
        """
        Check that triggering conditions comply with the product type.
        If the triggering conditions are met:
        - a new row is inserted into the trigger_validation
        - the trigger_validation id is retrieved from the previous insert
        - and a new row is inserted into the raw2valid table.

        @param cur: the cursor to execute the request
        @param queue_item: characterizes the inserted raw input data
        """
        error_message = 'handle_triggering_condition error: {}'
        inputs = []

        try:
            raw_input = json.loads(queue_item)
            input_type = raw_input['product_type_code']
            inputs.append(raw_input)

            # Call the triggering conditions based on the product type and create a trigger_validation and raw2valid row
            match input_type:
                case 'IW_GRDH_1S':
                    if not raw_input['is_partial']:
                        self.create_trigger_validation(inputs, 'Backscatter_10m_TC', cur)

                case 'S2_MAJA_L2A':
                    validation_status = self.validate_wics2_fsc_tc(raw_input, cur)
                    for tc_name, is_eligible in validation_status.items():
                        if is_eligible:
                            self.create_trigger_validation(inputs, tc_name, cur)

                case 'S1_NRB_L2A':
                    # Handling SAR Wet Snow Triggering Conditions
                    # To avoid unnecessary SWS TC processing, the triggerer checks that the raw_input tile is covered.
                    if raw_input['tile'] in self.tile_list:
                        is_eligible = self.validate_wics1_or_sws_tc(raw_input, 'SWS_TC', cur)
                        if is_eligible:
                            self.create_trigger_validation(inputs, 'SWS_TC', cur)

                    # Handling Water and Ice Coverage Sentinel-1 Triggering Conditions
                    is_eligible = self.validate_wics1_or_sws_tc(raw_input, 'WICS1_TC', cur)
                    if is_eligible:
                        self.create_trigger_validation(inputs, 'WICS1_TC', cur)

                    # Handling Water Dry Snow Triggering Conditions
                    is_eligible, fsc_ids, _ = self.validate_wds_tc(raw_input, cur)
                    if is_eligible:
                        inputs.extend(fsc_ids)
                        self.create_trigger_validation(inputs, 'WDS_TC', cur)

                case 'S2_FSC_L2B':
                    is_eligible, fsc_ids, sig0 = self.validate_wds_tc(raw_input, cur)
                    if is_eligible:
                        inputs.append(sig0)
                        self.create_trigger_validation(inputs, 'WDS_TC', cur)

        except (psycopg2.OperationalError, TypeError) as error:
            raise type(error)(error_message.format(error)) from error
        except Exception as error:
            raise Exception(error_message.format(error)) from error

    def eligible_previous_l2a_exists(self,
                         cur: psycopg2.extras.RealDictCursor,
                         raw_input: dict,
                         date_format: str = '%Y%m%d') -> Tuple[bool, int]:
        """
        This method check if any prevous L2A exists for a tile, the CC triggering condition and a measurement date
        between the raw_input measurement date and the raw_input measurement date minus 90 days.

        @param cur: the cursor to execute the request
        @param raw_input: raw input data
        @param date_format: the date format
        return: a boolean indicating if the task exists
        """

        error_message = 'The eligible_previous_l2a_exists method failed: {}'

        try:
            tile = raw_input['tile']
            measurement_day = raw_input['measurement_day']

            # Calculate measurement day minus 90 days
            converted_measurement_day = datetime.datetime.strptime(str(measurement_day), date_format)
            measurement_day_minus_90_days = converted_measurement_day + datetime.timedelta(days=-90)
            measurement_day_minus_90_days = int(measurement_day_minus_90_days.strftime(date_format))

            req = IS_PREVIOUS_L2A_EXISTS_FOR_THIS_TC_TILE_AND_MEASUREMENT_DAY_INTERVAL.format(tile,
                                                                                              measurement_day_minus_90_days,
                                                                                              measurement_day)
            res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
            is_previous_l2a_exists = res.fetchone()['result']

            return is_previous_l2a_exists, measurement_day_minus_90_days

        except (psycopg2.OperationalError, KeyError, TypeError, Exception) as error:
            self.logger.error(error_message.format(error))

    def eligible_previous_tv_exists(self,
                         cur: psycopg2.extras.RealDictCursor,
                         raw_input: dict,
                         date_format: str = '%Y%m%d') -> Tuple[bool, int]:
        """
        This method check if any eligible trigger_validation exists for a tile,
        the CC triggering condition and a measurement date between the raw_input
        measurement date and the raw_input measurement date minus 90 days.

        @param cur: the cursor to execute the request
        @param raw_input: raw input data
        @param date_format: the date format
        return: a boolean indicating if the task exists
        """

        error_message = 'The eligible_previous_l2a_exists method failed: {}'

        try:
            tile = raw_input['tile']
            measurement_day = raw_input['measurement_day']

            # Calculate measurement day minus 90 days
            converted_measurement_day = datetime.datetime.strptime(str(measurement_day), date_format)
            measurement_day_minus_90_days = converted_measurement_day + datetime.timedelta(days=-90)
            measurement_day_minus_90_days = int(measurement_day_minus_90_days.strftime(date_format))

            req = UNPROCESSED_TV_EXISTS_FOR_THIS_TC_TILE_AND_MEASUREMENT_DAY_INTERVAL.format(tile,
                                                                                             measurement_day_minus_90_days,
                                                                                             measurement_day)
            res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
            is_previous_tv_exists = res.fetchone()['result']

            return is_previous_tv_exists, measurement_day_minus_90_days

        except (psycopg2.OperationalError, KeyError, TypeError, Exception) as error:
            self.logger.error(error_message.format(error))

    def is_l2a_exists(self, cur:psycopg2.extras.RealDictCursor, raw_input: dict) -> Tuple:
        """
        Check for a same tile, that a L2A product exists for a L1C input.
        The most recent L2A must have a measurement day between L1C measurement day minus 1 day and
        L1C measurement minus 90 days.

        @param cur: the cursor to execute the request
        @param raw_input: the raw input data
        return the L2A product
        """
        error_message = 'The is_l2a_exists method failed: {}'

        try:
            date_format = '%Y%m%d'
            tile = raw_input['tile']
            s2msi1c_measurement_day = raw_input['measurement_day']

            # Calculate L1C <measurement_day> and <measurement_day - 90 days>
            temp_date = datetime.datetime.strptime(str(s2msi1c_measurement_day), date_format)

            s2msi1c_measurement_day_minus_90_days = int(
                (temp_date - datetime.timedelta(days=90)).strftime(date_format))

            # Format the SQL request and execute it
            req = IS_L2A_EXISTS_REQUEST.format(tile,
                                               s2msi1c_measurement_day_minus_90_days,
                                               s2msi1c_measurement_day
                                               )
            res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
            l2a_input = res.fetchone()

            return l2a_input

        except (psycopg2.OperationalError,KeyError, TypeError, Exception) as error:
            self.logger.error(error_message.format(error))

    def validate_wics2_fsc_tc(self, notify_input: dict, cur: psycopg2.extensions.cursor) -> defaultdict:
        """
        Check that the input data covers the WICS2 and FSC triggering conditions referenced in the
        SDD: https://docs.google.com/document/d/1e4wDDSEg_nP5VJoiSHYveRAZqrPwWP1uAQpFBchPs18/edit?usp=sharing
        The Sentinel-2 Clover Classification product type:
        - has been produced less than 7 days ago
        - no WICS2 or FSC processing task already references this product

        @param notify_input: characterizes the inserted raw input data
        @param cur: the cursor to execute the request
        @return the triggering condition validation status
        """

        error_message = 'The validate_wics2_fsc_tc method failed: {}'
        triggering_conditions_list = ['WICS2_TC', 'FSC_TC']

        try:
            production_date = datetime.datetime.strptime(notify_input['harvesting_date'][:10],
                                                         self.production_date_format).date()
            validation_status = defaultdict(bool)

            for triggering_condition in triggering_conditions_list:
                # Define production date limit
                _, earliest_acceptable_production_date = self.calculate_publication_and_measurement_deadlines(
                    triggering_condition)

                # If the L2A raw_input has been produced less than x days ago, defined by the
                # max_day_since_publication_date parameter in the config.yaml file, checking that no WIC S2 Processing Task
                # and no FSC Processing Task already references this product.
                if production_date >= earliest_acceptable_production_date:
                    raw_input_id = notify_input['id']
                    request = self.IS_ONE_TRIGGER_VALIDATION_EXISTS_FOR_AN_INPUT % (raw_input_id, triggering_condition)
                    res = HRWSIDatabaseApiManager.execute_request_in_database(cur, request)
                    status = res.fetchone()['result']
                    validation_status[triggering_condition] = status
                else:
                    validation_status[triggering_condition] = False

            return validation_status

        except (psycopg2.OperationalError, KeyError, TypeError, Exception) as error:
            self.logger.error(error_message.format(error))

    def validate_wds_tc(self, notify_input: dict, cur: psycopg2.extensions.cursor) -> Tuple[bool, list, dict]:
        """
        Check that the input data covers the WDS triggering conditions referenced in the
        SDD: https://docs.google.com/document/d/1e4wDDSEg_nP5VJoiSHYveRAZqrPwWP1uAQpFBchPs18/edit?usp=sharing
        The Sentinel-1 backscatter 10m product type:
        - has been produced less than 7 days ago
        - and an FSC product has been produced less than 7 days ago
        - and the Sentinel-1 backscatter 10m covers the same tile as the FSC product
        - and the Sentinel-1 backscatter 10m was measured the same day as the FSC product
        - and no WDS processing task already references the product
        If the product type is an FSC:
        - check that it has been produced less than 7 days ago

        Specific use case:
        - If two FSC products exists, a WDS processing task is created with them and the backscatter 10m input.

        @param notify_input: characterizes the inserted raw input data
        @param cur: the cursor to execute the request
        return the status of the triggering condition, the list of fsc ids and the backscatter 10m if the raw input
        is an FSC product
        """

        error_message = 'The validate_wds_tc method failed: {}'
        fsc_ids = []
        sig0 = None
        is_eligible = False
        triggering_condition = 'WDS_TC'
        product_type_code = 'S2_FSC_L2B'

        try:
            production_date = datetime.datetime.strptime(notify_input['harvesting_date'][:10],
                                                         self.production_date_format).date()
            _, earliest_acceptable_production_date = self.calculate_publication_and_measurement_deadlines(
                triggering_condition)
            raw_input_id = notify_input['id']
            input_type = notify_input['product_type_code']
            orbit_number = notify_input['relative_orbit_number']
            tile = notify_input['tile']
            # To avoid KeyError issue, an empty list is returned if the tile not exists in the ref_dict related to the
            # valid tile track file
            valid_orbit = self.valid_tile_track_wds.get(f"T{tile}", [])

            # If the backscatter or fsc raw_input has been produced less than x days ago, defined by the
            # max_day_since_publication_date parameter in the config.yaml file, and the relative orbit number is valid,
            # checking that no WDS trigger validation already references this product.
            if production_date >= earliest_acceptable_production_date:
                measurement_day = notify_input['measurement_day']
                request_pt = self.IS_ONE_TRIGGER_VALIDATION_EXISTS_FOR_AN_INPUT % (raw_input_id, triggering_condition)
                res = HRWSIDatabaseApiManager.execute_request_in_database(cur, request_pt)
                status = res.fetchone()['result']

                if status:
                    match input_type:
                        case 'S2_FSC_L2B':
                            # If the raw input is an FSC, the last backscatter 10m produced less than 7 days ago,
                            # covering the same tile and having the same measurement day is retrieved.
                            end_req = """ ORDER BY ri.harvesting_date DESC LIMIT 1"""
                            req_find_sig0 = self.IS_INPUT_SHARE_SAME_TILE_AND_MEASUREMENT_DAY + end_req
                            req_find_sig0 = req_find_sig0 % ("S1_NRB_L2A", str(measurement_day), tile,
                                                             str(earliest_acceptable_production_date))

                            res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req_find_sig0)
                            result = res.fetchone()
                            sig0 = dict(result) if result is not None else None
                            if sig0 and sig0.get('relative_orbit_number', None) in valid_orbit:
                                is_eligible = True
                                # Get the FSC produced less than 7 days ago, covering the same tile
                                # and the same measurement day
                                fsc_ids = self.get_product_having_same_tile_and_same_measurement_day(
                                    res, earliest_acceptable_production_date, measurement_day, product_type_code,
                                    raw_input_id, tile)

                        case 'S1_NRB_L2A':
                            if orbit_number in valid_orbit:
                                fsc_ids = self.get_product_having_same_tile_and_same_measurement_day(
                                    cur, earliest_acceptable_production_date, measurement_day, product_type_code,
                                    raw_input_id, tile)
                                if fsc_ids:
                                    is_eligible = True

            return is_eligible, fsc_ids, sig0

        except (psycopg2.OperationalError,KeyError, TypeError, Exception) as error:
            self.logger.error(error_message.format(error))

    def get_product_having_same_tile_and_same_measurement_day(self, cur: psycopg2.extensions.cursor,
                                                              earliest_acceptable_production_date: datetime.date,
                                                              measurement_day: int,
                                                              product_type_code: str,
                                                              raw_input_id: str,
                                                              tile: str) -> list:
        """
        This method recovers products covering the same tile on the same measurement date.

        @param cur: the cursor to execute the request
        @param earliest_acceptable_production_date: the earliest acceptable production date
        @param measurement_day: the measurement day
        @param product_type_code: the product type code
        @param raw_input_id: the product id
        @param tile: the tile

        @return the eligible products
        """
        # Get the products produced less than 7 days ago, covering the same tile and the same measurement day
        eligible_product_ids = []
        error_message = 'The get_product_having_same_tile_and_same_measurement_day method failed: {}'

        try:
            req_find_fsc = self.IS_INPUT_SHARE_SAME_TILE_AND_MEASUREMENT_DAY % (
                product_type_code, str(measurement_day), tile, str(earliest_acceptable_production_date))
            res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req_find_fsc)
            results = res.fetchall()

            if results:
                # Remove duplicates if the raw_input_id product type is the same
                # as the product type passed as a method parameter
                temp_product_ids = [dict(row) for row in results]
                eligible_product_ids = [product for product in temp_product_ids if product['id'] != raw_input_id]

            return eligible_product_ids

        except (psycopg2.OperationalError, Exception) as error:
            self.logger.error(error_message.format(error))

    def validate_cc_tc(self, notify_input: dict) -> bool:
        """
        Check that the input data covers the Cloud Cover triggering conditions referenced in the
        SDD: https://docs.google.com/document/d/1e4wDDSEg_nP5VJoiSHYveRAZqrPwWP1uAQpFBchPs18/edit?usp=sharing
        - The Sentinel-2 MSI L1C product type covers an EEA38 restricted tile
        - AND it has been published less than 7 days ago
        - AND it has been measured less than 30 days ago

        @param notify_input: characterizes the inserted raw input data
        @return the triggering condition validation status
        """

        error_message = 'The validate_cc_tc method failed: {}'

        try:
            earliest_acceptable_measurement_date, earliest_acceptable_publication_date = \
                self.calculate_publication_and_measurement_deadlines('CC_TC')
            self.logger.debug(f"earliest_acceptable_measurement_date = {earliest_acceptable_measurement_date}")
            self.logger.debug(f"earliest_acceptable_publication_date = {earliest_acceptable_publication_date}")

            # Get and transform raw_input publishing date and measurement date
            publishing_date = notify_input['publishing_date'].date()
            measurement_date = str(notify_input['measurement_day'])
            measurement_year = int(measurement_date[:4])
            measurement_month = int(measurement_date[4:6])
            measurement_day = int(measurement_date[6:])
            measurement_date = datetime.date(year=measurement_year, month=measurement_month, day=measurement_day)

            # Performs the TC checks
            validation_status = notify_input['tile'] in self.tile_list \
                                and publishing_date >= earliest_acceptable_publication_date \
                                and measurement_date >= earliest_acceptable_measurement_date
            return validation_status

        except (KeyError, Exception) as error:
            self.logger.error(error_message.format(error))

    def validate_wics1_or_sws_tc(self, notify_input: dict, tc_name: str, cur: psycopg2.extensions.cursor) -> bool:
        """
        Check that the input data covers the Water and Ice Coverage Sentinel-1 triggering conditions referenced in the
        SDD: https://docs.google.com/document/d/1e4wDDSEg_nP5VJoiSHYveRAZqrPwWP1uAQpFBchPs18/edit?usp=sharing
        The Sentinel-1 backscatter 10m product type:
        - has been produced less than 7 days ago with a specific orbit number
        - and no WICS1/SWS processing task already references the product

        @param notify_input: data of the inserted raw input
        @param tc_name: the name of the triggering condition
        @param cur: the database cursor to execute the request
        @return the triggering condition validation status
        """

        err_message = 'The validate_wics1_or_sws_tc method failed: {}'

        try:
            tc_name_map = {
                'SWS_TC': self.valid_tile_track_sws,
                'WICS1_TC': self.valid_tile_track_wics1
            }
            status = False

            production_date = datetime.datetime.strptime(notify_input['harvesting_date'][:10],
                                                         self.production_date_format).date()
            _, earliest_acceptable_production_date = self.calculate_publication_and_measurement_deadlines(
                tc_name)
            raw_input_id = notify_input['id']
            orbit_number = notify_input['relative_orbit_number']
            tile = notify_input['tile']
            ref_dict = tc_name_map[tc_name]

            # To avoid KeyError issue, an empty list is returned if the tile not exists in the ref_dict related to the
            # valid tile track file
            valid_orbit = ref_dict.get(f"T{tile}", [])

            # If the backscatter raw_input has been produced less than x days ago, defined by the
            # max_day_since_publication_date parameter in the config.yaml file, checking that no WICS1/SWS Processing Task
            # already references this product.
            if production_date >= earliest_acceptable_production_date and orbit_number in valid_orbit:
                raw_input_id = raw_input_id
                request_tv = self.IS_ONE_TRIGGER_VALIDATION_EXISTS_FOR_AN_INPUT % (raw_input_id, tc_name)
                res = HRWSIDatabaseApiManager.execute_request_in_database(cur, request_tv)
                status = res.fetchone()['result']

            return status

        except (KeyError, TypeError, psycopg2.OperationalError, Exception) as error:
            self.logger.error(err_message.format(error))

    def calculate_publication_and_measurement_deadlines(self, trigger_condition: str,
                                                        date_format: str = '%Y%m%d') -> (int, datetime):
        """
        Based on a specific triggering condition, calculate :
        - the measurement date limit,
        - the publishing date limit

        @param trigger_condition: the name of the trigger condition
        @param date_format: the date format of the publishing date
        return the earliest acceptable publishing date and the earliest acceptable measurement date
        """

        error_message = 'The calculate_publication_and_measurement_deadlines method failed: {}'

        # date format
        measurement_date_format = date_format

        # Today
        today = datetime.date.today()

        earliest_acceptable_measurement_date = None

        try:
            # Get max_day_since_publication_date and max_day_since_measurement_date values from the triggering conditions
            max_day_since_publication_date = self.triggering_conditions[trigger_condition][
                'max_day_since_publication_date']
            max_day_since_measurement_date = self.triggering_conditions[trigger_condition][
                'max_day_since_measurement_date']

            # Define publication date limit
            delta_publication_date = datetime.timedelta(days=max_day_since_publication_date)
            earliest_acceptable_publication_date = today - delta_publication_date

            # Define measurement date limit as an integer
            if max_day_since_measurement_date:
                delta_measurement_date = datetime.timedelta(days=max_day_since_measurement_date)
                earliest_acceptable_measurement_date = today - delta_measurement_date

            return earliest_acceptable_measurement_date, earliest_acceptable_publication_date

        except (KeyError, Exception) as error:
            self.logger.error(error_message.format(error))

    def get_unprocessed_l1c(self, cur: psycopg2.extras.RealDictCursor) -> None:
        """
        This method get all the unprocessed L1C and:
          - create a trigger validation for the L1C if no unfinished processing tasks exists on the same tile with a measurement
            date strictly before the raw input measurement day.
          - create a trigger validation for the L1C with the right L2A input if a processed processing task exists on
            the same tile with a measurement date between the raw input measurement date and the raw input measurement
            date minus 90 days.

        @param cur: the database cursor to execute the request
        """
        err_message = 'The get_unprocessed_l1c method failed: {}'

        try:
            req = GET_L1C_UNPROCESSED

            # Get unprocessed L1C data from database
            res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
            unprocessed_l1c = res.fetchall()
            self.logger.info("Found %i unprocessed L1Cs",len(unprocessed_l1c))

            earliest_acceptable_measurement_date, _ = self.calculate_publication_and_measurement_deadlines('CC_TC')
            # TODO Replace with the following line after the production climbed back its delay
            # req = COUNT_UNFINISHED_CC_PT_ON_TILE_AND_DATE_INTERVAL.format(measurement_day_minus_90_days, measurement_day)
            req = COUNT_UNFINISHED_CC_PT_ON_TILE_AND_DATE_INTERVAL.format(earliest_acceptable_measurement_date)
            res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
            results = res.fetchall()
            unprocessed_pt_tiles = [elem["tile"] for elem in results]

            req = COUNT_UNDISPATCHED_CC_PT_ON_TILE_AND_DATE_INTERVAL.format(earliest_acceptable_measurement_date)
            res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
            results = res.fetchall()
            to_be_dispatched_pt_tiles = [elem["tile"] for elem in results]

            invalid_tiles = []
            for raw_input in unprocessed_l1c:
                if set(invalid_tiles) == set(self.tile_list):
                    break
                
                tile = raw_input['tile']
                if tile in invalid_tiles:
                    continue
                
                self.logger.info(f"Working on input {raw_input}")
                # Check whether the input is on an eligible (tile, measurement date) pair
                is_eligible = self.validate_cc_tc(raw_input)
                self.logger.info("is_eligible = %r", is_eligible)

                if is_eligible:

                    # Check whether an anterior L2A exists on the same tile
                    eligible_previous_l2a_exists, _ = self.eligible_previous_l2a_exists(cur, raw_input)
                    self.logger.info("eligible_previous_l2a_exists = %r",eligible_previous_l2a_exists)
                    # Check whether a processing task is to be expected on the same tile for an anterior date
                    eligible_previous_tv_exists, _ = self.eligible_previous_tv_exists(cur, raw_input)
                    self.logger.info("eligible_previous_tv_exists = %r", eligible_previous_tv_exists)
                    if not eligible_previous_tv_exists and tile not in unprocessed_pt_tiles + to_be_dispatched_pt_tiles:
                        if not eligible_previous_l2a_exists:
                            self.logger.info("Creating a CC PT in INIT mode for raw input %s.", raw_input['id'])
                            self.create_trigger_validation([raw_input], 'CC_TC', cur)

                        else:
                            self.logger.info("Identifying previous L2A.")
                            l2a_input = self.is_l2a_exists(cur, raw_input)
                            self.logger.info("Creating NOMINAL CC processing task with L2A %s", l2a_input)
                            self.create_trigger_validation([raw_input, l2a_input], 'CC_TC', cur)
                    invalid_tiles.append(tile)


        except (psycopg2.OperationalError, TypeError, Exception) as error:
            self.logger.error(err_message.format(error))

    def get_unprocessed_grdh(self, cur: psycopg2.extensions.cursor):
        """
        This function retrieve all the unprocessed grdh raw_inputs with is_partial set to True and having a
        harvesting_date greater or equal than now less 2 hours from the HRWSI database.
        A classification of the data is then applied.
        - First, the data are classified following these rules:
            * same measurement day
            * same tile
        - Secondly, the data are classified following these rules:
            * if hardesting_date + 2 hours is greater or equal than now, each grdh orphan is put on a list
              for processing.
            * for each grdh pairs,
              if the GRDH Start Date Time is the same the Stop Date Time of the second GRDH
              or
              if the GRDH Stop Date Time is the same the Start Date Time of the second GRDH
              the pair meets the conditions and is put in a list for processing.
        - And finally, each orphan GRDH input that can complete a GRDH pair with an already processed orphan GRDH input,
          is removed and not processed.
        The processing will be handled by the create_trigger_validation function.
        """

        err_message = 'The get_unprocessed_grdh method failed: {}'
        iw_grdh_1s_list = defaultdict(list)
        orphan_grdh = []
        grdh_pairs = []
        now = datetime.datetime.now()
        harvesting_latest_date = datetime.timedelta(hours=2)

        req = self.GET_GRDH_UNPROCESSED

        try:
            # Get unprocessed GRDH data from database
            res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
            raw_inputs = res.fetchall()

            # Apply a first grdh raw_input classification (same measurement day and same tile)
            for raw_input in raw_inputs:
                key = f"{raw_input['tile']}_{raw_input['measurement_day']}_{raw_input['relative_orbit_number']}"
                iw_grdh_1s_list[key].append(dict(raw_input))

            # Apply a second grdh classification level
            # 1. All raw input pairs with is_partial set to True, same measurement day and covering the same tile are put
            #    in a grdh_pairs list for processing.
            # 2. each raw input that is not a pair with a harvesting_date greater or equal to now - 2h is put in
            #    the orphan_grdh list.
            for key, value in iw_grdh_1s_list.items():
                if len(value) < 2:
                    time_ref = value[0]['harvesting_date'] + harvesting_latest_date
                    if time_ref <= now:
                        orphan_grdh.append(value[0])
                else:
                    product1_name = value[0]['input_path'].split('/')[-1].split('.')[0]
                    product2_name = value[1]['input_path'].split('/')[-1].split('.')[0]
                    start_date_product1 = product1_name.split('_')[4]
                    stop_date_product1 = product1_name.split('_')[5]
                    start_date_product2 = product2_name.split('_')[4]
                    stop_date_product2 = product2_name.split('_')[5]

                    if stop_date_product1 == start_date_product2 or start_date_product1 == stop_date_product2:
                        grdh_pairs.append(value)

            # If a GRDH input can complete a GRDH pair with an already processed orphan GRDH input, the orphan is removed,
            # so as not to be processed.
            if not self.orphan_ref:
                self.orphan_ref = orphan_grdh
                return orphan_grdh, grdh_pairs

            orphan_ref_set = {(raw_input["tile"], raw_input["measurement_day"]) for raw_input in self.orphan_ref}

            # Filter the orphan_grdh by removing each raw_input that have tuples (tile, measurement day)
            # in the orphan_ref_set.
            orphan_grdh_filtered = [
                item for item in orphan_grdh if (item["tile"], item["measurement_day"]) not in orphan_ref_set]

            # Updating the orphan_ref list for the next iteration.
            self.orphan_ref = orphan_grdh

            return orphan_grdh_filtered, grdh_pairs

        except (psycopg2.OperationalError, TypeError, Exception) as error:
            self.logger.error(err_message.format(error))

    async def handle_grdh_raw_inputs(self, cur: psycopg2.extensions.cursor) -> None:
        """
        Periodically, unprocessed grdh data is recovered, sorted and processed.
        """

        error_message = 'handle_grdh_raw_inputs error: {}'
        tc_name = 'Backscatter_10m_TC'

        try:
            while True:
                # Wait 5 minutes to handle unprocessed grdh raw_inputs
                await asyncio.sleep(self.periodic_grdh_processing)
                orphan_grdh_list, grdh_pairs_list = self.get_unprocessed_grdh(cur)

                # Processing the grdh orphans
                if orphan_grdh_list:
                    for raw_input in orphan_grdh_list:
                        self.create_trigger_validation([raw_input], tc_name, cur)

                # Processing the GRDH pairs
                if grdh_pairs_list:
                    for raw_inputs in grdh_pairs_list:
                            self.create_trigger_validation(raw_inputs, tc_name, cur)

        except (psycopg2.OperationalError, TypeError, Exception) as error:
            self.logger.error(error_message.format(error))

    async def handle_l1c_raw_inputs(self, cur: psycopg2.extras.RealDictCursor) -> None: # pragma no cover
        """
        Periodically, unprocessed l1c data are recovered and processed.
        """

        error_message = 'handle_l1c_raw_inputs error: {}'

        try:
            while True:
                # Wait 5 minutes to handle unprocessed grdh raw_inputs
                await asyncio.sleep(self.periodic_l1c_processing)
                self.logger.info("Launching triggering for unprocessed L1Cs")
                self.get_unprocessed_l1c(cur)

        except (psycopg2.OperationalError, TypeError, Exception) as error:
            self.logger.error(error_message.format(error))

    def create_trigger_validation(self, data: list[dict], tc_name: str, cur: psycopg2.extras.RealDictCursor
                                  , validation_date: datetime.datetime = None,
                                  artificial_measurement_day: datetime.datetime = None) -> None:
        """
        Everytime a triggering condition is validated, insert a line in the trigger_validation table.
        This table references the triggering_condition table to let know to the Orchestrator what processing routine
        is to be run, and it also references all the raw_inputs relative to this specific triggering validation.

        @param data: the raw_input data dict
        @param tc_name: the name of the triggering condition
        @param cur: the psycopg2 cursor to execute the request
        @param validation_date: optional - the date of validation of the trigger condition
        @param artificial_measurement_day: optional - the processing date of the GFSC
        """

        self.logger.info("Begin create a trigger_validation and raw2valid line insert")
        error_message = 'create_trigger_validation error: {}'
        raw2valid_data = ()

        try:
            validation_date = validation_date if validation_date else datetime.datetime.now()
            trigger_validation_id = 0

            for raw_input in data:
                # Check if the raw_input is NRT
                raw_input_id = raw_input['id']
                self.logger.info("Checking input with ID %s",raw_input_id)
                product_type_code = raw_input['product_type_code']
                nrt_start_date = self.get_nrt_harvesting_date(cur, product_type_code)
                is_nrt = self.is_raw_input_nrt(cur, nrt_start_date, raw_input_id)

                # Handle trigger validation
                if not trigger_validation_id:
                    trigger_validation_data = ((tc_name, validation_date, is_nrt, artificial_measurement_day),)
                    res = HRWSIDatabaseApiManager.execute_request_in_database(cur, self.INSERT_TRIGGER_VALIDATION,
                                                                              trigger_validation_data)
                    trigger_validation_id = res.fetchone()['id']

                raw2valid_data += ((trigger_validation_id, raw_input_id),)

            _ = HRWSIDatabaseApiManager.execute_request_in_database(cur, self.INSERT_RAW2VALID, raw2valid_data)

        except (psycopg2.OperationalError, KeyError, TypeError, Exception) as error:
            raise type(error)(error_message.format(error)) from error

    @staticmethod
    def get_nrt_harvesting_date(cur: psycopg2.extensions.cursor, product_type_code: str) -> int:
        """
        Check that a date characterizes data harvesting from the wekeo resto api further back in time exists.

        @param cur: the psycopg2 cursor to execute the request
        @param product_type_code: the product type code
        return: date of the harvest.
        """
        error_message = 'get_nrt_harvesting_date error: {}'
        nrt_start_date = 0

        try:
            if product_type_code in ('S2MSI1C', 'IW_GRDH_1S'):
                req = GET_PAST_HARVEST_DATE_REQ.format(product_type_code)
                res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
                nrt_start_date = int(res.fetchone()['nrt_harvest_start_date'] or 0)

            return nrt_start_date

        except (psycopg2.OperationalError, TypeError) as error:
            raise type(error)(error_message.format(error)) from error
        except Exception as error:
            raise Exception(error_message.format(error)) from error

    @staticmethod
    def is_raw_input_nrt(cur: psycopg2.extensions.cursor, nrt_start_date: int, raw_input_id: str) -> bool:
        """
        Determines whether the new inserted raw input is nrt or not following these rules:
        - if a nrt_start_date exists, the raw input measurement_day must be greater than or equal to the nrt_start_date.
        - else, the raw input publishing_date must be less than or equal to the harvesting_date which must be less than
        or equal to publishing_date + 3 hours

        @param cur: the psycopg2 cursor to execute the request
        @param nrt_start_date: this date characterizes data retrieval from the wekeo resto api further back in time
        by the harvester. In normal operation, this date does not exist.
        @param raw_input_id: the unique id of the raw input.
        @return boolean proving whether is_nrt exists or not
        """
        error_message = 'is_raw_input_nrt error: {}'

        try:
            if nrt_start_date:
                req = IS_NRT_FROM_PAST_HARVEST_DATE_REQ.format(nrt_start_date, raw_input_id)
                res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
                is_nrt = res.fetchone()['is_measurement_day_valid']
            else:
                req = IS_NRT_FROM_NOW_HARVEST_DATE_REQ.format(raw_input_id)
                res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
                is_nrt = res.fetchone()['is_within_3hours']

            return is_nrt

        except (psycopg2.OperationalError, TypeError) as error:
            raise type(error)(error_message.format(error)) from error
        except Exception as error:
            raise Exception(error_message.format(error)) from error

    def gfsc_daily_tasks(self) -> None:
        """
        Every day between 22:00 UTC - 04:00 UTC every half hour, this method trigger production of all GFSC for the
        previous day:
        For each date between last_gfsc_processing_date and yesterday :
        - IF there is any unprocessed FSC and SWS in the last GFSC processing date
          no GFSC production is triggered for each tile for the 7 next days
        - IF there is not any unprocessed FSC and SWS in the last GFSC processing date, all the FSC and SWS products
          in the last GFSC processing date minus 7 days are retrieved. All GFSC are created for each tile and 
          last_gfsc_processing_date is update by 1 in database.
        Then, the trigger validation process requested by the orchestrator to create the processing tasks is executed.
        """

        error_message = 'gfsc_daily_tasks error: {}'
        sws_fsc_tc_code_str = "('FSC_TC', 'SWS_TC')"
        cc_sig0_tc_code_str = "('CC_TC', 'Backscatter_10m_TC')"
        date_format = '%Y%m%d'
        tc_name = 'GFSC_TC'
        product_type = 'GFSC_L2C'

        try:
            self.logger.info("Begin GFSC daily task.")

            with HRWSIDatabaseApiManager.database_connection() as (conn, cur):
                # Convert cur in a dict and activate autocommit
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                # Get the last GFSC processing date
                req = GET_LAST_PROCESSING_DATE.format(product_type)
                res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
                last_gfsc_processing_date = res.fetchone()['last_processing_date']
                self.logger.info(f"last_gfsc_processing_date : {last_gfsc_processing_date}")

                today_int = int(datetime.datetime.now().date().strftime('%Y%m%d'))
                self.logger.info(f"today_int : {today_int}")

                # flag to know if the last_gfsc_processing_date is updated in the database
                flag_update_gfsc_date_in_db = True

                while last_gfsc_processing_date < today_int :
                    # Check if all GRDs and L1Cs have been processed successfully
                    req = NB_OF_NOT_SUCCESSFULLY_PROCESSED_TASK_FOR_A_DAY_AND_SPECIFICS_ROUTINES.format(cc_sig0_tc_code_str,
                                                                                        last_gfsc_processing_date)
                    res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
                    nb_of_GRD_L1C_not_successfully_processed = int(res.fetchone()['total'])
                    self.logger.info(f"nb_of_GRD_L1C_not_successfully_processed : {nb_of_GRD_L1C_not_successfully_processed}")

                    # Check if all FSCs and SWSs have been processed successfully
                    req = NB_OF_NOT_SUCCESSFULLY_PROCESSED_TASK_FOR_A_DAY_AND_SPECIFICS_ROUTINES.format(sws_fsc_tc_code_str,
                                                                                        last_gfsc_processing_date)
                    res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
                    nb_of_FSC_SWS_not_successfully_processed = int(res.fetchone()['total'])
                    self.logger.info(f"nb_of_FSC_SWS_not_successfully_processed : {nb_of_FSC_SWS_not_successfully_processed}")

                    if nb_of_GRD_L1C_not_successfully_processed + nb_of_FSC_SWS_not_successfully_processed != 0:
                        flag_update_gfsc_date_in_db = False
                        # Update the last gfsc processing date only in local -
                        # current last GFSC processing date + 7 day
                        temp_date = datetime.datetime.strptime(str(last_gfsc_processing_date), date_format)
                        updated_gfsc_processing_date = int(
                                    (temp_date + datetime.timedelta(days=7)).strftime(date_format))
                        last_gfsc_processing_date = updated_gfsc_processing_date
                        self.logger.info(f"Not all FSC / SWS are successfully processed, new last_gfsc_processing_date : {last_gfsc_processing_date}")

                    else:
                        # Calculate the last GFSC processing date - 7 days
                        temp_date = datetime.datetime.strptime(str(last_gfsc_processing_date), date_format)
                        last_gfsc_processing_date_minus_7_days = int(
                            (temp_date - datetime.timedelta(days=7)).strftime(date_format))
                        self.logger.info(f"last_gfsc_processing_date_minus_7_days : {last_gfsc_processing_date_minus_7_days}")

                        # Get all the FSC and SWS products from the (last GFSC processing date - 7 days)
                        # to the last GFSC processing date
                        self.logger.info(f"------------")
                        for tile in self.tile_list:
                            self.logger.info(f"tile : {tile}")
                            req = GET_ALL_FSC_AND_SWS_IN_THE_LAST_7_DAYS.format(tile,
                                                                                last_gfsc_processing_date_minus_7_days,
                                                                                last_gfsc_processing_date)
                            res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
                            fsc_and_sws_products_in_the_last_7_days = res.fetchall()
                            self.logger.info(f"fsc_and_sws_products_in_the_last_7_days : {fsc_and_sws_products_in_the_last_7_days}")

                            if fsc_and_sws_products_in_the_last_7_days :
                                # Check if GFSC trigger already exist with the exact same input
                                req = GFSC_TC_ALREADY_EXIST.format([row['id'] for row in fsc_and_sws_products_in_the_last_7_days], tc_name, last_gfsc_processing_date)
                                res = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
                                gfsc_tc_already_exist = res.fetchone()['exists']
                                self.logger.info(f"gfsc_tc_already_exist : {gfsc_tc_already_exist}")
                                if not gfsc_tc_already_exist:
                                    self.logger.info(f"FSC or SWS in last 7 days : create TC + R2V")
                                    self.create_trigger_validation(fsc_and_sws_products_in_the_last_7_days,
                                                                   tc_name,
                                                                   cur, validation_date=datetime.datetime.now(),
                                                                   artificial_measurement_day=last_gfsc_processing_date)
                            self.logger.info(f"------------")

                        # Update the last gfsc processing date -
                        # current last GFSC processing date + 1 day
                        temp_date = datetime.datetime.strptime(str(last_gfsc_processing_date), date_format)
                        updated_gfsc_processing_date = int(
                                    (temp_date + datetime.timedelta(days=1)).strftime(date_format))
                        # Update in database only if flag_update_gfsc_date_in_db
                        if flag_update_gfsc_date_in_db:
                            self.logger.info(f"Update last_gfsc_processing_date in Database.")
                            req = SET_LAST_GFSC_PROCESSING_DATE.format(updated_gfsc_processing_date)
                            _ = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)

                        last_gfsc_processing_date = updated_gfsc_processing_date
                        self.logger.info(f"new last_gfsc_processing_date : {last_gfsc_processing_date}")

        except psycopg2.OperationalError as error:
            self.logger.error(error_message.format(error))

    def wics1s2_daily_tasks(self) -> None:
        """
        The WICS1S2 processing routine works as follows : if at least one WIC S1 product and at least one WIC S2 product
        were sensed on the same measurement date, they are merged two by two into a new WICS1S2.
        """

        error_message = 'The wics1s2_daily_tasks method failed: {}'
        tc_name = 'WICS1S2_TC'
        raw2valid_data = ()

        try:
            today_str = datetime.datetime.now().strftime('%Y%m%d')
            today_int = int(today_str)

            with HRWSIDatabaseApiManager.database_connection() as (conn, cur):
                # Convert cur in a dict and activate autocommit
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                # Retrieve all pairs of WICS1 and WICS2 that have the same measurement date, the same tile
                res = HRWSIDatabaseApiManager.execute_request_in_database(cur, GET_WICS1S2_PAIRS_REQUEST)
                rows = res.fetchall()

                for row in rows:
                    wics1_id = row['id_wics1']

                    # Check that WICS1 do not already have an associated WICS1S2 processing task.
                    request=IS_ONE_TRIGGER_VALIDATION_EXISTS_FOR_AN_INPUT % (str(wics1_id), str(tc_name))
                    res = HRWSIDatabaseApiManager.execute_request_in_database(
                        cur, request)
                    wics1_pt_not_exists = res.fetchone()['result']

                    if wics1_pt_not_exists:
                        wics2_ids = row['wics2_ids']
                        measurement_date = row['measurement_day']

                        # Uses the current time as the validation date
                        processing_date = datetime.datetime.now()

                        # Check that the measurement date is the current date. Required to define the NRT status.
                        if measurement_date == today_int:
                            trigger_validation_data = ((tc_name, processing_date, True, None),)
                        else:
                            trigger_validation_data = ((tc_name, processing_date, False, None),)

                        # Trigger validation insertion
                        cur = HRWSIDatabaseApiManager.execute_request_in_database(cur, self.INSERT_TRIGGER_VALIDATION,
                                                                                  trigger_validation_data)
                        trigger_validation_id = cur.fetchone()['id']

                        raw2valid_data += ((trigger_validation_id, wics1_id),)
                        # Loop on wics2 id
                        for wics2_id in wics2_ids:
                            raw2valid_data += ((trigger_validation_id, wics2_id),)

                # If pairs exist, insert data in raw2valid table
                if raw2valid_data:
                    _ = HRWSIDatabaseApiManager.execute_request_in_database(cur, self.INSERT_RAW2VALID, raw2valid_data)

        except psycopg2.OperationalError as error:
            self.logger.error(error_message.format(error))

def main():
    parser = argparse.ArgumentParser(description="Launch a job with a specific flavour.")

    parser.add_argument(
        "-c", "--configuration-folder-path",
        type=Path,
        dest="configuration_folder",
        required=True,
        help="The path of the configuration folder."
    )

    args = parser.parse_args()

    triggerer: Triggerer = Triggerer(
        args.configuration_folder
    )
    triggerer.run()

if (__name__ == "__main__"):  # pragma: no cover
    main()