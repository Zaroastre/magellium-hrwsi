#!/usr/bin/env python3
"""
Orchestrator module is used to interact with the HRWSI Database to create processing tasks.
"""

import argparse
import asyncio
import datetime
import json
import logging
from pathlib import Path

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from magellium.hrwsi.system.apimanager.api_manager import ApiManager
from magellium.hrwsi.system.apimanager.hrwsi_database_api_manager import HRWSIDatabaseApiManager
from magellium.hrwsi.system.settings.queries_and_constants import (
    ADD_PROCESSING_TASK_REQUEST,
    GET_PROCESSING_DATE_GFSC_PT,
    GET_UNPROCESSED_RAW2VALID_INPUTS_REQUEST,
    LISTEN_RAW2VALID_REQUEST,
    NOTIFY_RAW2VALID_REQUEST,
    PROCESSING_ROUTINE_REQUEST,
    PT_ALREADY_IN_DATABASE_REQUEST,
)
from magellium.hrwsi.utils.logger import LogUtil


class NrtDailyOrchestrator:
    """Manage interaction between Database objects and Orchestrator objects"""

    LOGGER_LEVEL = logging.DEBUG
    PROCESSING_ROUTINE_REQUEST = PROCESSING_ROUTINE_REQUEST
    ADD_PROCESSING_TASK_REQUEST = ADD_PROCESSING_TASK_REQUEST
    PT_ALREADY_IN_DATABASE_REQUEST = PT_ALREADY_IN_DATABASE_REQUEST
    LISTEN_REQUEST = LISTEN_RAW2VALID_REQUEST
    GET_PROCESSING_DATE_GFSC_PT = GET_PROCESSING_DATE_GFSC_PT
    GET_UNPROCESSED_RAW2VALID_INPUTS_REQUEST = GET_UNPROCESSED_RAW2VALID_INPUTS_REQUEST
    NOTIFY_RAW2VALID_REQUEST = NOTIFY_RAW2VALID_REQUEST

    def __init__(self, configuration_folder: Path):

        self.logger = LogUtil.get_logger('Log_orchestrator', self.LOGGER_LEVEL, "log_orchestrator/logs.log")

        # Load config file
        config_data = ApiManager.read_config_file(configuration_folder)

        self.r2v_queue = asyncio.Queue()
        self._stop_event = asyncio.Event()
        self.interval = config_data["async_loop"]["interval"]

    async def run_cycle(self) -> None:  # pragma: no cover
        """
        Run orchestrator workflow :
        - wait for raw2valid insertion notifications
        - create processing tasks
        """

        error_message = 'Orchestrator error: {}'

        try:
            while True:
                # Init and start the tasks
                self.logger.info("Initializing the Orchestrator")

                tasks = [
                    asyncio.create_task(self.handle_notify(), name="handle_notify_task"),
                    asyncio.create_task(self.handle_r2v_input(), name="handle_r2v_input_task")
                ]
                self.logger.info("Orchestrator handle_notify_task and handle_r2v_input_task started.")

                # Tasks are restarted at regular intervals
                await asyncio.sleep(self.interval)

                self.logger.info("Restarting Orchestrator cycle...")
                # Stop and cancel the tasks cleanly
                await self.stop_tasks()
                [task.cancel() for task in tasks]
                await asyncio.gather(*tasks, return_exceptions=True)

                # The asyncio event is used to notify multiple asyncio tasks that some event has happened.
                # Reset the event for the next Harvester cycle
                self._stop_event = asyncio.Event()

        except (TypeError, RuntimeError, Exception) as error:
            self.logger.error(error_message.format(error))

    async def stop_tasks(self):
        """Stop cleanly the running task."""

        self.logger.info("Stopping current tasks...")
        self._stop_event.set()

    async def handle_notify(self) -> None:
        """
        When a raw2valid input insertion notification pop, collect inputs, create plan and processing tasks
        and clear notification.
        """

        self.logger.info("Begin handle_notify : Receive input insertion notification")
        error_message = "Error in handle_notify : {}"

        try:
            # Connect to Database
            with HRWSIDatabaseApiManager.database_connection() as (conn, cur):
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

                # Listen channel
                _ = HRWSIDatabaseApiManager.execute_request_in_database(cur, self.LISTEN_REQUEST)


                # If the Orchestrator service is restarted, all unprocessed raw_2_valid inputs are retrieved
                # and sent to the raw2valid_insertion database notify/listen channel.
                HRWSIDatabaseApiManager.get_statements_to_notify_again(GET_UNPROCESSED_RAW2VALID_INPUTS_REQUEST,
                                                                       NOTIFY_RAW2VALID_REQUEST,
                                                                       self.logger)

                while not self._stop_event.is_set():
                    # Prevents conn.poll from blocking the asyncio event loop
                    await asyncio.to_thread(conn.poll)

                    while conn.notifies:
                        notify = conn.notifies.pop(0)
                        self.r2v_queue.put_nowait(notify.payload)
                        self.logger.debug("Insertion raw2valid type : %s", notify.payload)

        except (KeyError, psycopg2.OperationalError, TypeError) as error:
            self.logger.error(error_message.format(error))
        except Exception as error:
            self.logger.error(error_message.format(error))

    async def handle_r2v_input(self) -> None:
        """
        When an insertion notification pop, collect r2v input and create processing task.
        """

        error_message = 'handle_r2v_input error: {}'

        try:
            # while the queue is not empty, pop a raw2valid item from the queue and process it.
            while True:
                with HRWSIDatabaseApiManager.database_connection() as (conn, cur):
                    # Convert cur in a dict and activate autocommit
                    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as dict_cur:
                        queue_item = await self.r2v_queue.get()
                        await self.create_processing_task(dict_cur, queue_item)

        except (psycopg2.OperationalError, KeyError) as error:
            raise type(error)(error_message.format(error)) from error
        except Exception as error:
            raise Exception(error_message.format(error)) from error

    async def create_processing_task(self, cur: psycopg2.extensions.cursor, queue_item: str) -> None:
        """
        Verify that the raw input has no processing task,
        then create a new processing task.
        """

        self.logger.info("Begin create_processing_task")
        error_message = 'create_processing_task error: {}'
        date_format = "%Y-%m-%d %H:%M:%S"

        try:
            r2v_input = json.loads(queue_item)
            raw_input_id = r2v_input.get("raw_input_id")
            trigger_validation_id = r2v_input.get("trigger_validation_id")
            if raw_input_id is None or trigger_validation_id is None:
                raise ValueError('raw_input_id or trigger_validation_id is None')

            # Checking that no processing task already exists for a triggering condition id
            req = PT_ALREADY_IN_DATABASE_REQUEST.format(trigger_validation_id)
            cur = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
            is_pt_exists = cur.fetchone()

            if is_pt_exists:
                self.logger.error(
                    "The trigger validation id %s already has a processing task, passing.", str(trigger_validation_id))
            else:
                # Get current datetime and formating as follows 'YYYY-MM-DD HH:MM:SS'
                now = datetime.datetime.now()
                formated_date = now.strftime(date_format)
                processing_date = None

                # GFSC specific use case:
                # - Get the triggering condition name
                # - If the triggering condition name is GFSC_TC,
                # - Else: today date is used
                req = self.GET_PROCESSING_DATE_GFSC_PT.format(raw_input_id, trigger_validation_id)
                cur = HRWSIDatabaseApiManager.execute_request_in_database(cur, req)
                res = cur.fetchone()
                if res['triggering_condition_name'] == 'GFSC_TC':
                    processing_date = res['artificial_measurement_day']

                req_pt_insert = self.ADD_PROCESSING_TASK_REQUEST
                data_to_insert = ((trigger_validation_id, formated_date, False, processing_date),)
                HRWSIDatabaseApiManager.execute_request_in_database(cur, req_pt_insert, data_to_insert)
        
        except psycopg2.errors.UniqueViolation as error: # pragma no cover
            self.logger.error(error)
        except (psycopg2.OperationalError, ValueError, TypeError) as error:
            raise type(error)(error_message.format(error)) from error
        except Exception as error:
            raise Exception(error_message.format(error)) from error

def main():  # pragma: no cover
    parser = argparse.ArgumentParser(description="Launch a job with a specific flavour.")

    parser.add_argument(
        "-c", "--configuration-folder-path",
        type=Path,
        dest="configuration_folder",
        required=True,
        help="The path of the configuration folder."
    )

    args = parser.parse_args()

    orchestrator = NrtDailyOrchestrator(args.configuration_folder)
    asyncio.run(orchestrator.run_cycle())

if (__name__ == "__main__"):  # pragma: no cover
    main()