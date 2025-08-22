import psycopg2
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime as DateTime

from magellium.hrwsi.system.harvesters.application.ports.outputs.repository import HarvesterRepository
from magellium.hrwsi.system.core.sentinel_tiles import SentinelTile
from magellium.hrwsi.system.common.logger import LoggerFactory
from magellium.hrwsi.system.common.states import TileProcessState


class PostgreSqlHarvesterRepository(HarvesterRepository):

    LOGGER = LoggerFactory.get_logger(__name__)
    __pool: SimpleConnectionPool | None = None

    def __init__(self, host: str, port: int, username: str, password: str, database_name: str, minconn: int = 5, maxconn: int = 25):
        if PostgreSqlHarvesterRepository.__pool is None:
            dsn = f"host={host} port={port} user={username} password={password} dbname={database_name}"
            PostgreSqlHarvesterRepository.__pool = SimpleConnectionPool(minconn, maxconn, dsn)
            self.LOGGER.info(f"SQL connection pool initialized [min={minconn}, max={maxconn}]")

    def __get_connection(self):
        if PostgreSqlHarvesterRepository.__pool is None:
            raise RuntimeError("SQL connection pool is not initialized")
        sql_connection: psycopg2.extensions.connection = PostgreSqlHarvesterRepository.__pool.getconn()
        self.LOGGER.debug("SQL connection acquired from pool")
        return sql_connection

    def __release_connection(self, sql_connection: psycopg2.extensions.connection):
        PostgreSqlHarvesterRepository.__pool.putconn(sql_connection)
        self.LOGGER.debug("SQL connection released back to pool")

    def __execute_read_query(self, query: str, parameters: tuple) -> list[tuple]:
        sql_connection: psycopg2.extensions.connection = self.__get_connection()
        try:
            with sql_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as sql_cursor:
                self.LOGGER.debug(f"Executing SQL query: {query} with parameters {parameters}")
                sql_cursor.execute(query, parameters)
                records = sql_cursor.fetchall()
                self.LOGGER.debug(f"Query returned {len(records)} rows")
                return records
        except psycopg2.Error as exception:
            self.LOGGER.error(f"SQL error in query [{query}] with params {parameters}: {exception}", exc_info=True)
        finally:
            self.__release_connection(sql_connection)

    def __find(self, query: str, parameters: tuple) -> list[SentinelTile]:
        records = self.__execute_read_query(query, parameters)
        tiles = self.__map_records_to_sentinel_tiles(records)
        self.LOGGER.info(f"Mapped {len(tiles)} records to SentinelTile objects")
        return tiles

    
    def find_all_sentinel_tiles_by_state(self, state: TileProcessState) -> list[SentinelTile]:
        query = "SELECT * FROM table WHERE state = %s"
        self.LOGGER.info(f"Finding all SentinelTiles with state: {state.name}")
        return self.__find(query, (state.value,))

    def find_all_sentinel_tiles_before_date_by_state(self, to_date: DateTime, state: TileProcessState) -> list[SentinelTile]:
        query = "SELECT * FROM table WHERE acquisition_date <= %s AND state = %s"
        self.LOGGER.info(f"Finding SentinelTiles before {to_date.isoformat()} with state: {state.name}")
        return self.__find(query, (to_date, state.value))
    

    def find_all_sentinel_tiles_after_date_by_state(self, from_date: DateTime, state: TileProcessState) -> list[SentinelTile]:
        query = "SELECT * FROM table WHERE acquisition_date >= %s AND state = %s"
        self.LOGGER.info(f"Finding SentinelTiles after {from_date.isoformat()} with state: {state.name}")
        return self.__find(query, (from_date, state.value))
    
    def find_all_sentinel_tiles_between_dates_by_state(self, from_date: DateTime, to_date: DateTime, state: TileProcessState) -> list[SentinelTile]:
        query = "SELECT * FROM table WHERE acquisition_date >= %s AND acquisition_date <= %s AND state = %s"
        self.LOGGER.info(f"Finding SentinelTiles between {from_date.isoformat()} - {to_date.isoformat()} with state: {state.name}")
        return self.__find(query, (from_date, to_date, state.value))

    # CANDIDATE_ALREADY_IN_DATABASE_REQUEST
    # SELECT input_path FROM hrwsi.raw_inputs ri WHERE ri.measurement_day>=%s AND ri.product_type_code='%s';

    # GET_LAST_PUBLISHING_DATE_INPUT
    # SELECT ri.publishing_date FROM hrwsi.raw_inputs ri WHERE ri.product_type_code='%s' ORDER BY ri.start_date DESC LIMIT 1;

    # GET_UNPROCESSED_PRODUCTS_REQUEST
    # SELECT id, product_type_code, product_path, TO_CHAR(creation_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS creation_date, TO_CHAR(catalogue_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS catalogue_date, kpi_file_path FROM hrwsi.products p WHERE p.id NOT IN (SELECT ri.id FROM hrwsi.raw_inputs ri)

    # GET_WEKEO_API_MANAGER_PARAMS
    # SELECT triggering_condition_name, input_type, collection, max_day_since_publication_date, max_day_since_measurement_date, tile_list_file, geometry_file, polarisation, timeliness, nrt_harvest_start_date, archive_harvest_start_date, archive_harvest_end_date FROM systemparams.wekeo_api_manager

    # GRD_CANDIDATE_ALREADY_IN_DATABASE_REQUEST
    # SELECT ri.tile, ri.start_date FROM hrwsi.raw_inputs ri WHERE ri.measurement_day>=%s AND ri.product_type_code='%s';

    # INSERT_CANDIDATE_REQUEST
    # INSERT INTO hrwsi.raw_inputs (id, product_type_code, start_date, publishing_date, tile, measurement_day, relative_orbit_number, input_path, is_partial, harvesting_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()) ON CONFLICT (id) DO NOTHING

    # LISTEN_PRODUCTS_REQUEST
    # LISTEN product_insertion

    # UNSET_HARVEST_START_DATES
    # UPDATE systemparams.wekeo_api_manager SET %s WHERE CONCAT(triggering_condition_name, timeliness) = '%s';

    @staticmethod
    def __map_record_to_sentinel_tile(record: dict) -> SentinelTile:
        return SentinelTile.map_from_dict_record(record)

    @staticmethod
    def __map_records_to_sentinel_tiles(records: list[dict]) -> list[SentinelTile]:
        return [PostgreSqlHarvesterRepository.__map_record_to_sentinel_tile(r) for r in records]
