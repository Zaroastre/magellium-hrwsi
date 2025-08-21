import psycopg2
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime as DateTime

from magellium.hrwsi.system.harvesters.application.ports.outputs.repository import HarvesterRepository
from magellium.hrwsi.system.core.entities import SentinelTile
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
            with sql_connection.cursor() as sql_cursor:
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

    @staticmethod
    def __map_record_to_sentinel_tile(record: tuple) -> SentinelTile:
        # TODO: mappe correctement record -> SentinelTile
        return SentinelTile(record[0], record[1], record[2])

    @staticmethod
    def __map_records_to_sentinel_tiles(records: list[tuple]) -> list[SentinelTile]:
        return [PostgreSqlHarvesterRepository.__map_record_to_sentinel_tile(r) for r in records]
