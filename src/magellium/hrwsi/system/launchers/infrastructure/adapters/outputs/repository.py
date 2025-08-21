from magellium.hrwsi.system.harvesters.application.ports.outputs.repository import HarvesterRepository

class PostgreSqlHarvesterRepository(HarvesterRepository):
    def __init__(self, host: str, port: int, username: str, password: str, database_name: str):
        self.__host: str = host
        self.__port: int = port
        self.__username: str = username
        self.__password: str = password
        self.__database_name: str = database_name