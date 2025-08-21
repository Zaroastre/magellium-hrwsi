from datetime import datetime as DateTime

from magellium.hrwsi.system.launchers.application.business.services.launcher import LauncherService
from magellium.hrwsi.system.launchers.application.business.use_cases import (
    UseCase, 
    HarvestAllTilesWithErrorStateUseCase, 
    HarvestAllTilesWithIdleStateUseCase,
    HarvestTilesWithErrorStateAfterDateUseCase,
    HarvestTilesWithErrorStateBeforeDateUseCase,
    HarvestTilesWithErrorStateBetweenDatesUseCase,
    HarvestTilesWithIdleStateAfterDateUseCase,
    HarvestTilesWithIdleStateBeforeDateUseCase,
    HarvestTilesWithIdleStateBetweenDatesUseCase
)
from magellium.scheduler import Scheduler

class LauncherProcessManager:
    def __init__(self, service: LauncherService, harvest_from_date: DateTime|None, harvest_to_date: DateTime|None):
        self.__scheduler: Scheduler = Scheduler()

        self.__harvest_from_date: DateTime|None = harvest_from_date
        self.__harvest_to_date: DateTime|None = harvest_to_date

        self.__harvest_all_tiles_with_idle_state_use_case: UseCase = HarvestAllTilesWithIdleStateUseCase(service)
        self.__harvest_all_tiles_with_error_state_use_case: UseCase = HarvestAllTilesWithErrorStateUseCase(service)
        self.__harvest_tiles_with_idle_state_after_date_use_case: UseCase = HarvestTilesWithIdleStateAfterDateUseCase(service, harvest_from_date)
        self.__harvest_tiles_with_error_state_after_date_use_case: UseCase = HarvestTilesWithErrorStateAfterDateUseCase(service, harvest_from_date)
        self.__harvest_tiles_with_idle_state_before_date_use_case: UseCase = HarvestTilesWithIdleStateBeforeDateUseCase(service, harvest_to_date)
        self.__harvest_tiles_with_error_state_before_date_use_case: UseCase = HarvestTilesWithErrorStateBeforeDateUseCase(service, harvest_to_date)
        self.__harvest_tiles_with_idle_state_between_dates_use_case: UseCase = HarvestTilesWithIdleStateBetweenDatesUseCase(service, harvest_from_date, harvest_to_date)
        self.__harvest_tiles_with_error_state_between_dates_use_case: UseCase = HarvestTilesWithErrorStateBetweenDatesUseCase(service, harvest_from_date, harvest_to_date)

        self.harvesting_execution_interval_in_seconds: int = 5

    def __harvest_data(self) -> None:
        if ((self.__harvest_from_date is not None) and (self.__harvest_to_date is not None)):
            self.__harvest_tiles_with_idle_state_between_dates_use_case.execute()
            self.__harvest_tiles_with_error_state_between_dates_use_case.execute()
        elif (self.__harvest_from_date is not None):
            self.__harvest_tiles_with_idle_state_after_date_use_case.execute()
            self.__harvest_tiles_with_error_state_after_date_use_case.execute()
        elif (self.__harvest_to_date is not None):
            self.__harvest_tiles_with_idle_state_before_date_use_case.execute()
            self.__harvest_tiles_with_error_state_before_date_use_case.execute()
        else:
            self.__harvest_all_tiles_with_idle_state_use_case.execute()
            self.__harvest_all_tiles_with_error_state_use_case.execute()


    def start_harvesting(self) -> None:
        harvesting_task: callable = self.__harvest_data
        self.__scheduler.add_job(harvesting_task, self.harvesting_execution_interval_in_seconds)
        self.__scheduler.start()
    
    def stop_harvesting(self) -> None:
        self.__scheduler.stop()