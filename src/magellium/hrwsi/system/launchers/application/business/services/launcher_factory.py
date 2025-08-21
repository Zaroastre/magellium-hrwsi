from magellium.hrwsi.system.launchers.application.ports.outputs.repository import Repository
from magellium.hrwsi.system.launchers.application.business.services.launcher import LauncherService
from magellium.hrwsi.system.launchers.application.business.services.archive_launcher import ArchiveLauncherService
from magellium.hrwsi.system.launchers.application.business.services.near_real_time_launcher import NearRealTimeLauncherService
from magellium.hrwsi.system.common.modes import RunMode


class LauncherServiceFactory:
    
    @staticmethod
    def create_archive_launcher_service(repository: Repository) -> LauncherService:
        return ArchiveLauncherService(repository)
    
    @staticmethod
    def create_near_real_time_launcher_service(repository: Repository) -> LauncherService:
        return NearRealTimeLauncherService(repository)
    
    @staticmethod
    def create_launcher_service(run_mode: RunMode, repository: Repository) -> LauncherService:
        if (run_mode == RunMode.ARCHIVE):
            return LauncherServiceFactory.create_archive_launcher_service(repository)
        elif (run_mode == RunMode.NEAR_REAL_TIME):
            return LauncherServiceFactory.create_near_real_time_launcher_service(repository)
        else:
            raise ValueError("Invalid run mode")
