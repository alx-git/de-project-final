from airflow.operators.python import get_current_context
from datetime import datetime
from logging import Logger

from py.dwh.dwh_loader import DwhLoader
from py.lib.settings import StgTimeSettingsRepository
from py.lib.vertica_connect import VerticaConnect


class DwhRepository:

    def __init__(self, loader: DwhLoader, vertica_dest: VerticaConnect, logger: Logger) -> None:
        
        self.loader = loader
        self.vertica_dest = vertica_dest
        self.settings_repository = StgTimeSettingsRepository()
        self.log = logger

    def global_metrics_repository(self) -> int:

        context = get_current_context()
        current_update_dt = context["execution_date"].date()
        db = 'global_metrics'
        
        if not self.settings_repository.get_setting(
            self.vertica_dest.connection(), db):
            latest_update_dt = datetime.min
        else:
            latest_update_dt = self.settings_repository.get_setting(
            self.vertica_dest.connection(), db)[0].date()
        
        self.loader.global_metrics_loader(latest_update_dt, current_update_dt)

        self.settings_repository.save_setting(self.vertica_dest.connection(),
                                              db, current_update_dt)

