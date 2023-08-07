from airflow.operators.python import get_current_context
from datetime import datetime
from logging import Logger

from py.lib.pg_connect import PgConnect
from py.lib.settings import StgTimeSettingsRepository
from py.lib.vertica_connect import VerticaConnect
from py.stg.stg_loader import StgLoader
from py.stg.stg_reader import StgReader


class StgRepository:

    def __init__(self, reader: StgReader, loader: StgLoader, pg_dest: PgConnect,
                 vertica_dest: VerticaConnect, logger: Logger) -> None:
        
        self.reader = reader
        self.loader = loader
        self.pg_dest = pg_dest
        self.vertica_dest = vertica_dest
        self.settings_repository = StgTimeSettingsRepository()
        self.log = logger

    def transactions_repository(self) -> int:

        context = get_current_context()
        current_update_dt = context["execution_date"]
        db = 'transactions'
        
        if not self.settings_repository.get_setting(
            self.vertica_dest.connection(), db):
            latest_update_dt = datetime.min
        else:
            latest_update_dt = self.settings_repository.get_setting(
            self.vertica_dest.connection(), db)[0]
        
        obj = self.reader.transactions_reader(latest_update_dt, current_update_dt)
        if not obj:
            self.log.info("Quitting.")
            return 0
        
        self.loader.transactions_loader(obj)

        self.settings_repository.save_setting(self.vertica_dest.connection(),
                                              db, current_update_dt)

    def currencies_repository(self) -> int:

        context = get_current_context()
        current_update_dt = context["execution_date"]
        db = 'currencies'
        
        if not self.settings_repository.get_setting(
            self.vertica_dest.connection(), db):
            latest_update_dt = datetime.min
        else:
            latest_update_dt = self.settings_repository.get_setting(
            self.vertica_dest.connection(), db)[0]
        
        obj = self.reader.currencies_reader(latest_update_dt, current_update_dt)
        if not obj:
            self.log.info("Quitting.")
            return 0
        
        self.loader.currencies_loader(obj)

        self.settings_repository.save_setting(self.vertica_dest.connection(),
                                              db, current_update_dt)