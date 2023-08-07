import logging
import os
import pendulum
import sys

from airflow import DAG
from airflow.decorators import task

parent_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(parent_dir, '..'))
from py.lib.pg_connect import PgConnectionBuilder
from py.lib.vertica_connect import VerticaConnectionBuilder
from py.stg.stg_repository import StgRepository
from py.stg.stg_reader import StgReader
from py.stg.stg_loader import StgLoader


log = logging.getLogger(__name__)

with DAG(
    dag_id='stg',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    catchup=False,
    tags=['stg'],
    is_paused_upon_creation=False
) as dag:
    
    pg = PgConnectionBuilder.pg_conn("PG_CONNECTION")
    vertica = VerticaConnectionBuilder().connect('VERTICA_CONNECTION')

    @task(task_id="stg_transactions_load")
    def load_stg_transactions(ds=None, **kwargs):
    
        stg_repository = StgRepository(
            StgReader(pg),
            StgLoader(vertica), 
            pg,
            vertica,
            log)
        
        stg_repository.transactions_repository()

    @task(task_id="stg_currencies_load")
    def load_stg_currencies(ds=None, **kwargs):
    
        stg_repository = StgRepository(
            StgReader(pg),
            StgLoader(vertica), 
            pg,
            vertica,
            log)

        stg_repository.currencies_repository()        
 
    stg_transactions = load_stg_transactions()
    stg_currencies = load_stg_currencies()
    stg_transactions
    stg_currencies

    