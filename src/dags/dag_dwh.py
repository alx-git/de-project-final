import logging
import os
import pendulum
import sys

from airflow import DAG
from airflow.decorators import task

parent_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(parent_dir, '..'))
from py.dwh.dwh_repository import DwhRepository
from py.dwh.dwh_loader import DwhLoader
from py.lib.vertica_connect import VerticaConnectionBuilder


log = logging.getLogger(__name__)

with DAG(
    dag_id='dwh',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    catchup=False,
    tags=['dwh'],
    is_paused_upon_creation=False
) as dag:
    
    vertica = VerticaConnectionBuilder().connect('VERTICA_CONNECTION')

    @task(task_id="dwh_global_metrics_load")
    def load_dwh_global_metrics(ds=None, **kwargs):
    
        dwh_repository = DwhRepository(
            DwhLoader(vertica), 
            vertica,
            log)
        
        dwh_repository.global_metrics_repository()

    dwh_global_metrics = load_dwh_global_metrics()
    dwh_global_metrics

    