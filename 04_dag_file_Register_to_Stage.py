from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import time
from datetime import datetime
from datetime import datetime, timedelta
import requests
import json
#import pypff
import os
import pandas as pd
import numpy as np
import hashlib

from ETL.register_to_stage import main

def execute_reg_to_stg():
   main()

default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 0,
   #'retry_delay': timedelta(minutes = 5)
}
with DAG('04-Register_To_Stage',
      schedule=None,
      start_date = datetime(2023, 4, 26),
      max_active_runs = 1,
      default_args = default_args,
      catchup = False
   ) as dag:
    exec_reg_to_stg_task = PythonOperator(
     task_id='exec_reg_to_stg_operator',
     python_callable=execute_reg_to_stg
   )
