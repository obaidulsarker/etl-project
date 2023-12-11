from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
#from airflow.utils.trigger_rule import TriggerRule

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

from ETL.pre_validation_JSON_LOG_master_v2 import main

def execute_pre_validation():
   main()

default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 0
}
with DAG('02-Pre-Validation',
      schedule=None,
      start_date = datetime(2023, 4, 26),
      max_active_runs = 1,
      default_args = default_args,
      catchup = False,
      #trigger_rule=TriggerRule.ALL_DONE,
   ) as dag:
    exec_pre_validation_task = PythonOperator(
     task_id='exec_Pre_Validation_operator',
     python_callable=execute_pre_validation
   )
