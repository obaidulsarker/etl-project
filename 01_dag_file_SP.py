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
default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 0
}
with DAG('01-SP_to_VM',
      schedule=None,
      start_date = datetime(2023, 4, 26),
      default_args = default_args,
      catchup = False
   ) as dag:
    exec_sharepoint_task = BashOperator(
     task_id='exec_SP_to_Blob_operator',
     bash_command='./sp_to_vm.sh',
   )
