import os
import sys
import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator



"""
default args to set the schedule
"""

default_args = {
    'owner': 'refresh scripts',
    'depends_on_past': False,
    'on_failure_callback': None,
    'start_date': '2019-11-19',
    'catchup': False,
    'retries': 0,
}

dag = DAG('data_mart_refresh_daily',
          default_args=default_args,
          description='Load in daily Python Script and refresh DBT models',
          schedule_interval="0 9 * * *",
          catchup=False

          )

t0 = BashOperator(
    task_id='python_load',
    bash_command='python3 dubai_stream.py',
    dag=dag
)

t1 = BashOperator(
    task_id='most_upvoted_and_comments',
    bash_command='dbt run --profile prod-gcp --models most_upvoted_and_comments',
    dag=dag
)

t2 = BashOperator(
    task_id='count_of_submissions',
    bash_command='dbt run --profile prod-gcp --models count_of_submissions',
    dag=dag
)

t0 >> t1
t0 >> t2