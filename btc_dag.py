from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args ={
    "owner": "Bozza",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 31),
    "email": ["eric.katumo@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}
with DAG(
    'polygon_btc_data',
    default_args=default_args,
    schedule_interval='@daily',

) as dag:

# Activate virtual environment
    activate_venv = BashOperator(
        task_id='activate_virtual_env',
        bash_command='source /home/luxds/katumo/cbk_daily/venv/bin/activate',
    )

    execute_file = BashOperator(
    task_id='execute_python_file',
    bash_command='python /home/luxds/kithinji/crypto_price/app_data.py',
        )

    # Execute the tasks in the correct order
    activate_venv >> execute_file

