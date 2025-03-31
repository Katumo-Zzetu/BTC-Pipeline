from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# DAG default arguments
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 31),
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

    activate_venv = BashOperator(
        task_id='activate_virtual_env',
        bash_command='source /home/user/project/venv/bin/activate',
    )

    execute_file = BashOperator(
        task_id='execute_python_file',
        bash_command='python /home/user/project/btc_prices.py',
    )

    activate_venv >> execute_file
