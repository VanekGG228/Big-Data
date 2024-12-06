from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import os
import pandas as pd


DATA_FILE_PATH = "/home/gefest111/Big-data/bdt7/tiktok_google_play_reviews.csv"
LOG_FILE_PATH = "/home/gefest111/Big-data/bdt7/log_file.txt"


def check_file_is_empty() -> str:
    if os.stat(DATA_FILE_PATH).st_size == 0:
        return "log_empty_file"
    else:
        return "process_data_group.replace_nulls"



def replace_nulls():
    df = pd.read_csv(DATA_FILE_PATH)
    df.fillna("-", inplace=True)
    df.to_csv(DATA_FILE_PATH, index=False)


def sort_by_date():
    df = pd.read_csv(DATA_FILE_PATH)
    if "created_date" in df.columns:
        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
        df = df.sort_values(by="created_date")
    df.to_csv(DATA_FILE_PATH, index=False)


def clean_content():
    import re
    df = pd.read_csv(DATA_FILE_PATH)
    if "content" in df.columns:
        df["content"] = df["content"].apply(
            lambda x: re.sub(r"[^\w\s,.!?-]", "", x) if isinstance(x, str) else x
        )
    df.to_csv(DATA_FILE_PATH, index=False)


with DAG(
    dag_id="data_processing_dag",
    schedule_interval=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=DATA_FILE_PATH,
        poke_interval=10,
        timeout=600,
    )

    task_branch = BranchPythonOperator(
        task_id="task_branch",
        python_callable=check_file_is_empty,
    )

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command=f'echo "File is empty" >> {LOG_FILE_PATH}',
    )

    with TaskGroup("process_data_group") as process_data_group:
        replace_nulls_task = PythonOperator(
            task_id="replace_nulls",
            python_callable=replace_nulls,
        )
        sort_by_date_task = PythonOperator(
            task_id="sort_by_date",
            python_callable=sort_by_date,
        )
        clean_content_task = PythonOperator(
            task_id="clean_content",
            python_callable=clean_content,
        )

        replace_nulls_task >> sort_by_date_task >> clean_content_task

    wait_for_file >> task_branch
    task_branch >> log_empty_file
    task_branch >> process_data_group
