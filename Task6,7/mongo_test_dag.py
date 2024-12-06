from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime
import pandas as pd


FINAL_DATASET_PATH = "/home/gefest111/Big-data/bdt7/tiktok_google_play_reviews.csv"


def load_to_mongo():
    df = pd.read_csv(FINAL_DATASET_PATH)
    mongo_hook = MongoHook(conn_id="mongo_default")
    collection = mongo_hook.get_collection("processed_reviews")
    data = df.to_dict("records")
    collection.insert_many(data)


with DAG(
    dag_id="load_to_mongo_dag",
    schedule=[Dataset(FINAL_DATASET_PATH)],
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    load_data_task = PythonOperator(
        task_id="load_to_mongo",
        python_callable=load_to_mongo,
    )
