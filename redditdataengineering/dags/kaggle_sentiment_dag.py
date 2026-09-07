from datetime import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.kaggle_pipeline import process_sentiment_dataset


with DAG(
    dag_id="kaggle_sentiment_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning", "kaggle", "sentiment"],
) as dag:
    transform = PythonOperator(
        task_id="transform_sentiment_dataset",
        python_callable=process_sentiment_dataset,
    )

    upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_s3_pipeline,
    )

    transform >> upload
