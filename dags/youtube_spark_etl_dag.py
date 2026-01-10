from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "youtube_etl_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="youtube_etl_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_etl = BashOperator(
        task_id="run_youtube_spark_etl",
        bash_command="bash /opt/spark-apps/youtube-video-stats/youtube_etl_job_airflow.sh {{ ds }}",
    )

    run_etl