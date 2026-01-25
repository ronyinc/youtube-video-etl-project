from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

import os
import pandas as pd
from sqlalchemy import create_engine, text

CSV_PATH = "/opt/spark-apps/youtube-video-stats/staging_files/youtube-staged.csv"
TARGET_SCHEMA = "staging"
TARGET_TABLE = "youtube_raw"

def ensure_table_exists():
    """Create the schema/table if they don't exist."""
    c = BaseHook.get_connection("pg_warehouse")
    engine = create_engine(c.get_uri())  # <- safe: uses URL-encoded URI from Airflow

    ddl = """
    CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION app_user;

    CREATE TABLE IF NOT EXISTS staging.youtube_raw (
      video_id        TEXT PRIMARY KEY,
      title           TEXT,
      published_at     TIMESTAMPTZ,
      duration        TEXT,
      duration_total_seconds    INTEGER,
      view_count       BIGINT,
      like_count       BIGINT,
      comment_count    BIGINT,
      ingested_at     TIMESTAMPTZ DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

def load_csv_to_staging():
    """Append the CSV file as-is into staging.youtube_raw."""
    # quick sanity to surface path issues nicely
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}. Is the folder mounted?")

    c = BaseHook.get_connection("pg_warehouse")
    engine = create_engine(c.get_uri())  # <- safe

    df = pd.read_csv(CSV_PATH)

    df = df.rename(columns={
    "publishedAt": "published_at",
    "viewCount": "view_count",
    "likeCount": "like_count",
    "commentCount": "comment_count"})

    df.to_sql(
        name=TARGET_TABLE,
        schema=TARGET_SCHEMA,
        con=engine,
        if_exists="append",
        index=False
    )

with DAG(
    dag_id="youtube_api_load_csv_to_staging",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=["staging","csv","postgres"],
) as dag:
    t1 = PythonOperator(task_id="ensure_table", python_callable=ensure_table_exists)
    t2 = PythonOperator(task_id="load_csv", python_callable=load_csv_to_staging)
    t1 >> t2