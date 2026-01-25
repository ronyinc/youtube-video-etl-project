from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

import os
import pandas as pd
from sqlalchemy import create_engine, text, types as sqltypes

CSV_PATH = "/opt/spark-apps/youtube-video-stats/staging_files/youtube-staged.csv"
TARGET_SCHEMA = "staging"
TARGET_TABLE = "youtube_raw"
TMP_TABLE = "youtube_raw_tmp"
CONN_ID = "pg_warehouse"

def get_engine():
    c = BaseHook.get_connection(CONN_ID)
    return create_engine(c.get_uri())

def load_csv_to_tmp():
    """Load latest CSV snapshot into a temp table"""
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"The file not found in the location {CSV_PATH}")
    
    engine = get_engine()
    df = pd.read_csv(CSV_PATH)

    df = df.rename(columns={
        "publishedAt": "published_at",
        "viewCount": "view_count",
        "likeCount": "like_count",
        "commentCount": "comment_count",
        })
    expected_cols = ["video_id", "title", "published_at", "duration",
        "duration_total_seconds", "view_count", "like_count", "comment_count"]
    
    df = df[expected_cols]

    dtype_map = {
    "video_id": sqltypes.TEXT(),
    "title": sqltypes.TEXT(),
    "published_at": sqltypes.TIMESTAMP(timezone=True),  # TIMESTAMPTZ
    "duration": sqltypes.TEXT(),
    "duration_total_seconds": sqltypes.INTEGER(),
    "view_count": sqltypes.BIGINT(),
    "like_count": sqltypes.BIGINT(),
    "comment_count": sqltypes.BIGINT(),
    }

    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)

    df.to_sql(
        name=TMP_TABLE,
        schema=TARGET_SCHEMA,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
        dtype=dtype_map,
    )

def delete_bad_tmp_rows():
    engine = get_engine()
    sql = f"""
    DELETE FROM {TARGET_SCHEMA}.{TMP_TABLE}
    WHERE video_id IS NULL
       OR TRIM(video_id) = ''
       OR video_id = '#NAME?';
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def update_changed_rows():

    """
    Step 1: Update only rows where at least one tracked column changed.
    Uses IS DISTINCT FROM for NULL-safe comparisons.
    """

    engine = get_engine()

    update_sql = f"""
    UPDATE {TARGET_SCHEMA}.{TARGET_TABLE} trg
    SET 
        title = tmp.title,
        published_at = tmp.published_at,
        duration = tmp.duration,
        duration_total_seconds = tmp.duration_total_seconds,
        view_count = tmp.view_count,
        like_count = tmp.like_count,
        comment_count = tmp.comment_count,
        ingested_at = NOW()

    FROM {TARGET_SCHEMA}.{TMP_TABLE} tmp
    WHERE trg.video_id = tmp.video_id
    AND  (
            trg.title is distinct from tmp.title or 
            trg.published_at is distinct from tmp.published_at or
            trg.duration is distinct from tmp.duration or 
            trg.duration_total_seconds is distinct from tmp.duration_total_seconds or 
            trg.view_count is distinct from tmp.view_count or 
            trg.like_count is distinct from tmp.like_count or 
            trg.comment_count is distinct from tmp.comment_count
    ); 
"""
    with engine.begin() as conn:
        conn.execute(text(update_sql))

def insert_new_rows():
    """
    insert all the new video ids
    """

    engine = get_engine()

    insert_sql = f"""

    INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE} (
      video_id, title, published_at, duration, duration_total_seconds,
      view_count, like_count, comment_count
    )

    SELECT 
            tmp.video_id, tmp.title, tmp.published_at, tmp.duration, tmp.duration_total_seconds,
            tmp.view_count, tmp.like_count, tmp.comment_count
    FROM
          {TARGET_SCHEMA}.{TMP_TABLE} tmp
    LEFT JOIN 
              {TARGET_SCHEMA}.{TARGET_TABLE} trg
              ON trg.video_id = tmp.video_id
    WHERE           
          trg.video_id is NULL
          ON CONFLICT (video_id) DO NOTHING;
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql))    
    

def drop_tmp_table():

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.{TMP_TABLE};"))


with DAG(
    dag_id="youtube_api_db_differential_load",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["youtube","staging", "postgres", "differential", "two-step"],
) as dag:
    t1 = PythonOperator(task_id="load_tmp", python_callable=load_csv_to_tmp)
    t_bad = PythonOperator(task_id="delete_bad_tmp_rows", python_callable=delete_bad_tmp_rows)
    t2 = PythonOperator(task_id="update_changed", python_callable=update_changed_rows)
    t3 = PythonOperator(task_id="insert_new", python_callable=insert_new_rows)
    t4 = PythonOperator(task_id="drop_tmp", python_callable=drop_tmp_table)

    t1 >> t_bad >> t2 >> t3 >> t4