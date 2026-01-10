#!/bin/bash
set -euo pipefail

RUN_DATE="${1:-$(date +%F)}"

export PATH=$PATH:/opt/hadoop/bin

LANDING_FILE="/opt/spark-apps/youtube-video-stats/landing-input/youtube_data_${RUN_DATE}.csv"

HDFS_INPUT_DIR="/youtube_etl/input"
HDFS_OUTPUT_DIR="/youtube_etl/output/youtube_staged_${RUN_DATE}"

FINAL_DIR="/opt/spark-apps/youtube-video-stats/staging_files"
FINAL_CSV="${FINAL_DIR}/youtube-staged.csv"

echo "RUN_DATE=${RUN_DATE}"
echo "LANDING_FILE=${LANDING_FILE}"

echo "1) Upload to HDFS"

hdfs dfs -mkdir -p "${HDFS_INPUT_DIR}"
hdfs dfs -rm -f "${HDFS_INPUT_DIR}/youtube_data_${RUN_DATE}.csv" || true
hdfs dfs -put -f "${LANDING_FILE}" "${HDFS_INPUT_DIR}/"

echo "2) Running Spark Job"
spark-submit --master spark://spark-master:7077 /opt/spark-apps/youtube-video-stats/youtube_etl_spark_job.py "${RUN_DATE}"

echo "3) Export merged CSV"
mkdir -p "${FINAL_DIR}"
rm -f "${FINAL_CSV}" || true

TMP_CSV="/tmp/youtube-staged-${RUN_DATE}.csv"
rm -f "${TMP_CSV}" || true

hdfs dfs -getmerge "${HDFS_OUTPUT_DIR}/part-*.csv" "${TMP_CSV}"

#fail if empty
test -s "${TMP_CSV}" || { echo "ERROR: merged file is empty: ${TMP_CSV}"; exit 2; }

cat "${TMP_CSV}" > "${FINAL_CSV}"
rm -f "${TMP_CSV}"

echo "DONE -> ${FINAL_CSV}"
ls -lh "${FINAL_CSV}"
head -n 10 "${FINAL_CSV}"