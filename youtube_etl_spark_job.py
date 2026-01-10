from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, coalesce, lit
import sys

def main(run_date: str) -> None:
    
    spark = (
        SparkSession.builder
        .appName("YouTubeETL")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )

    hdfs_input = f"hdfs://hdfs-namenode:9000/youtube_etl/input/youtube_data_{run_date}.csv"
    hdfs_output = f"hdfs://hdfs-namenode:9000/youtube_etl/output/youtube_staged_{run_date}"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(hdfs_input)
    )

    #keep the records which start with "PT" (and not just "PT")
    df = df.filter(col("duration").startswith("PT") & (col("duration") != "PT"))

    # Extract H / M / S (optional groups)
    # regexp_extract returns "" when no match; cast("") -> null; then coalesce -> 0

    df = (
           df.withColumn("hours", regexp_extract(col("duration"),   r"(\d+)H", 1))
             .withColumn("minutes", regexp_extract(col("duration"), r"(\d+)M", 1))
             .withColumn("seconds", regexp_extract(col("duration"), r"(\d+)S", 1))
    )

    df = (
            df.withColumn("i_hours",   coalesce(col("hours").cast("int"), lit(0)))
              .withColumn("i_minutes", coalesce(col("minutes").cast("int"), lit(0)))
              .withColumn("i_seconds", coalesce(col("seconds").cast("int"), lit(0)))
              .withColumn(
                    "duration_total_seconds",
                    col("i_hours")*lit(3600) + col("i_minutes")*lit(60) + col("i_seconds")
              )
    )

    selected_cols = [
        "video_id",
        "title",
        "publishedAt",
        "duration",
        "duration_total_seconds",
        "viewCount",
        "likeCount",
        "commentCount",
    ]

    df_out = df.select(*[c for c in selected_cols if c in df.columns])

    # write the file to the hdfs location

    (
        df_out.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(hdfs_output)

    )

    spark.stop()
    print(f"✅ Wrote staged output to: {hdfs_output}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: youtube_etl_spark_job.py <YYYY-MM-DD>")
        sys.exit(1)

    main(sys.argv[1])