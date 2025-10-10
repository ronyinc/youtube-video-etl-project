from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils_single_csv import write_single_csv
import os, shutil


def main():
    
    
    spark = SparkSession.builder.appName("VideoStatsTransform").master("spark://spark-master:7077").getOrCreate()

    input_path = "/opt/spark-apps/youtube-video-stats/landing-input/youtube_data_2025-10-06.csv"

    df = spark.read.option("header","true").csv(input_path)

    df.printSchema()
    # df.show(5, truncate=False)

    df_pt = df.filter(F.col("duration").rlike(r"^PT"))

    df_pt = (
        df_pt
        .withColumn("hours", F.regexp_extract("duration", r"(\d+)H", 1))
        .withColumn("minutes", F.regexp_extract("duration", r"(\d+)M", 1))
        .withColumn("seconds", F.regexp_extract("duration", r"(\d+)S", 1))
    )

    df_pt = (
    df_pt
    .withColumn("hours",   F.when(F.col("hours")   == "", 0).otherwise(F.col("hours").cast("int")))
    .withColumn("minutes", F.when(F.col("minutes") == "", 0).otherwise(F.col("minutes").cast("int")))
    .withColumn("seconds", F.when(F.col("seconds") == "", 0).otherwise(F.col("seconds").cast("int")))

    )

    df_pt = df_pt.withColumn(
        "total_seconds",
        (F.col("hours")*3600) + (F.col("minutes")*60) + (F.col("seconds"))
    )

    df_pt.show(10, truncate=False)

    final_csv = "./staging_files/youtube-staged.csv"
    write_single_csv(df_pt, final_csv, header=True)

    print(f"wrote: {os.path.abspath(final_csv)}")

    spark.stop()


if __name__ == "__main__":
    main()