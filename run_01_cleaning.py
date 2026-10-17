"""
Step 1: Load raw SPARCS hospital discharge data from HDFS, explore data
quality, run the cleaning pipeline, and save the cleaned data back to HDFS.

Run with:
    python run_01_cleaning.py
"""

from pyspark.sql import SparkSession
from src.data_cleaning import load_raw_data, clean_pipeline

HDFS_PATH = "hdfs://localhost:9000/healthcare_project/raw/hospital_discharge.csv"
HDFS_CLEAN_PATH = "hdfs://localhost:9000/healthcare_project/clean/hospital_discharge_clean"


def main():
    spark = (
        SparkSession.builder
        .appName("HospitalDischargeCleaning")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    print("\n=== Loading raw data from HDFS ===")
    raw_df = load_raw_data(spark, HDFS_PATH)
    print("Row count:", raw_df.count())
    raw_df.printSchema()
    raw_df.show(5)

    print("\n=== Running cleaning pipeline ===")
    clean_df = clean_pipeline(spark, HDFS_PATH)
    print("Row count after cleaning:", clean_df.count())
    clean_df.show(5)

    print("\n=== Saving cleaned data to HDFS ===")
    clean_df.write.mode("overwrite").option("header", True).csv(HDFS_CLEAN_PATH)
    print(f"Clean data written to {HDFS_CLEAN_PATH}")

    spark.stop()
    print("\nDone. Run run_02_analysis.py next.")


if __name__ == "__main__":
    main()
