import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

def main():
    # 1) Build a local SparkSession
    spark = (
        SparkSession.builder
        .appName("FraudLakehouseBronzeIngest")
        .master("local[*]")  # use all local cores
        .getOrCreate()
    )

    # Be a bit quieter
    spark.sparkContext.setLogLevel("WARN")

    # 2) Define input & output paths
    raw_path = "data/raw"
    bronze_path = "data/bronze/transactions"

    print(f"Reading raw CSV files from: {raw_path}")

    # 3) Read ALL CSV files from data/raw
    # Official docs: spark.read.csv(..., header=True, inferSchema=True) 
    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(raw_path)
    )

    print("=== Raw DataFrame Schema ===")
    df_raw.printSchema()

    print("=== Sample rows from raw DataFrame ===")
    df_raw.show(10, truncate=False)

    # 4) Basic "bronze" cleanup:
    # - ensure event_time_utc is a proper timestamp
    # - standardize column names if needed (ours are already snake_case)
    df_bronze = (
        df_raw
        .withColumn("event_time_utc_ts", to_timestamp(col("event_time_utc")))
        .dropna(subset=["transaction_id", "card_id", "amount", "event_time_utc_ts"])
    )

    print("=== Bronze DataFrame Schema ===")
    df_bronze.printSchema()

    print("=== Sample rows from bronze DataFrame ===")
    df_bronze.show(10, truncate=False)

    # 5) Write out as Parquet to bronze path
    # By default Spark writes partitioned folder structure into this directory.
    print(f"Writing bronze Parquet to: {bronze_path}")
    (
        df_bronze
        .write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    print("Bronze write complete.")

    # 6) Stop SparkSession cleanly
    spark.stop()


if __name__ == "__main__":
    # Ensure paths are relative to project root even if script run from elsewhere
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(project_root)
    main()
