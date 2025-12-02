import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    hour,
    when,
)

def main():
    spark = (
        SparkSession.builder
        .appName("FraudLakehouseSilverTransform")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # 1) Paths
    bronze_path = "data/bronze/transactions"
    silver_path = "data/silver/transactions_enriched"

    print(f"Reading bronze Parquet from: {bronze_path}")

    # 2) Read bronze
    df_bronze = spark.read.parquet(bronze_path)

    print("=== Bronze Schema ===")
    df_bronze.printSchema()
    df_bronze.show(10, truncate=False)

    # 3) Basic data quality filters
    #    - keep only positive amounts
    #    - drop rows with nulls in key fields
    df_clean = (
        df_bronze
        .filter(col("amount") > 0)
        .dropna(subset=["transaction_id", "card_id", "event_time_utc_ts"])
    )

    # 4) Derived columns:
    #    - transaction_date (YYYY-MM-DD)
    #    - transaction_hour (0-23)
    #    - risk_level: LOW / MEDIUM / HIGH
    df_enriched = (
        df_clean
        .withColumn("transaction_date", to_date(col("event_time_utc_ts")))
        .withColumn("transaction_hour", hour(col("event_time_utc_ts")))
        .withColumn(
            "risk_level",
            when(col("risk_score") >= 0.8, "HIGH")
            .when(col("risk_score") >= 0.4, "MEDIUM")
            .otherwise("LOW")
        )
    )

    print("=== Silver (enriched) Schema ===")
    df_enriched.printSchema()
    df_enriched.show(10, truncate=False)

    # 5) Optional: partition by date for better layout
    print(f"Writing silver Parquet to: {silver_path}")
    (
        df_enriched
        .write
        .mode("overwrite")
        .partitionBy("transaction_date")
        .parquet(silver_path)
    )

    print("Silver write complete.")

    spark.stop()


if __name__ == "__main__":
    # Ensure paths relative to project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(project_root)
    main()
