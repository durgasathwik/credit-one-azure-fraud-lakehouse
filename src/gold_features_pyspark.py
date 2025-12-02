import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as F_sum,
    avg as F_avg,
    max as F_max,
    count as F_count,
    when,
    round as F_round,
)

def main():
    spark = (
        SparkSession.builder
        .appName("FraudLakehouseGoldFeatures")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # 1) Paths
    silver_path = "data/silver/transactions_enriched"
    gold_path = "data/gold/card_daily_features"

    print(f"Reading silver Parquet from: {silver_path}")

    # 2) Read silver layer
    df_silver = spark.read.parquet(silver_path)

    print("=== Silver Schema ===")
    df_silver.printSchema()
    df_silver.show(10, truncate=False)

    # We expect at least:
    # card_id, amount, is_high_risk, risk_score, transaction_date, transaction_hour, risk_level, ...

    # 3) Build daily per-card aggregates (Gold features)
    # This uses groupBy + agg as in the PySpark docs: groupBy(...).agg(...) 
    df_features = (
        df_silver
        .groupBy("card_id", "transaction_date")
        .agg(
            F_count("*").alias("tx_count"),
            F_sum("amount").alias("total_amount"),
            F_max("amount").alias("max_amount"),
            F_avg("amount").alias("avg_amount"),
            F_avg("risk_score").alias("avg_risk_score"),
            F_sum(
                when(col("is_high_risk") == 1, 1).otherwise(0)
            ).alias("high_risk_tx_count"),
        )
    )

    # 4) Derived ratios / flags on top of aggregates
    df_features = (
        df_features
        .withColumn(
            "high_risk_ratio",
            F_round(col("high_risk_tx_count") / col("tx_count"), 3)
        )
        .withColumn(
            "is_card_high_risk_day",
            when(col("high_risk_ratio") >= 0.3, 1).otherwise(0)
        )
    )

    print("=== Gold Features Schema ===")
    df_features.printSchema()

    print("=== Sample Gold Features Rows ===")
    df_features.show(20, truncate=False)

    # 5) Write Gold features as Parquet, partitioned by date
    print(f"Writing gold features to: {gold_path}")
    (
        df_features
        .write
        .mode("overwrite")
        .partitionBy("transaction_date")
        .parquet(gold_path)
    )

    print("Gold write complete.")

    spark.stop()


if __name__ == "__main__":
    # Ensure paths relative to project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(project_root)
    main()
