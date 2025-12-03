import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

def main():
    # Make sure we're at project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(project_root)

    spark = (
        SparkSession.builder
        .appName("ExploreGoldFeatures")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    gold_path = "data/gold/card_daily_features"
    print(f"Reading gold features from: {gold_path}")

    df_gold = spark.read.parquet(gold_path)

    print("=== Gold Features Schema ===")
    df_gold.printSchema()

    print("=== Sample Gold Features Rows ===")
    df_gold.show(10, truncate=False)

    # 1) Top 10 cards by total_amount across all days
    print("\n=== Top 10 cards by total_amount (across all days) ===")
    df_total_by_card = (
        df_gold.groupBy("card_id")
        .sum("total_amount")
        .withColumnRenamed("sum(total_amount)", "total_amount_sum")
        .orderBy(desc("total_amount_sum"))
    )
    df_total_by_card.show(10, truncate=False)

    # 2) Days with the highest number of high-risk transactions
    print("\n=== Top 10 card_date combos by high_risk_tx_count ===")
    df_high_risk_days = (
        df_gold
        .orderBy(desc("high_risk_tx_count"))
    )
    df_high_risk_days.show(10, truncate=False)

    # 3) Count how many cards are flagged 'high risk day'
    print("\n=== Count of high-risk days vs non-high-risk days ===")
    df_risk_day_counts = (
        df_gold
        .groupBy("is_card_high_risk_day")
        .count()
    )
    df_risk_day_counts.show(truncate=False)

    # 4)Top 5 cards by high_risk_ratio
    print("\n=== Top 5 cards by high_risk_ratio ===")
    df_high_risk_ratio = (
        df_gold
        .groupBy("card_id")
        .avg("high_risk_ratio")
        .withColumnRenamed("avg(high_risk_ratio)", "avg_high_risk_ratio")
        .orderBy(desc("avg_high_risk_ratio"))
    )
    df_high_risk_ratio.show(5, truncate=False)

    # 5)Daily total_amount trend last 7 days
    print("\n=== Daily total_amount trend for last 7 days ===")
    df_daily_trend = (
        df_gold
        .groupBy("transaction_date")
        .sum("total_amount")
        .withColumnRenamed("sum(total_amount)", "daily_total_amount")
        .orderBy(desc("transaction_date"))
        .limit(7)
    )

    spark.stop()


if __name__ == "__main__":
    main()
