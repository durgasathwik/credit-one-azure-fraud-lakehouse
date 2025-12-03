import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


def build_spark():
    """
    Build a local SparkSession with Kafka support via the spark-sql-kafka-0-10 package.
    """
    spark = (
        SparkSession.builder
        .appName("FraudStreamFromKafkaConsole")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    # Make paths relative to project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(project_root)

    spark = build_spark()

    # Schema must match what transaction_generator produces
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("country", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("event_time_utc", StringType(), True),
        StructField("is_high_risk", IntegerType(), True),
        StructField("risk_score", DoubleType(), True),
    ])

    print("Starting streaming read from Kafka topic 'transactions'...")

    # 1) Read from Kafka as a streaming DataFrame
    # Spark docs pattern: readStream.format('kafka').option(...).load() 
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:19092")
        .option("subscribe", "transactions")
        .option("startingOffsets", "earliest")  # or "latest"
        .load()
    )

    print("Kafka stream schema (raw):")
    kafka_df.printSchema()

    # value is binary -> cast to string, then parse JSON with our schema
    json_df = kafka_df.select(
        from_json(col("value").cast("string"), transaction_schema).alias("data")
    )

    parsed_df = json_df.select("data.*")

    print("Parsed transaction schema:")
    parsed_df.printSchema()

    high_risk_df = parsed_df.filter(col("is_high_risk") == 1)


    # 2) Simple streaming sink: write to console
    query = (
        high_risk_df
        .writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("append")
        .start()
    )

    # Keep it running until Ctrl+C
    query.awaitTermination()


if __name__ == "__main__":
    main()
