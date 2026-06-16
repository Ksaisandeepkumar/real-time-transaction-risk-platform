from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

BASE = Path(__file__).resolve().parents[1]
INPUT_PATH = str(BASE / "data" / "input" / "transactions_raw.csv")
BRONZE_PATH = str(BASE / "data" / "output" / "bronze_transactions_parquet")
SILVER_PATH = str(BASE / "data" / "output" / "silver_transactions_scored_parquet")
GOLD_PATH = str(BASE / "data" / "output" / "gold_transaction_risk_summary_parquet")


def create_spark_session():
    return (
        SparkSession.builder
        .appName("TransactionRiskProjectSummary")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_safe(spark, path, label, file_type="parquet"):
    try:
        if file_type == "csv":
            df = spark.read.option("header", True).option("inferSchema", True).csv(path)
        else:
            df = spark.read.parquet(path)
        print(f"\n{label} loaded successfully")
        return df
    except Exception as exc:
        print(f"\n{label} not available yet: {exc}")
        return None


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_safe(spark, INPUT_PATH, "Raw transactions CSV", "csv")
    bronze_df = read_safe(spark, BRONZE_PATH, "Bronze transactions")
    silver_df = read_safe(spark, SILVER_PATH, "Silver scored transactions")
    gold_df = read_safe(spark, GOLD_PATH, "Gold transaction risk summary")

    if raw_df is not None:
        print("\n===== RAW TRANSACTION SUMMARY =====")
        print("Raw record count:", raw_df.count())

        print("\nDuplicate transaction_id proof in raw data:")
        raw_df.groupBy("transaction_id").agg(count("*").alias("record_count")) \
            .filter(col("transaction_id").isNotNull()) \
            .filter(col("record_count") > 1) \
            .show(truncate=False)

        print("\nInvalid raw transaction records:")
        raw_df.filter(
            col("transaction_id").isNull()
            | col("customer_id").isNull()
            | (col("amount") <= 0)
        ).show(truncate=False)

    if bronze_df is not None:
        print("\n===== BRONZE SUMMARY =====")
        print("Bronze count:", bronze_df.count())

    if silver_df is not None:
        print("\n===== SILVER SUMMARY =====")
        print("Silver count:", silver_df.count())
        print("Unique transaction_id count:", silver_df.select("transaction_id").distinct().count())

        print("\nDuplicate proof after Silver deduplication:")
        silver_df.groupBy("transaction_id").agg(count("*").alias("record_count")) \
            .filter(col("record_count") > 1) \
            .show(truncate=False)

        print("\nInvalid record proof after Silver validation:")
        silver_df.filter(
            col("transaction_id").isNull()
            | col("customer_id").isNull()
            | (col("amount") <= 0)
        ).show(truncate=False)

        print("\nRisk band distribution:")
        silver_df.groupBy("risk_band").agg(count("*").alias("transaction_count")).show(truncate=False)

    if gold_df is not None:
        print("\n===== GOLD SUMMARY =====")
        print("Gold count:", gold_df.count())
        print("\nSample Gold output:")
        gold_df.show(20, truncate=False)


if __name__ == "__main__":
    main()
