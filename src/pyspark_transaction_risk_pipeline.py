from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when, count, sum as spark_sum, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

BASE = Path(__file__).resolve().parents[1]
INPUT_PATH = str(BASE / "data" / "input" / "transactions_raw.csv")
BRONZE_PATH = str(BASE / "data" / "output" / "bronze_transactions_parquet")
SILVER_PATH = str(BASE / "data" / "output" / "silver_transactions_scored_parquet")
GOLD_PATH = str(BASE / "data" / "output" / "gold_transaction_risk_summary_parquet")

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("merchant", StringType(), True),
    StructField("location", StringType(), True),
    StructField("payment_method", StringType(), True),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("RealTimeTransactionRiskPlatform")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(INPUT_PATH)
        .withColumn("bronze_loaded_at", current_timestamp())
    )
    bronze_df.write.mode("overwrite").parquet(BRONZE_PATH)

    silver_df = (
        bronze_df
        .filter(col("transaction_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("amount") > 0)
        .dropDuplicates(["transaction_id"])
        .withColumn(
            "risk_score",
            when(col("amount") >= 3000, 90)
            .when(col("amount") >= 1000, 60)
            .otherwise(25),
        )
        .withColumn(
            "risk_band",
            when(col("risk_score") >= 80, "HIGH")
            .when(col("risk_score") >= 50, "MEDIUM")
            .otherwise("LOW"),
        )
        .withColumn("silver_loaded_at", current_timestamp())
    )
    silver_df.write.mode("overwrite").parquet(SILVER_PATH)

    gold_df = (
        silver_df
        .groupBy("risk_band", "payment_method")
        .agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
        )
    )
    gold_df.write.mode("overwrite").parquet(GOLD_PATH)

    print("PySpark transaction risk pipeline completed")
    print(f"Bronze records: {bronze_df.count()}")
    print(f"Silver records: {silver_df.count()}")
    print(f"Gold records: {gold_df.count()}")
    gold_df.show(truncate=False)


if __name__ == "__main__":
    main()
