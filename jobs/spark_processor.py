from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ==============================
# CONFIG
# ==============================
KAFKA_BROKERS = 'kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092'
SOURCE_TOPIC = 'financial_transactions'
AGGREGATES_TOPIC = 'transaction_aggregates'
ANOMALIES_TOPIC = 'transaction_anomalies'

CHECKPOINT_DIR = '/mnt/spark-checkpoints'
STATES_DIR = '/mnt/spark-state'

# ==============================
# SPARK SESSION
# ==============================
spark = (
    SparkSession.builder
    .appName("FinancialTransactionProcessor")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
    .config("spark.sql.streaming.stateStore.stateStoreDir", STATES_DIR)
    .config("spark.sql.shuffle.partitions", 4)  # 200
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==============================
# SCHEMA
# ==============================
transaction_schema = StructType([
    StructField('transactionId', StringType(), True),
    StructField('userId', StringType(), True),
    StructField('merchantId', StringType(), True),
    StructField('amount', DoubleType(), True),
    StructField('transactionTime', LongType(), True),
    StructField('transactionType', StringType(), True),
    StructField('location', StringType(), True),
    StructField('paymentMethod', StringType(), True),
    StructField('isInternational', StringType(), True),  # will convert
    StructField('currency', StringType(), True),
])

# ==============================
# READ FROM KAFKA
# ==============================
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "latest")  # production-safe
    .load()
)

# ==============================
# PARSE JSON
# ==============================
transactions_df = (
    kafka_stream
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), transaction_schema).alias("data"))
    .select("data.*")
)

# ==============================
# DATA CLEANING
# ==============================

# convert timestamp
transactions_df = transactions_df.withColumn(
    "transactionTimestamp",
    (col("transactionTime") / 1000).cast("timestamp")
)

# convert string -> boolean
transactions_df = transactions_df.withColumn(
    "isInternational",
    col("isInternational") == "True"
)

# ==============================
# AGGREGATION (WINDOWED)
# ==============================
aggregated_df = (
    transactions_df
    .withWatermark("transactionTimestamp", "10 minutes")
    .groupBy(
        window(col("transactionTimestamp"), "5 minutes"),
        col("merchantId")
    )
    .agg(
        sum("amount").alias("totalAmount"),
        count("*").alias("transactionCount")
    )
)

# ==============================
# WRITE AGGREGATES TO KAFKA
# ==============================
aggregates_query = (
    aggregated_df
    .withColumn("key", col("merchantId").cast("string"))
    .withColumn(
        "value",
        to_json(struct(
            col("merchantId"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("totalAmount"),
            col("transactionCount")
        ))
    )
    .selectExpr("key", "value")
    .writeStream
    .format("kafka")
    .outputMode("update")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("topic", AGGREGATES_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/aggregates")
    .trigger(processingTime="10 seconds")
    .start()
)

# ==============================
# ANOMALY DETECTION
# ==============================
anomalies_df = transactions_df.filter(col("amount") > 140000)

# ==============================
# WRITE ANOMALIES TO KAFKA
# ==============================
anomalies_query = (
    anomalies_df
    .withColumn("key", col("transactionId"))
    .withColumn("value", to_json(struct("*")))
    .selectExpr("key", "value")
    .writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("topic", ANOMALIES_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/anomalies")
    .trigger(processingTime="10 seconds")
    .start()
)

# ==============================
# KEEP STREAMS RUNNING
# ==============================
spark.streams.awaitAnyTermination()