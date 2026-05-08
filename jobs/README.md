# jobs/

This folder contains **PySpark Structured Streaming** jobs that read from Kafka, process data, and write to Elasticsearch (or other sinks).

---

## How to Submit a Job

From your host machine:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0 \
  /opt/spark/jobs/your_job.py
```

> All files in this folder are mounted inside the Spark containers at `/opt/spark/jobs/`.

---

## Job Template

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("KafkaSparkArch-Job") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType().add("id", StringType()).add("value", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092") \
    .option("subscribe", "your-topic") \
    .option("startingOffsets", "latest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

query = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/spark-checkpoints/your-job") \
    .start()

query.awaitTermination()
```

---

## Notes

- Kafka bootstrap servers inside Docker: `kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092`
- Checkpoints go in `/mnt/spark-checkpoints/` (mapped to `./mnt/checkpoints/` on host)
- State store goes in `/mnt/spark-state/`
