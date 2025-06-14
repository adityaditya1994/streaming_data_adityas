# Databricks notebook source
# MAGIC %md
# MAGIC # Kinesis Stream Consumer with PySpark
# MAGIC 
# MAGIC This notebook demonstrates how to consume data from Amazon Kinesis Stream using PySpark Structured Streaming.

# COMMAND ----------

# MAGIC %pip install boto3 python-dotenv

# COMMAND ----------

import os
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define the schema for the incoming data
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# COMMAND ----------

# Configure AWS credentials
spark.conf.set("spark.hadoop.fs.s3a.access.key", dbutils.secrets.get(scope="aws", key="access_key"))
spark.conf.set("spark.hadoop.fs.s3a.secret.key", dbutils.secrets.get(scope="aws", key="secret_key"))
spark.conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{os.getenv('AWS_REGION')}.amazonaws.com")

# COMMAND ----------

# Read from Kinesis Stream
kinesis_stream = spark.readStream \
    .format("kinesis") \
    .option("streamName", os.getenv("KINESIS_STREAM_NAME")) \
    .option("initialPosition", "earliest") \
    .option("region", os.getenv("AWS_REGION")) \
    .load()

# Parse the JSON data
parsed_stream = kinesis_stream.select(
    from_json(col("data").cast("string"), schema).alias("parsed_data")
).select("parsed_data.*")

# COMMAND ----------

# Add window aggregations
windowed_stream = parsed_stream \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window("timestamp", "5 minutes"),
        "device_id"
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
        avg("pressure").alias("avg_pressure"),
        count("*").alias("record_count")
    )

# COMMAND ----------

# Write the stream to Delta table
query = windowed_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kinesis_checkpoint") \
    .table("sensor_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the Results
# MAGIC 
# MAGIC You can query the Delta table to see the processed results:
# MAGIC ```sql
# MAGIC SELECT * FROM sensor_metrics ORDER BY window DESC LIMIT 10;
# MAGIC ```
