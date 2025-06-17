# Databricks notebook source
# MAGIC %md
# MAGIC # Kinesis Stream Consumer with PySpark
# MAGIC 
# MAGIC This notebook demonstrates how to consume data from Amazon Kinesis Stream using PySpark Structured Streaming.

# COMMAND ----------

# MAGIC %pip install boto3 python-dotenv

# COMMAND ----------

import os
from dotenv import load_dotenv
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Load environment variables
load_dotenv()

# Initialize Spark Session with AWS configurations
spark = SparkSession.builder \
    .appName("KinesisStreamConsumer") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
    .getOrCreate()

# Set up AWS credentials for boto3
boto3.setup_default_session(
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

# Load environment variables
load_dotenv()

# Define the schema for the incoming data
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# COMMAND ----------

# Get AWS configuration from environment
aws_region = os.getenv('AWS_REGION')
if not aws_region:
    raise ValueError("AWS_REGION environment variable is not set")

# Configure AWS endpoints and regions
spark.conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
spark.conf.set("spark.kinesis.client.region", aws_region)

# Additional Kinesis-specific configurations
spark.conf.set("spark.kinesis.client.maxRecords", "1000")
spark.conf.set("spark.kinesis.client.numRetries", "3")
spark.conf.set("spark.kinesis.client.retryIntervalMs", "1000")

# COMMAND ----------

# Read from Kinesis Stream
kinesis_stream_name = os.getenv('KINESIS_STREAM_NAME')
if not kinesis_stream_name:
    raise ValueError("KINESIS_STREAM_NAME environment variable is not set")

kinesis_stream = spark.readStream \
    .format("kinesis") \
    .option("streamName", kinesis_stream_name) \
    .option("initialPosition", "earliest") \
    .option("region", aws_region) \
    .option("awsAccessKeyId", os.getenv('AWS_ACCESS_KEY_ID')) \
    .option("awsSecretKey", os.getenv('AWS_SECRET_ACCESS_KEY')) \
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
delta_query = windowed_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kinesis_checkpoint") \
    .table("sensor_metrics")

# COMMAND ----------

# Display raw sensor data in console
raw_console_query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

# Display windowed aggregations in console
agg_console_query = windowed_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

# Wait for the queries to terminate
raw_console_query.awaitTermination()
agg_console_query.awaitTermination()
delta_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the Results
# MAGIC 
# MAGIC You can query the Delta table to see the processed results:
# MAGIC ```sql
# MAGIC SELECT * FROM sensor_metrics ORDER BY window DESC LIMIT 10;
# MAGIC ```
