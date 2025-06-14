<!-- Use this file to provide workspace-specific custom instructions to Copilot -->

This is a Python project that demonstrates a real-time data processing pipeline using:
1. Amazon Kinesis Streams for data ingestion
2. PySpark for data processing
3. Databricks Community Edition for running the Spark jobs

The project follows these patterns:
- Uses environment variables for configuration
- Implements producer-consumer pattern
- Uses Spark Structured Streaming for real-time processing
- Implements windowed aggregations
- Uses Delta Lake for data storage

Key components:
- Kinesis Producer: Generates and sends sample IoT sensor data
- Spark Consumer: Processes streaming data using windowed operations
- Configuration: Centralized configuration management
