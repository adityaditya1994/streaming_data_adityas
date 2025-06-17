# Kinesis Streaming Pipeline with PySpark and Databricks

This project demonstrates a real-time data processing pipeline using Amazon Kinesis Streams and PySpark on Databricks Community Edition.

## Project Structure

```
├── src/
│   ├── producer/
│   │   └── kinesis_producer.py
│   ├── consumer/
│   │   └── spark_consumer.py
│   └── utils/
│       └── config.py
├── .env.example
├── requirements.txt
└── README.md
```
<img width="955" alt="image" src="https://github.com/user-attachments/assets/8a562388-e79e-4a71-8cee-7a29cdd0fce4" />
<img width="955" alt="image" src="https://github.com/user-attachments/assets/8a562388-e79e-4a71-8cee-7a29cdd0fce4" />


## Prerequisites

1. Python 3.8+
2. AWS Account with Kinesis access
3. Databricks Community Edition account
4. Required Python packages (see requirements.txt)

## Setup

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Copy `.env.example` to `.env` and fill in your credentials:
   ```bash
   cp .env.example .env
   ```

## Usage

1. Start the Kinesis producer:
   ```bash
   python src/producer/kinesis_producer.py
   ```

2. Run the Spark consumer on Databricks:
   - Upload the consumer code to Databricks
   - Configure the Databricks notebook with your AWS credentials
   - Run the notebook to start consuming and processing data

## Components

### Kinesis Producer
- Generates sample data and sends it to Kinesis Stream
- Configurable data generation rate
- Supports multiple data formats

### Spark Consumer
- Reads data from Kinesis Stream using Spark Structured Streaming
- Processes data in real-time
- Supports windowed operations and aggregations
- Writes processed data to Delta tables

## Configuration

Update the `.env` file with your credentials:
- AWS credentials for Kinesis access
- Databricks host and access token
- Kinesis stream configuration

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is licensed under the MIT License - see the LICENSE file for details.
