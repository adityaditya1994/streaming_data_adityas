import os
import json
import time
import random
import boto3
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class KinesisProducer:
    def __init__(self):
        self.kinesis_client = boto3.client(
            'kinesis',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        self.stream_name = os.getenv('KINESIS_STREAM_NAME')

    def generate_sample_data(self):
        """Generate sample IoT sensor data"""
        return {
            'device_id': f'device_{random.randint(1, 5)}',
            'temperature': round(random.uniform(20.0, 30.0), 2),
            'humidity': round(random.uniform(30.0, 70.0), 2),
            'pressure': round(random.uniform(980.0, 1020.0), 2),
            'timestamp': datetime.now().isoformat()
        }

    def put_record(self, data):
        """Put a single record to Kinesis stream"""
        try:
            response = self.kinesis_client.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(data),
                PartitionKey=str(data['device_id'])
            )
            print(f"Successfully put record to Kinesis stream. Sequence number: {response['SequenceNumber']}")
            return True
        except Exception as e:
            print(f"Error putting record to Kinesis stream: {str(e)}")
            return False

    def run(self, interval=1.0):
        """Run continuous data generation and ingestion"""
        print(f"Starting data production to Kinesis stream: {self.stream_name}")
        print(f"Data will be generated every {interval} seconds")
        
        while True:
            data = self.generate_sample_data()
            success = self.put_record(data)
            if success:
                print(f"Sent data: {data}")
            time.sleep(interval)

if __name__ == "__main__":
    producer = KinesisProducer()
    try:
        producer.run(interval=1.0)
    except KeyboardInterrupt:
        print("\nStopping data production...")
