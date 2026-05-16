import os
import json
import time
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaConsumer
from azure.storage.blob import BlobServiceClient

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Kafka Config
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')
EVENT_HUB_CONNECTION_STRING = os.getenv('EVENT_HUB_CONNECTION_STRING')
EVENT_HUB_NAME = os.getenv('EVENT_HUB_NAME')

# Azure Storage Config
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_CONTAINER_NAME = os.getenv('AZURE_CONTAINER_NAME', 'bronze')

# Batch settings
BATCH_SIZE = 100  
BATCH_TIMEOUT = 60  

# Initialize Azure Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    EVENT_HUB_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='$ConnectionString',
    sasl_plain_password=EVENT_HUB_CONNECTION_STRING,
    group_id='bronze_consumer_group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def upload_batch_to_azure(batch_data):
    if not blob_service_client or not batch_data:
        return

    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"bronze/orders_{timestamp_str}_{uuid.uuid4().hex[:8]}.jsonl"
    json_lines = "\n".join([json.dumps(record) for record in batch_data])
    
    blob_client = container_client.get_blob_client(file_name)
    blob_client.upload_blob(json_lines, overwrite=True)
    print(f"Uploaded {len(batch_data)} records to {AZURE_CONTAINER_NAME}/{file_name}")

if __name__ == "__main__":
    print("Starting consumer to bronze...")
    batch = []
    last_upload_time = time.time()
    
    try:
        for message in consumer:
            batch.append(message.value)
            
            current_time = time.time()
            if len(batch) >= BATCH_SIZE or (current_time - last_upload_time) >= BATCH_TIMEOUT:
                if batch:
                    upload_batch_to_azure(batch)
                    batch = []
                    last_upload_time = current_time
                    
    except KeyboardInterrupt:
        if batch:
            upload_batch_to_azure(batch)
    finally:
        consumer.close()