import asyncio
import json
import websockets
from kafka import KafkaProducer

# Configuration
KAFKA_TOPIC = "bluesky-posts"
# Use localhost:9092 because we run this script from the host machine
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092" 
BLUESKY_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

# Keywords based on your project requirements (EVs, Tesla, Charging, etc.)
KEYWORDS = [
    "electric car", "ev", "tesla", "byd", "nissan", "bmw", "volkswagen",
    "batterie", "battery", "charge", "charging", "autonomie", "range", 
    "recharge", "subvention", "cost", "environment"
]

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def is_relevant(text):
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in KEYWORDS)

async def listen_to_firehose():
    producer = create_kafka_producer()
    print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    
    print(f"Connecting to Bluesky Jetstream: {BLUESKY_URL}")
    async with websockets.connect(BLUESKY_URL) as websocket:
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                
                # Extract the post text
                # Jetstream structure usually: commit -> record -> text
                if 'commit' in data and 'record' in data['commit']:
                    record = data['commit']['record']
                    text = record.get('text', '')
                    
                    # Filter: Only send if it contains EV keywords
                    if is_relevant(text):
                        # Prepare simplified object for Spark
                        payload = {
                            "did": data.get("did"),
                            "time_us": data.get("time_us"),
                            "text": text,
                            "lang": record.get("langs", ["unknown"])[0] if record.get("langs") else "unknown",
                            "created_at": record.get("createdAt")
                        }
                        
                        producer.send(KAFKA_TOPIC, payload)
                        print(f"Sent to Kafka: {text[:50]}...")
                        
            except websockets.exceptions.ConnectionClosed:
                print("Connection closed, retrying...")
                continue
            except Exception as e:
                print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(listen_to_firehose())