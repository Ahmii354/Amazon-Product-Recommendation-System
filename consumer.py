from kafka import KafkaConsumer
import json
from pymongo import MongoClient

# Kafka consumer configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'recommendations1'

# MongoDB configuration
mongo_uri = 'mongodb://localhost:27017'
mongo_db = 'mydatabase'
mongo_collection = 'mycollection'

# Connect to MongoDB
mongo_client = MongoClient(mongo_uri)
db = mongo_client[mongo_db]
collection = db[mongo_collection]

# Create Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Consume messages from Kafka and insert into MongoDB
for message in consumer:
    recommendation = message.value
    asin = recommendation['asin']
    rating = recommendation['rating']

    # Create document to insert into MongoDB
    document = {'asin': asin, 'rating': rating}

    # Insert document into MongoDB collection
    collection.insert_one(document)

    print(f"Inserted Recommendation - ASIN: {asin}, Rating: {rating}")

