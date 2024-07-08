from pymongo import MongoClient

# Connect to MongoDB
mongo_client = MongoClient('mongodb://localhost:27017')
db = mongo_client['recommendations']
collection = db['recommendations']

# Retrieve the inserted data
data = collection.find()

# Display the data
for doc in data:
    print(doc)
