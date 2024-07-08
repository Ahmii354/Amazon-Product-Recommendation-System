from flask import Flask, render_template, request
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.ml.feature import StringIndexerModel
from kafka import KafkaProducer
from pymongo import MongoClient
import json
from pyspark.ml.feature import IndexToString


app = Flask(__name__)
spark = SparkSession.builder \
    .appName("Recommendation System") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/data.reviews") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/data.reviews") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()


sc = spark.sparkContext
df = spark.read.csv("output.csv", header=True)
df = df.select("asin", "reviewerID", "overall")
model = ALSModel.load("als_model.joblib")
asin_indexer_model = StringIndexerModel.load("asin_indexer_model")

# Kafka producer configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'recommendations1'

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
mongo_uri = 'mongodb://localhost:27017'
mongo_db = 'mydatabase'
mongo_collection = 'mycollection'

# Connect to MongoDB
mongo_client = MongoClient(mongo_uri)
db = mongo_client[mongo_db]
collection = db[mongo_collection]

@app.route('/')
def index():
    return render_template('index.html')
    
@app.route('/recommendations', methods=['POST'])
def get_recommendations():
    user_id = request.form['user_id']

    indexed_user_id = StringIndexer(inputCol="reviewerID", outputCol="reviewer_id_indexed").fit(df).transform(df.filter(df.reviewerID == user_id)).first()
    if indexed_user_id:
        indexed_user_id = indexed_user_id["reviewer_id_indexed"]
        all_user_recs = model.recommendForAllUsers(10)
        user_recs = all_user_recs.filter(all_user_recs.reviewer_id_indexed == indexed_user_id).first()
        if user_recs:
            recommendations = user_recs["recommendations"]
            recommendations_df = spark.createDataFrame(recommendations)
            recommendations_df = recommendations_df.select("asin_indexed", "rating")
            asin_converter = IndexToString(inputCol="asin_indexed", outputCol="asin", labels=asin_indexer_model.labels)
            recommendations_df = asin_converter.transform(recommendations_df)
            recommendations_list = recommendations_df.select("asin", "rating").collect()
            for recommendation in recommendations_list:
                producer.send(kafka_topic, recommendation.asDict())
            #return render_template('loading.html')  # Display a loading screen while recommendations are generated
            recommendations = []
            for document in collection.find():
                recommendations.append((document['asin'], document['rating']))
            return render_template('recommendations.html',recommendations=recommendations)
        else:
            message = "No recommendations found for the user."
    else:
        message = "User ID not found in the dataset."
    return render_template('error.html', message=message)

if __name__ == '__main__':
    app.run(debug=True)