from flask import Flask, render_template, request
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexerModel
from pyspark.ml.feature import IndexToString
from pymongo import MongoClient

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
mongo_client = MongoClient('mongodb://localhost:27017')
db = mongo_client['recommendations']
collection = db['recommendations']


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
            collection.insert_one({"user_id": user_id, "recommendations": recommendations_list})
            return render_template('recommend.html', user_id=user_id, recommendations=recommendations_list)
        else:
            message = "No recommendations found for the user."
    else:
        message = "User ID not found in the dataset."
    return render_template('error.html', message=message)


if __name__ == '__main__':
    app.run(debug=True)
