
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType, DoubleType
from pyspark.sql.functions import col, lower, when
from transformers import pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType
from dotenv import load_dotenv
from datetime import datetime
import configparser
import json
import logging

# # Load environment variables
# os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
# os.environ["PYSPARK_PYTHON"] = "/home/pce/anaconda3/envs/spark_env/bin/python3.11"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/pce/anaconda3/envs/spark_env/bin/python3.11"
# load_dotenv()


logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

# Get the path to the parent parent directory
config_dir = os.path.abspath(os.path.join(os.getcwd()))

# Specify the path to config file
config_file_path = os.path.join(config_dir, "config.ini")
config = configparser.ConfigParser()
config.read(config_file_path)

config_file_path_json = os.path.join(config_dir, "config.json")
with open(config_file_path_json) as f:
    config_json = json.load(f)

gcs_config = config["GCS"]["credentials_path"]
raw_bucket_name = config["GCS"]["raw_bucket_name"]
formatted_bucket_name = config["GCS"]["formatted_bucket_name"]


def create_spark_session():
    spark = SparkSession.builder \
        .appName("Sentiment Analysis with Roberta") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "15g") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_config) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  # Set log level to ERROR
    return spark

schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("business_name", StringType(), True),
        StructField("time", DoubleType(), True),
        StructField("rating", LongType(), True),
        StructField("text", StringType(), True),
        StructField("verified", StringType(), True),
        StructField("date", LongType(), True)
    ])


def preprocess_data(spark, input_path):
    reviews_df = spark.read.schema(schema).parquet(input_path)
    reviews_df = reviews_df.withColumn("review", lower(col("text")))
    reviews_df = reviews_df.withColumn("Sentiment", when(col("rating") <= 3, 0).otherwise(1))
    return reviews_df

def analyze_sentiment(text):
    result = sentiment_pipeline(text)
    label = result[0]['label']
    score = result[0]['score']
    return label, score

if __name__ == "__main__":
    spark = create_spark_session()
    bussiness_df = preprocess_data(spark, f"gs://{raw_bucket_name}/business_reviews_*")

    # Load the sentiment-analysis pipeline
    sentiment_pipeline = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest", tokenizer="cardiffnlp/twitter-roberta-base-sentiment-latest")

    # Register the function as a UDF
    analyze_sentiment_udf = udf(lambda text: analyze_sentiment(text)[0], StringType())
    analyze_score_udf = udf(lambda text: analyze_sentiment(text)[1], FloatType())
    
  
    # Drop the 'text' column before saving
    bussiness_df = bussiness_df.drop("text")
    
    # Apply the UDF to the DataFrame
    bussiness_df = bussiness_df.withColumn("sentiment_label", analyze_sentiment_udf(col("review")))
    bussiness_df = bussiness_df.withColumn("sentiment_score", analyze_score_udf(col("review")))

    # Save the results to a Parquet file
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = f"gs://{formatted_bucket_name}/business_sentiment_{timestamp}.parquet"

    bussiness_df.write.mode("overwrite").parquet(output_path)
    bussiness_df.show()
    
    spark.stop()


