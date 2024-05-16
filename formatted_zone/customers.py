from pyspark.sql import SparkSession
import logging 
import os 
import configparser
import json
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession


# Configure logging
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




if __name__ == "__main__":
    gcs_config = config["GCS"]["credentials_path"]
    raw_bucket_name = config["GCS"]["raw_bucket_name"]
    formatted_bucket_name = config["GCS"]["formatted_bucket_name"]

    spark = SparkSession.builder \
    .appName("GCS Files Read") \
    .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("google.cloud.auth.service.account.json.keyfile", gcs_config) \
    .getOrCreate()

    # Read the Parquet file into a DataFrame from GCS Raw Bucket
    customers_df = spark.read.parquet('gs://'+raw_bucket_name+'/customers*.parquet')

    logger.info('-----------------------------------------------------')
    logger.info("Cleaning data for customers")

    # Drop duplicates if present
    customers_df = customers_df.dropDuplicates()

    # Dump customers file to formatted_zone
    # customers_df.write.mode('overwrite').parquet("./data/formatted_zone/customers")
    customers_df.write.mode('overwrite').parquet(f'gs://{formatted_bucket_name}/customers')