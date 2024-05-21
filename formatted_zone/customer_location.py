import logging 
import os 
import configparser
import json
from datetime import datetime

import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

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
        .appName("RecipeProcessing") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_config) \
        .getOrCreate()

    
    # Read the Parquet file into a DataFrame from GCS Raw Bucket
    # customer_loc = "./data/gcs_raw_parquet/customer_location.parquet"
    # df_customer_loc = spark.read.parquet(customer_loc)
    df_customer_loc = spark.read.parquet('gs://'+raw_bucket_name+'/customer_location*')

    logger.info('-----------------------------------------------------')
    logger.info("Cleaning data for customer_location")

    # Drop duplicates if present
    df_customer_loc = df_customer_loc.dropDuplicates()

    # print(df_customer_loc.show(2, 0))

    # Dump file to formatted_zone
    # df_customer_loc.write.parquet("./data/formatted_zone/customer_location")
    df_customer_loc.write.mode('overwrite').parquet(f'gs://{formatted_bucket_name}/customer_location_'+datetime.now().strftime("%Y%m%d%H%M%S")+'.parquet')
