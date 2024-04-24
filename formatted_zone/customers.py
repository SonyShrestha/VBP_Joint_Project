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

    spark = SparkSession.builder \
        .appName("Read Parquet File") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .getOrCreate()

    # Read the Parquet file into a DataFrame from GCS Raw Bucket
    customers = "./data/gcs_raw_parquet/customers.parquet"
    customers_df = spark.read.parquet(customers)

    logger.info('-----------------------------------------------------')
    logger.info("Cleaning data for customers")

    # Drop duplicates if present
    customers_df = customers_df.dropDuplicates()

    # Dump customers file to formatted_zone
    customers_df.write.parquet("./data/formatted_zone/customers")