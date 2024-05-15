import logging 
import os 
import configparser
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import split


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
        .config("spark.sql.execution.pythonUDF.arrow.enabled", "true")\
        .getOrCreate()
    
    # Read the Parquet file into a DataFrame from GCS Raw Bucket
    location_loc = "./data/gcs_raw_parquet/location.parquet"
    df_location = spark.read.parquet(location_loc)

    logger.info('-----------------------------------------------------')
    logger.info("Cleaning data for location (customers)")

    # Don't drop duplicates as many people can live in same location.
    # Select specific columns
    df_location = df_location.select('location_id', 'postal_code', 'latitude', 'longitude')

    # drop Nan values, if any
    df_location = df_location.na.drop()

    # print(df_location)

    # Dump file to formatted_zone
    df_location.write.parquet("./data/formatted_zone/location")
