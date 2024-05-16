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
    formatted_bucket_name = config["GCS"]["formatted_bucket_name"]

    # spark = SparkSession.builder \
    #     .appName("Read Parquet File") \
    #     .config("spark.sql.repl.eagerEval.enabled", True) \
    #     .config("spark.sql.execution.pythonUDF.arrow.enabled", "true")\
    #     .getOrCreate()
    spark = SparkSession.builder \
    .appName("GCS Files Read") \
    .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("google.cloud.auth.service.account.json.keyfile", gcs_config) \
    .getOrCreate()
    
    # Read the Parquet file into a DataFrame from GCS Raw Bucket
    # supermarket_loc = "./data/gcs_raw_parquet/establishments_catalonia.parquet"
    # df_supermarket_info = spark.read.parquet(supermarket_loc)
    df_supermarket_info = spark.read.parquet('gs://'+raw_bucket_name+'/establishments_catalonia.*')

    logger.info('-----------------------------------------------------')
    logger.info("Cleaning data for establishments_catalonia")

    # Drop duplicates if present
    df_supermarket_info = df_supermarket_info.dropDuplicates()

    # Select specific columns
    df_supermarket_info = df_supermarket_info.select('id', 'commercial_name', 'county_code', 'full_address', 'UTMx', 'UTMy', 'location')

    # create columns for latitude and longitude
    df_supermarket_info = df_supermarket_info.withColumn('latitude', split(df_supermarket_info['location'], ',').getItem(0).cast('float'))
    df_supermarket_info = df_supermarket_info.withColumn('longitude', split(df_supermarket_info['location'], ',').getItem(1).cast('float'))

    # Drop the original 'location' column
    df_supermarket_info = df_supermarket_info.drop('location')

    # drop Nan values, if any
    df_supermarket_info = df_supermarket_info.na.drop()

    # print(df_supermarket_info)

    # Dump file to formatted_zone
    # df_supermarket_info.write.parquet("./data/formatted_zone/establishments_catalonia")
    df_supermarket_info.write.mode('overwrite').parquet(f'gs://{formatted_bucket_name}/establishments_catalonia')
