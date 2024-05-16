import logging 
import os 
import configparser
import json
from pyspark.sql import SparkSession
import uuid
from pyspark.sql.functions import udf, monotonically_increasing_id, col, regexp_replace
from pyspark.sql.types import StringType
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


def generate_uuid():
    return str(uuid.uuid4())

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
    customers_df = spark.read.parquet('gs://'+raw_bucket_name+'/customer_puchase.*')

    # Drop duplicates if present
    customers_df = customers_df.dropDuplicates()

    # Generate id as unique identifier
    # generate_uuid_udf = udf(generate_uuid, StringType())
    customers_df = customers_df.withColumn("id", monotonically_increasing_id())

    logger.info('-----------------------------------------------------')
    logger.info("Cleaning data for customer_purchase")

    # Unit price also contains , value
    customers_df = customers_df.withColumn('unit_price', regexp_replace(col('unit_price'), ',', ''))
    customers_df = customers_df.select("id","customer_id","customer_name", "product_name","unit_price","quantity","purchase_date")
    

    # Dump customers file to formatted_zone
    customers_df.write.mode('overwrite').parquet(f'gs://{formatted_bucket_name}/customer_purchase')