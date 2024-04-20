from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
import logging 
import os 
import configparser
from pyspark.sql.functions import explode, expr
import json
from pyspark.sql.functions import udf, array
from pyspark.sql import functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col,lit, lower,regexp_extract,trim,when, initcap,to_date, date_format, datediff
from pyspark.sql.functions import split, coalesce
from pyspark.sql.types import ArrayType, StringType
import re

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


if __name__ == "__main__":
    gcs_config = config["GCS"]["credentials_path"]
    raw_bucket_name = config["GCS"]["raw_bucket_name"]

    spark = SparkSession.builder \
    .appName("Read Parquet File") \
    .getOrCreate()

    # Specify the path to the Parquet file
    parquet_file_path = "./data/parquet/flipkart.parquet"

    # Read the Parquet file into a DataFrame
    flipkart_df = spark.read.parquet(parquet_file_path)

    # Drop duplicates
    flipkart_df = flipkart_df.dropDuplicates()

    # Convert date format 23 Feb 2025 to 2025-02-23
    flipkart_df = flipkart_df.withColumn("expiry_date", to_date("expiry_date", "d MMM yyyy"))\
       .withColumn("expiry_date", date_format("expiry_date", "yyyy-MM-dd"))
    
    flipkart_df = flipkart_df.withColumn("manufacturing_date", to_date("manufacturing_date", "d MMM yyyy"))\
       .withColumn("manufacturing_date", date_format("manufacturing_date", "yyyy-MM-dd"))
    
    flipkart_df = flipkart_df.withColumn("avg_expiry_date_days", datediff("expiry_date","manufacturing_date"))
    # flipkart_df.write.json("./data/cleaned/flipkart.json")

    product_avg_expiry_date = flipkart_df.groupBy("name").agg(
        F.min("avg_expiry_date_days").alias("min_avg_expiry_days")
    )

    # distinct_values = flipkart_df.select("expiry_date","expiry_date_formatted").distinct().collect()
    # print("Distinct values in item_description column:")
    # for row in distinct_values:
    #     print(row["expiry_date"],row["expiry_date_formatted"])

    product_avg_expiry_date.show()