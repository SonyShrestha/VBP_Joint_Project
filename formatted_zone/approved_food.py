from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging 
import os 
import configparser
from pyspark.sql import functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col,lit, lower,regexp_extract,trim,when, initcap,to_date, date_format, datediff
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

    scrapped_date= '2024-01-01'
    # Specify the path to the Parquet file
    parquet_file_path = "./data/parquet/approved_food.parquet"

    # Read the Parquet file into a DataFrame
    approved_food_df = spark.read.parquet(parquet_file_path)

    # Drop duplicates
    approved_food_df = approved_food_df.dropDuplicates()

    # Convert date format 23 Feb 2025 to 2025-02-23
    approved_food_df = approved_food_df.withColumn("Expiry_Date", to_date("Expiry_Date", "d MMM yyyy"))\
       .withColumn("Expiry_Date", date_format("Expiry_Date", "yyyy-MM-dd"))
    
    approved_food_df = approved_food_df.withColumn("scrapped_date", lit(scrapped_date))
    
    approved_food_df = approved_food_df.withColumnRenamed("Expiry_Date", "expiry_date")\
        .withColumnRenamed("Product_name", "product_name")\
        .withColumnRenamed("Price", "price")\
        .withColumnRenamed("Product_Description", "product_description")

    approved_food_df = approved_food_df.withColumn("avg_expiry_date_days", datediff("expiry_date","scrapped_date"))

    approved_food_df = approved_food_df.filter(col("avg_expiry_date_days")>0)

    # approved_food_df.write.csv("./data/cleaned/approved_food.csv")

    product_avg_expiry_date = approved_food_df.groupBy("product_name").agg(
        F.min("avg_expiry_date_days").alias("min_avg_expiry_days")
    )

    product_avg_expiry_date.show()