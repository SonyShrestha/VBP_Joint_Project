import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import re
import csv
import json
import uuid
import random
import pyspark.pandas as ps
from datetime import datetime, timedelta
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, split, regexp_extract, when, round, regexp_replace, sum, sequence, to_date, lit, row_number, weekofyear
from pyspark.sql.window import Window
from datetime import datetime, date, timedelta
from pyspark.sql.types import StringType, IntegerType, FloatType, ArrayType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, explode
import pyarrow as pa
import pyarrow.parquet as pq
import findspark
import logging
import configparser
from datetime import datetime, timedelta

root_dir = os.path.abspath(os.path.join(os.getcwd()))

# Specify the path to config file
config_file_path = os.path.join(root_dir, "config.ini")
config = configparser.ConfigParser()
config.read(config_file_path)

config_file_path_json = os.path.join(root_dir, "config.json")
with open(config_file_path_json) as f:
    config_json = json.load(f)

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

# Base directory for files
# raw_data_dir = config.get('COMMON', 'raw_data_dir')
# raw_data_dir= "D:/BDMA/UPC/BDM/P1/VBP_Joint_Project/data/raw"

# gcs cinfiguration
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(root_dir, "gcs_config.json")
# os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-11.0.7"
# os.environ["SPARK_HOME"] = "C:/spark"
# findspark.init()
spark = SparkSession.builder.getOrCreate()

# Function to load data from GCS
def load_data_from_gcs(filepath):
    spark = SparkSession.builder \
        .appName("time series") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22") \
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("google.cloud.auth.service.account.json.keyfile", os.path.join(root_dir, "gcs_config.json")) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    df = spark.read.parquet(filepath)

    return df

def test_gcs():
    gcs_parquet_path = 'gs://spicy_1/bigbasket_products*'
    bigbasket_products_df = load_data_from_gcs(gcs_parquet_path)
    bigbasket_products_df.show()

# Function to generate UUID for product ID
def generate_uuid():
    return str(uuid.uuid4())

def write_parquet_to_gcs(df, filename, zone):
    df.write.mode('overwrite').parquet(f'gs://{zone}/{filename}_'+datetime.now().strftime("%Y%m%d%H%M%S")+'.parquet')

# Generate an increasing number of customers over time with occasional dips
def generate_customer_count_sequence(total_days, total_customers):
    counts = []
    current_count = 0
    for day in range(total_days):
        choice= random.choice(range(10))
        if day % 7 == 0 and day > 0:  # Increase more significantly at the start of each week
            current_count += random.randint(1, 3)
        elif choice==5:
            current_count -= random.randint(15, 20)
        elif choice==3:
            current_count += random.randint(30, 50)
        else:
            current_count += random.randint(0, 1)
        current_count = min(current_count, total_customers)
        if current_count<=0:
            current_count=1
        counts.append(current_count)
    return counts

b2c_cust_counts= [1, 2, 1, 1, 2, 2, 3, 6, 6, 6, 7, 1, 2, 3, 5, 5, 5, 43, 43, 81, 82, 85, 86, 122, 123, 105, 105, 89, 91, 92, 92, 126, 127, 128, 128, 131, 132, 116, 117, 118, 103, 104, 105, 105, 105, 106, 106, 106, 106, 109, 109, 109, 110, 111, 112, 113, 116, 116, 116, 116, 147, 147, 148, 150, 151, 151, 151, 133, 118, 119, 122, 123, 123, 124, 124, 124, 125, 126, 168, 168, 169, 169, 170, 171, 174, 210, 211, 211, 212, 244, 245, 246, 276, 277, 278, 258, 259, 260, 262, 262, 246, 246, 247, 248, 288, 289, 289, 289, 328, 328, 329, 330, 333, 334, 335, 335, 336, 378, 378, 380, 380, 360, 360, 361, 361, 361, 362, 363, 363, 363, 363, 363, 363, 366, 349, 350, 351, 351, 391, 392, 395, 396, 432, 433, 433, 478, 478, 480, 479, 480, 500, 500]

def b2c_generate_daily_data(current_date, num_customers, customer_ids, product_ids):
    random.seed(current_date.toordinal())

    # Ensure num_customers does not exceed the number of available customer IDs
    if num_customers > len(customer_ids):
        raise ValueError("Number of customers exceeds available unique customer IDs")

    customers_today = random.sample(customer_ids, num_customers)
    data = []

    for customer_id in customers_today:
        # Ensure the number of items does not exceed the number of unique products available
        num_items = min(random.randint(1, 10), len(product_ids))
        products_today = random.sample(product_ids, num_items)
        for product in products_today:
            purchase_id = generate_uuid()
            quantity = random.randint(1, 3)
            data.append((purchase_id, current_date, customer_id, product, quantity))

    return data

def get_B2C_time_series():
  # Load the customer and inventory data
  all_customer_parquet= "gs://spicy_1/customers*"
  all_products_parquet= "gs://formatted_zone/all_products*"
  sm_inventory_parquet= "gs://spicy_1/supermarket_products*"

  df_customer = load_data_from_gcs(all_customer_parquet)
  df_customer.show(5)
  df_inventory = load_data_from_gcs(sm_inventory_parquet)
  df_inventory.show(5)
  df_products = load_data_from_gcs(all_products_parquet)
  df_products.show(5)

  date_range_df = spark.sql("SELECT explode(sequence(to_date('2024-01-01'), to_date('2024-05-31'), interval 1 day)) AS date")

  date_range_df.show()

  # Get lists of customer ids, and product ids
  customer_ids = [row['customer_id'] for row in df_customer.select("customer_id").collect()]
  print(len(customer_ids))

  product_ids = [row['product_id'] for row in df_products.select("product_id").collect()]
  print(len(product_ids))

  synthetic_data = []
  # Generate synthetic customer purchase data
  start_date = date(2024, 1, 1)
  end_date = date(2024, 5, 31)
  current_date = start_date
  index = 0
  while current_date <= end_date:
      daily_data = b2c_generate_daily_data(current_date, b2c_cust_counts[index], customer_ids, product_ids)
      synthetic_data.extend(daily_data)
      current_date += timedelta(days=1)
      index += 1
  
  columns = ["purchase_id", "date", "customer_id", "product_id", "quantity"]
  df_b2c_time_series = spark.createDataFrame(synthetic_data, columns)
  print(f"shape of the dataframe= {(df_b2c_time_series.count(), len(df_b2c_time_series.columns))}")
  df_b2c_time_series.show()

  # joining with customer
  df_b2c_cust= df_b2c_time_series.join(df_customer, "customer_id")
  df_b2c_cust.show()

  # Define the window specification
  window_spec = Window.partitionBy("product_id").orderBy("product_name")
  #  remove the quantity column
  df_inventory= df_inventory.drop("quantity")
  # Add row numbers to each row in df2
  df_inventory_with_row_number = df_inventory.withColumn("row_number", row_number().over(window_spec))
  # filter the first supermarket with the desired product_id
  df_inventory_with_row_number= df_inventory_with_row_number.filter(col('row_number')==1)
  # show df
  df_inventory_with_row_number.show()

  df_b2c= df_b2c_cust.join(df_inventory_with_row_number, "product_id")
  df_b2c= df_b2c.withColumnRenamed("product_price", "unit_price")
  df_b2c= df_b2c.drop("row_number")
  print(f"shape of the dataframe= {(df_b2c.count(), len(df_b2c.columns))}")
  df_b2c.show()

  df_b2c= df_b2c.select("date", "purchase_id", "customer_name", "product_id", "product_name", "unit_price", "quantity", "manufacture_date", "expiry_date")
  df_b2c.show()
  write_parquet_to_gcs(df_b2c, "b2c_time_series", "formatted_zone")

if __name__ == "__main__":
    # num_of_customers = int(config["CUSTOMER_PURCHASE"]["num_of_customers"])
    # num_of_purchases = int(config["CUSTOMER_PURCHASE"]["num_of_purchases"])
    # raw_data_dir = config["COMMON"]["raw_data_dir"]
    
    logger.info('-----------------------------------------------------')
    logger.info("Generating B2C time series")
    # get_all_products()
    get_B2C_time_series()
    # generate_customer_purchase(num_of_customers, num_of_purchases, raw_data_dir)