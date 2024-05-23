import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import re
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

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

# Get the path to the parent parent directory
config_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir, os.pardir, os.pardir))

# Specify the path to config file
config_file_path = os.path.join(config_dir, "config.ini")

config = configparser.ConfigParser()
config.read(config_file_path)

# gcs cinfiguration
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "D:/BDMA/UPC/BDM/P1/VBP_Joint_Project/gcs_config.json"
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-11.0.7"
os.environ["SPARK_HOME"] = "C:/spark"
# findspark.init()
spark = SparkSession.builder.getOrCreate()

# Function to load data from GCS
def load_data_from_gcs(filepath):
    spark = SparkSession.builder \
        .appName("time series") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22") \
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("google.cloud.auth.service.account.json.keyfile", "D:/BDMA/UPC/BDM/P1/VBP_Joint_Project/gcs_config.json") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    df = spark.read.parquet(filepath)

    return df

def test_gcs():
    gcs_parquet_path = 'gs://spicy_1/bigbasket_products*'
    bigbasket_products_df = load_data_from_gcs(gcs_parquet_path)
    bigbasket_products_df.show()

# Define a function to extract price information
@udf(FloatType())
def extract_price(price):
    try:
        return 0.011*float(price.replace(",",""))
    except:
        return 0.0
    
# Define a function to extract product descriptions
@udf(StringType())
def extract_specs(spec_str):
    try:
        descr=''
        for text in spec_str:
            descr+=" "+text
        return descr
    except:
        return "no description available"
    
# Read the JSON file
def read_json(file):
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

# Function to generate UUID for product ID
def generate_uuid():
    return str(uuid.uuid4())

# Define a function to extract price information
@udf(FloatType())
def extract_price(price_str):
    try:
        price_list = price_str.split(" ")
        if len(price_list) > 1:
            qty = int(price_list[0])
            total_price_str = price_list[2]
            if total_price_str[0] == "£":
                total_price = float(total_price_str[1:].replace(",", ""))
            else:
                total_price = float(total_price_str[:-1].replace(",", "")) / 100
            unit_price = total_price / qty
            return 1.16 * unit_price
        else:    
            if price_str[0] == "£":
                return 1.16 * float(price_str[1:].replace(",", ""))
            else:
                return 1.16 * float(price_str[:-1].replace(",", "")) / 100
    except:
        return 0.0

def write_parquet_to_gcs(df, filename):
    df.write.mode('overwrite').parquet(f'gs://formatted_zone/{filename}_'+datetime.now().strftime("%Y%m%d%H%M%S")+'.parquet')
    
def get_all_products():
    #processing big basket data
    parquet_filepath= "gs://spicy_1/bigbasket_products*"
    df_bigbasket = load_data_from_gcs(parquet_filepath)

    # Print the shape of the DataFrame
    print("Shape of DataFrame: ({}, {})".format(df_bigbasket.count(), len(df_bigbasket.columns)))

    # Show the first few rows of the DataFrame
    df_bigbasket.show(5)
    filter_category=['Beauty & Hygiene', 'Kitchen, Garden & Pets', 'Cleaning & Household', 'Baby Care']
    df_bigbasket_filtered = df_bigbasket.filter(~col("category").isin(filter_category))

    # Print the shape of the DataFrame
    print("Shape of DataFrame: ({}, {})".format(df_bigbasket_filtered.count(), len(df_bigbasket_filtered.columns)))
    df_bigbasket_filtered.groupBy("category").count().show()
    df_bigbasket_products = df_bigbasket_filtered.select('product', 'sale_price', 'description')
    df_bigbasket_products = df_bigbasket_products.withColumn("product_id", F.expr("uuid()"))

    # Round sale_price
    df_bigbasket_products = df_bigbasket_products.withColumn("sale_price", round(col("sale_price") * 0.011, 2))

    # Rename columns
    df_bigbasket_products = df_bigbasket_products \
        .withColumnRenamed("product", "product_name") \
        .withColumnRenamed("sale_price", "price") \
        .withColumnRenamed("description", "product_description") \
        .select("product_id", "product_name", "price", "product_description")

    # Show the resulting DataFrame
    df_bigbasket_products.show()

    # processing flipkart data
    parquet_filepath= "gs://spicy_1/flipkart_products*"

    df_flipkart= load_data_from_gcs(parquet_filepath)

    # Print the shape of the DataFrame
    print("Shape of DataFrame: ({}, {})".format(df_flipkart.count(), len(df_flipkart.columns)))

    # Show the first few rows of the DataFrame
    df_flipkart.show(5)

    df_flipkart = df_flipkart.withColumn("product_id", F.expr("uuid()"))

    df_flipkart = df_flipkart \
    .withColumnRenamed("name", "product_name") \
    .withColumnRenamed("selling_price", "price") \
    .withColumnRenamed("specifications", "product_description") \
    .select("product_id", "product_name", "price", "product_description")

    df_flipkart= df_flipkart.withColumn("final_price", extract_price(df_flipkart['price']))
    df_flipkart= df_flipkart.withColumn("price", round(df_flipkart['final_price'], 2))
    df_flipkart= df_flipkart.withColumn("spec", extract_specs(df_flipkart['product_description']))
    df_flipkart= df_flipkart.drop("product_description")
    df_flipkart= df_flipkart\
                .withColumnRenamed("spec", "product_description")\
                .select("product_id", "product_name", "price", "product_description")
    df_flipkart.show()

    # processing the approved food data
    parquet_filepath= "gs://spicy_1/ApprovedFood*"

    # df_approvedfood= spark.read.parquet("data/gcs_raw_parquet/approved_food.parquet")
    df_approvedfood= load_data_from_gcs(parquet_filepath)

    # Print the shape of the DataFrame
    print("Shape of DataFrame: ({}, {})".format(df_approvedfood.count(), len(df_approvedfood.columns)))

    # Show the first few rows of the DataFrame
    df_approvedfood.show(5)

    df_approvedfood = df_approvedfood.withColumn("product_id", F.expr("uuid()"))

    df_approvedfood= df_approvedfood.withColumn("final_price", extract_price(df_approvedfood['Price']))

    df_approvedfood= df_approvedfood.withColumn("price", round(df_approvedfood['final_price'], 2))

    df_approvedfood = df_approvedfood \
        .withColumnRenamed("Product_name", "product_name") \
        .withColumnRenamed("Product_Description", "product_description") \
        .select("product_id", "product_name", "price", "product_description")

    # Show the resulting DataFrame
    df_approvedfood.show()

    df_concat= df_approvedfood.union(df_flipkart).union(df_bigbasket_products)

    df_concat= df_concat.withColumn("price", round(df_concat['price'], 2))

    # Print the shape of the DataFrame
    print("Shape of DataFrame: ({}, {})".format(df_concat.count(), len(df_concat.columns)))

    # Show the first few rows of the DataFrame
    df_concat.show()

    # write_parquet_to_gcs(df_concat, "all_products")

b2c_customer_counts= [1, 2, 2, 1, 1, 1, 1, 2, 1, 1, 1, 2, 3, 3, 6, 6, 6, 6, 6, 23, 24, 27, 28, 29, 11, 11, 12, 1, 3, 3, 4, 5, 5, 6, 7, 10, 10, 11, 1, 2, 3, 20, 23, 40, 40, 40, 41, 42, 22, 23, 24, 25, 26, 26, 27, 46, 47, 47, 47, 47, 47, 48, 48, 49, 49, 32, 32, 32, 32, 32, 33, 34, 19, 20, 21, 21, 41, 43, 25, 40, 40, 22, 41, 42, 43, 44, 45, 45, 30, 31, 31, 33, 34, 34, 35, 16, 17, 17, 20, 38, 38, 39, 55, 55, 55, 57, 76, 59, 60, 61, 62, 62, 65, 80, 80, 62, 62, 62, 62, 65, 81, 81, 82, 83, 68, 50, 51, 51, 66, 49, 30, 15, 16, 18, 0, 0, 0, 0, 1, 1, 2, 3, 4, 1, 1, 1, 2, 4, 4, 1, 2, 3, 4, 4, 6, 6, 7, 1, 2, 3, 4, 6, 1, 16, 17, 18, 18, 18, 20, 20, 1, 2, 2, 2, 21, 22, 23, 6, 7, 8, 8, 8, 11, 12, 27, 7, 8, 8, 9, 11, 12, 1, 19, 19, 20, 21, 22, 23, 24, 9, 10, 11, 12, 13, 14, 14, 33, 33, 14, 14, 16, 16, 16, 16, 17, 18, 19, 21, 21, 21, 22, 39, 40, 40, 43, 43, 44, 45, 46, 47, 47, 48, 48, 49, 64, 47, 47, 47, 48, 49, 32, 33, 33, 34, 34, 35, 36, 36, 36, 37, 38, 57, 60, 60, 61, 45, 30, 31, 32, 35, 35, 36, 54, 55, 56, 57, 60, 60, 61, 61, 62, 62, 63, 65, 84, 65, 66, 66, 67, 68, 70, 50, 51, 51, 69, 69, 69, 72, 73, 73, 74, 74, 55, 71, 73, 74, 74, 91, 91, 92, 75, 77, 77, 78, 79, 80, 80, 80, 82, 83, 99, 99, 80, 81, 82, 83, 83, 83, 83, 98, 99, 100, 103, 104, 105, 89, 90, 91, 92, 93, 94, 95, 79, 79, 62, 62, 63, 78, 78, 78, 79, 80, 81, 82, 83, 64, 65, 66, 49, 50, 52, 52, 67, 67, 84, 84, 64, 65, 66, 66, 85, 69, 70, 70, 72, 73, 74, 75, 75, 76, 76, 77, 77, 78, 78, 79, 79, 99, 100, 101, 84, 85, 85, 86, 67, 70, 71, 56, 57, 42, 43, 44, 47, 47, 48, 48, 68, 68, 69, 70, 70, 71, 72, 87, 88, 106, 108, 108, 109, 109, 124, 124, 125, 128, 147, 147, 148, 149, 150, 151, 154, 154, 155, 171, 156, 156, 172, 173, 173, 174, 193, 193, 193, 212, 213, 213, 214, 215, 215, 216, 216, 219, 234, 234, 251, 267, 267, 247, 250, 251, 270, 271, 271, 271, 255, 256, 256, 271, 287, 306, 324, 325, 326, 311, 311, 311, 311, 312, 292, 295, 280, 296, 296, 296, 296, 296, 298, 298, 298, 299, 300, 301, 301, 303, 303, 303, 304, 285, 286, 286, 287, 287, 287, 287, 287, 287, 287, 290, 291, 271, 272, 273, 254, 254, 256, 256, 274, 258, 259, 260, 261, 262, 262, 277, 278, 278, 279, 280, 281, 281, 282, 283, 283, 283, 284, 286, 266, 267, 268, 269, 269, 269, 270, 271, 271, 256, 256, 256, 256, 259, 260, 277, 278, 279, 280, 297, 299, 299, 299, 299, 300, 301, 302, 303, 304, 304, 322, 322, 323, 343, 345, 346, 346, 362, 363, 364, 364, 366, 346, 346, 362, 362, 363, 364, 366, 366, 384, 402, 403, 384, 401, 403, 403, 423, 406, 406, 406, 406, 409, 429, 446, 463, 482, 482, 483, 486, 486, 486, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 484, 469, 469, 469, 470, 471, 473, 474, 474, 474, 494, 495, 496, 499, 499, 499, 499, 500, 500, 500, 500, 500, 500, 500, 500, 482, 482, 485, 485, 485, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 484, 484, 485, 485, 486, 487, 487, 487, 488, 489, 490, 491, 473, 474, 475, 475, 477, 477, 478, 479, 463, 463, 448, 450, 450, 466, 467, 468, 468, 468, 471, 472, 472, 453, 453, 468, 469, 470, 453, 453, 454, 455, 456, 456, 458, 459, 478, 479, 480, 481, 481, 484, 485, 485, 486, 487, 488, 488, 491, 492, 493, 493, 493, 493, 494]

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
            current_count += random.randint(15, 20)
        else:
            current_count += random.randint(0, 1)
        current_count = min(current_count, total_customers)
        if current_count<=0:
            current_count=1
        counts.append(current_count)
    return counts

def generate_daily_data(current_date, num_customers, customer_ids, product_ids):
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

def get_customer_purchases():
    all_customer_parquet= "gs://spicy_1/customers*"
    all_products_parquet= "gs://formatted_zone/all_products*"

    df_customer = load_data_from_gcs(all_customer_parquet)
    df_customer.show(5)
    df_products = load_data_from_gcs(all_products_parquet)
    df_products.show(5)

    date_range_df = spark.sql("SELECT explode(sequence(to_date('2023-01-02'), to_date('2024-12-29'), interval 1 day)) AS date")
    date_range_df.show(5)

    # Get lists of customer ids, store ids, and product ids
    customer_ids = [row['customer_id'] for row in df_customer.select("customer_id").collect()]
    print(len(customer_ids))
    product_ids = [row['product_id'] for row in df_products.select("product_id").collect()]
    print(len(product_ids))

    synthetic_data = []
    start_date = date(2023, 1, 2)
    end_date = date(2024, 12, 29)
    current_date = start_date
    index = 0
    while current_date <= end_date:
        daily_data = generate_daily_data(current_date, b2c_customer_counts[index], customer_ids, product_ids)
        synthetic_data.extend(daily_data)
        current_date += timedelta(days=1)
        index += 1

    columns = ["purchase_id", "date", "customer_id", "product_id", "quantity"]
    df_time_series = spark.createDataFrame(synthetic_data, columns)
    row_count = df_time_series.count()
    column_count = len(df_time_series.columns)
    print(f"shape of the dataframe= {(row_count, column_count)}")
    df_time_series.show()

    df_cust= df_time_series.join(df_customer, "customer_id")
    df_cust.show()

    df_ts= df_cust.join(df_products, "product_id")

    df_ts= df_ts.withColumnRenamed("price", "unit_price")\
                .withColumnRenamed("date", "purchase_date")
    
    df_ts= df_ts.select("customer_id","customer_name","product_name","unit_price","quantity","purchase_date")
    df_ts.show()

    df_ts_pd= df_ts.toPandas()

    df_ts_pd.to_csv("customer_purchase_new.csv", index=False)
    # write_parquet_to_gcs(df_ts, "customer_purchases")


if __name__ == "__main__":
    # num_of_customers = int(config["CUSTOMER_PURCHASE"]["num_of_customers"])
    # num_of_purchases = int(config["CUSTOMER_PURCHASE"]["num_of_purchases"])
    # raw_data_dir = config["COMMON"]["raw_data_dir"]
    
    logger.info('-----------------------------------------------------')
    logger.info("Generating dataset for customer purchases")
    # get_all_products()
    get_customer_purchases()
    # generate_customer_purchase(num_of_customers, num_of_purchases, raw_data_dir)