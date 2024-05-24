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

b2c_customer_counts= [1, 1, 1, 1, 2, 2, 2, 5, 5, 6, 1, 2, 2, 1, 4, 4, 46, 47, 47, 48, 28, 29, 13, 14, 44, 45, 46, 47, 48, 49, 50, 98, 148, 149, 150, 151, 151, 152, 153, 154, 155, 156, 157, 157, 137, 138, 169, 169, 169, 172, 173, 216, 216, 248, 231, 231, 232, 233, 234, 234, 274, 275, 275, 277, 277, 277, 277, 278, 278, 278, 279, 279, 280, 326, 326, 327, 328, 331, 365, 365, 406, 407, 407, 407, 410, 411, 411, 412, 413, 394, 394, 395, 396, 397, 397, 398, 399, 400, 401, 402, 403, 386, 387, 387, 387, 388, 388, 388, 389, 390, 390, 390, 393, 393, 393, 393, 394, 394, 441, 442, 442, 424, 424, 464, 464, 464, 466, 500, 500, 500, 500, 500, 500, 500, 485, 485, 486, 487, 488, 469, 470, 471, 471, 472, 472, 472, 473, 476, 476, 477, 477, 477]

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
    # Generate the customer counts
    start_date = date(2024, 1, 1)
    end_date = date(2024, 5, 31)
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

    # Add a random column
    df_with_random = df_ts.withColumn("random", rand())

    # Shuffle the DataFrame by sorting on the random column
    shuffled_df = df_with_random.orderBy("random")

    # Drop the random column (optional)
    shuffled_df = shuffled_df.drop("random")

    # Show the shuffled DataFrame
    shuffled_df.show()

    df_ts_pd= shuffled_df.toPandas()

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