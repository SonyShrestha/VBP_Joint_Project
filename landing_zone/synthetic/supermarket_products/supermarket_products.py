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
from pyspark.sql.types import StringType, IntegerType, FloatType, ArrayType, DateType
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

# Load configuration
config_path = os.path.join('../../','config.ini')
config = configparser.ConfigParser()
config.read(config_path)
logging.info(f'Configuration loaded from {config_path}')

# Base directory for files
# raw_data_dir = config.get('COMMON', 'raw_data_dir')
raw_data_dir= os.path.join(root_dir, "data/raw")

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

def write_parquet_to_gcs(df, filename, zone):
    df.write.mode('overwrite').parquet(f'gs://{zone}/{filename}_'+datetime.now().strftime("%Y%m%d%H%M%S")+'.parquet')

def get_all_supermarkets():
    stores_csv = os.path.join(raw_data_dir, 'establishments_catalonia.csv')
    
    # filter only supermarkets
    supermarkets = []
    with open(stores_csv, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if "supermercat" in row['activity_description'].lower():
                supermarkets.append(row)
    df_supermarkets= pd.DataFrame(supermarkets)
    df_supermarkets= df_supermarkets[["id", "commercial_name"]]
    df_supermarkets= df_supermarkets.rename(columns={"id": "store_id", "commercial_name": "store_name"})

    # create spark dataframe
    df_sm = spark.createDataFrame(df_supermarkets)
    # Drop duplicates
    df_supermarket_filtered = df_sm.dropDuplicates()
    # Drop rows with any null values
    df_supermarket_filtered = df_supermarket_filtered.na.drop()
    # Count the number of rows and columns (shape equivalent)
    row_count = df_supermarket_filtered.count()
    column_count = len(df_supermarket_filtered.columns)
    print(f"shape of the dataframe= {(row_count, column_count)}")
    df_supermarket_filtered.show()

    # output the spark dataframe to gcs
    # write_parquet_to_gcs(df_supermarket_filtered, "all_supermarkets")

# Define UDF to generate random product_ids

# Extract product_ids from df_products and convert to list
products_gcs_path= "gs://formatted_zone/all_products*"
df_products = load_data_from_gcs(products_gcs_path)
product_list = df_products.select("product_id").rdd.flatMap(lambda x: x).collect()
print(len(product_list))
@udf(ArrayType(StringType()))
def generate_random_products():
    return np.random.choice(product_list, np.random.randint(200, 501), replace=False).tolist()

# Define UDF to generate random quantity
@udf(IntegerType())
def generate_random_quantity():
    return np.random.randint(1, 101)

# Function to generate random manufacture_date and expiry_date
def generate_dates(num_rows):
    manufacture_start = pd.to_datetime('2023-11-01')
    manufacture_end = pd.to_datetime('2024-05-20')
    
    # Generate random manufacture dates
    manufacture_dates = manufacture_start + pd.to_timedelta(np.random.randint(0, (manufacture_end - manufacture_start).days + 1, num_rows), unit='D')
    
    # Generate random expiry dates (between 1 and 200 days after manufacture date)
    expiry_dates = manufacture_dates + pd.to_timedelta(np.random.randint(5, 201, num_rows), unit='D')
    
    return manufacture_dates, expiry_dates

def get_supermarket_inventory():
    products_gcs_path= "gs://formatted_zone/all_products*"
    sm_gcs_path= "gs://formatted_zone/all_supermarkets*"

    df_products = load_data_from_gcs(products_gcs_path)
    row_count = df_products.count()
    column_count = len(df_products.columns)
    print(f"shape of the dataframe= {(row_count, column_count)}")
    df_products.show(5)
    df_supermarkets= load_data_from_gcs(sm_gcs_path)
    row_count = df_supermarkets.count()
    column_count = len(df_supermarkets.columns)
    print(f"shape of the dataframe= {(row_count, column_count)}")
    df_supermarkets.show(5)

    # Set seed for reproducibility
    np.random.seed(42)

    # Add a column 'product_ids' with randomly selected product_ids to df_supermarkets
    df_supermarkets = df_supermarkets.withColumn("product_list", generate_random_products())

    # Add a column 'num_of_products' with the number of products in 'product_ids'
    df_supermarkets = df_supermarkets.withColumn("num_of_products", F.size("product_list"))

    df_supermarkets = df_supermarkets.withColumn("product_id", explode(df_supermarkets.product_list))
    df_supermarkets = df_supermarkets.drop("product_list", "num_of_products")
    df_supermarkets = df_supermarkets.dropna()
    df_supermarket_inventory = df_supermarkets.join(df_products, "product_id")
    df_supermarket_inventory = df_supermarket_inventory.withColumn("quantity", generate_random_quantity())
    df_supermarket_inventory = df_supermarket_inventory.dropna()
    row_count = df_supermarket_inventory.count()
    column_count = len(df_supermarket_inventory.columns)
    print(f"shape of the dataframe= {(row_count, column_count)}")
    df_supermarket_inventory.show()

    pd_sm_inventory= df_supermarket_inventory.toPandas()
    pd_sm_inventory['manufacture_date'], pd_sm_inventory['expiry_date'] = generate_dates(len(pd_sm_inventory))
    pd_sm_inventory= pd_sm_inventory.rename(columns={"price": "product_price"})
    pd_sm_inventory= pd_sm_inventory[['store_id','store_name','product_id','product_name','product_price','manufacture_date','expiry_date', 'quantity']]
    # output to csv file
    pd_sm_inventory.to_csv(os.path.join(raw_data_dir, "supermarket_products.csv"), index=False)
    
    spark_sm= spark.createDataFrame(pd_sm_inventory)
    spark_sm = spark_sm.withColumn('manufacture_date', col('manufacture_date').cast(DateType()))
    spark_sm = spark_sm.withColumn('expiry_date', col('expiry_date').cast(DateType()))

    pd_sm_inventory= spark_sm.toPandas()
    table = pa.Table.from_pandas(pd_sm_inventory)
    filename= "supermarket_products"
    str_parquet= f'{filename}_{datetime.now().strftime("%Y%m%d%H%M%S")}.parquet'
    print(str_parquet)
    output_path = f"{str_parquet}"
    print(output_path)
    pq.write_table(table, output_path)
    write_parquet_to_gcs(pd_sm_inventory, "supermarket_inventory", "spicy_1")

if __name__ == "__main__":
    logger.info('-----------------------------------------------------')
    logger.info("Generating supermarket inventory")
    # get_all_supermarkets()
    get_supermarket_inventory()