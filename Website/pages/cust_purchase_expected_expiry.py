import streamlit as st
import pandas as pd
import os
import re 
from pyspark.sql import SparkSession
import configparser
import json

# Get the path to the parent parent directory
root_dir = os.path.abspath(os.path.join(os.getcwd()))

# Set page config for a better appearance
# st.set_page_config(page_title="Local Parquet Data Viewer", layout="wide")

# Title of the app
st.title('Customer Purchase with Expected Expiry')

root_dir = os.path.abspath(os.path.join(os.getcwd()))

# Specify the path to config file
config_file_path = os.path.join(root_dir, "config.ini")
config = configparser.ConfigParser()
config.read(config_file_path)

config_file_path_json = os.path.join(root_dir, "config.json")
with open(config_file_path_json) as f:
    config_json = json.load(f)

formatted_zone_bucket = config["GCS"]["formatted_bucket_name"]

# Function to load data from local Parquet file
def load_data(filepath):
    return pd.read_parquet(filepath)

@st.cache_data
def load_data_from_gcs(filepath):
    spark = SparkSession.builder \
        .appName("Feature 1") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2") \
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("google.cloud.auth.service.account.json.keyfile", os.path.join(root_dir,"gcs_config.json")) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    df = spark.read.parquet(filepath)
    
    # Convert PySpark DataFrame to Pandas DataFrame
    return df.toPandas()


def cust_purchase_expected_expiry():
    # Specify the path to the local Parquet file
    # parquet_file_path = os.path.join(root_dir,'data', 'formatted_zone', 'purchases_nearing_expiry')
    parquet_file_path = 'gs://'+formatted_zone_bucket+'/purchases_nearing_expiry*'

    try:
        st.write("<br>", unsafe_allow_html=True) 
        st.header("Estimation of Expected Expiry Date")

        col1, col2, col3,  col4, col5, col6 = st.columns(6)
        # Read the Parquet file into a DataFrame
        df = load_data_from_gcs(parquet_file_path)

        df = df[["customer_name", "product_name", "purchase_date", "expected_expiry_date"]]
        df['product_name'] = df['product_name'].str.title()
        df['purchase_date'] = pd.to_datetime(df['purchase_date'])
        df['expected_expiry_date'] = pd.to_datetime(df['expected_expiry_date'])

        df = df[df['expected_expiry_date'] >= df['purchase_date'] + pd.DateOffset(days=10)]
        df['customer_name'] = df['customer_name'].str.strip()
        df['product_name'] = df['product_name'].str.strip()
        

        distinct_customer = df['customer_name'].drop_duplicates().sort_values().tolist()
        # Filters for customer name and product name
        with col1:
            customer_name_filter = st.selectbox('**Customer Name**',distinct_customer, index=None)
        
        distinct_product = df[df['customer_name'] == customer_name_filter]['product_name'].drop_duplicates().sort_values().tolist()

        with col2:
            product_name_filter = st.selectbox('**Product Name**',distinct_product, index=None)

        with col3:
            min_purchase_date = st.date_input('Min Purchase Date', value=df['purchase_date'].min().date(), key='min_purchase_date')
        
        with col4:
            max_purchase_date = st.date_input('Max Purchase Date', value=df['purchase_date'].max().date(), key='max_purchase_date')

        with col5:
            min_expiry_date = st.date_input('Min Expiry Date', value=df['expected_expiry_date'].min().date(), key='min_expiry_date')
        
        with col6:
            max_expiry_date = st.date_input('Max Expiry Date', value=df['expected_expiry_date'].max().date(), key='max_expiry_date')

        # Apply filters
        if customer_name_filter:
            pattern = re.escape(customer_name_filter)
            df = df[df['customer_name'].str.contains(pattern, case=False, na=False)]
        if product_name_filter:
            pattern = re.escape(product_name_filter)
            df = df[df['product_name'].str.contains(pattern, case=False, na=False)]
        df = df[(df['purchase_date'] >= pd.to_datetime(min_purchase_date)) & (df['purchase_date'] <= pd.to_datetime(max_purchase_date))]
        df = df[(df['expected_expiry_date'] >= pd.to_datetime(min_expiry_date)) & (df['expected_expiry_date'] <= pd.to_datetime(max_expiry_date))]
        
        # Convert dates to string with date format only
        df['purchase_date'] = df['purchase_date'].dt.strftime('%Y-%m-%d')
        df['expected_expiry_date'] = df['expected_expiry_date'].dt.strftime('%Y-%m-%d')
        
        # Display the DataFrame
        df.rename(columns={
            'customer_name': 'Customer Name',
            'product_name': 'Product Name',
            'purchase_date': 'Purchase Date',
            'expected_expiry_date': 'Expected Expiry Date'
        }, inplace=True)
        
        st.write("<br>", unsafe_allow_html=True)  
        st.dataframe(df.sample(frac=1).reset_index(drop=True), use_container_width=True)
        
    except FileNotFoundError as e:
        st.error(f"File not found: {e}")
    except Exception as e:
        st.error(f"An error occurred: {e}")


    # Add an image at the end
    image_path = os.path.join(root_dir,'images','expiry_notification.jpg') 

    st.image(image_path, caption='Expiry Notification', use_column_width=True)

    # Custom CSS for footer
    st.markdown("""
        <style>
            footer {visibility: hidden;}
            .footer {
                position: fixed;
                left: 0;
                bottom: 0;
                width: 100%;
                background-color: #f1f1f1;
                color: black;
                text-align: center;
            }
        </style>
        <div class="footer">
            <p>Developed by SpicyBytes</p>
        </div>
    """, unsafe_allow_html=True)
