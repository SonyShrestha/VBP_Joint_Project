import streamlit as st
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace
import configparser
import json
import re

root_dir = os.path.abspath(os.path.join(os.getcwd()))

# Specify the path to config file
config_file_path = os.path.join(root_dir, "config.ini")
config = configparser.ConfigParser()
config.read(config_file_path)

config_file_path_json = os.path.join(root_dir, "config.json")
with open(config_file_path_json) as f:
    config_json = json.load(f)

# Set page config for a better appearance
st.set_page_config(page_title="Food Item Perishability", layout="wide")

# Title of the app
st.title('Perishability of Food Items')

# Specify the path to the GCS Parquet file
formatted_zone_bucket = config["GCS"]["formatted_bucket_name"]
gcs_parquet_path = 'gs://'+formatted_zone_bucket+'/estimated_avg_expiry*'

# Function to load data from GCS
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

try:
    col1, col2, col3,  col4, col5 = st.columns(5)

    estimated_avg_expiry_df = load_data_from_gcs(gcs_parquet_path)

    estimated_avg_expiry_df = estimated_avg_expiry_df[['category', 'sub_category', 'product_name', 'avg_expiry_days']]
    estimated_avg_expiry_df['category'] = estimated_avg_expiry_df['category'].str.strip()
    estimated_avg_expiry_df['sub_category'] = estimated_avg_expiry_df['sub_category'].str.strip()
    estimated_avg_expiry_df['product_name'] = estimated_avg_expiry_df['product_name'].str.strip()
    
    
    # Format the DataFrame
    estimated_avg_expiry_df['category'] = estimated_avg_expiry_df['category'].str.title()
    estimated_avg_expiry_df['sub_category'] = estimated_avg_expiry_df['sub_category'].str.title()
    estimated_avg_expiry_df['product_name'] = estimated_avg_expiry_df['product_name'].str.title()
    estimated_avg_expiry_df.rename(columns={
        'category': 'Category',
        'sub_category': 'Sub Category',
        'product_name': 'Product',
        'avg_expiry_days': 'Average Expiry Days'
    }, inplace=True)
    
    estimated_avg_expiry_df = estimated_avg_expiry_df.replace('', pd.NA).dropna(how='any')

    distinct_category = estimated_avg_expiry_df['Category'].drop_duplicates().sort_values().tolist()
    

    # Sidebar for filtering options
    with col1:
        search_term_category = st.selectbox('**Category**', distinct_category, index=None)

    # Assuming 'search_term_category' contains the category selected from the first dropdown.
    distinct_subcategory = estimated_avg_expiry_df[estimated_avg_expiry_df['Category'] == search_term_category]['Sub Category'].drop_duplicates().sort_values().tolist()


    # Place the text input for "Sub Category" in the second column
    with col2:
        search_term_subcategory = st.selectbox('**Sub Category**', distinct_subcategory, index=None)
    
    distinct_product_name = estimated_avg_expiry_df[((estimated_avg_expiry_df['Category'] == search_term_category) & (estimated_avg_expiry_df['Sub Category'] == search_term_subcategory))]['Product'].drop_duplicates().sort_values().tolist()


    with col3:
        search_term_product_name = st.selectbox('**Product Name**', distinct_product_name, index=None)

    # Initialize state
    if 'min_selected_days' not in st.session_state:
        st.session_state.min_selected_days = int(estimated_avg_expiry_df['Average Expiry Days'].min())
    if 'max_selected_days' not in st.session_state:
        st.session_state.max_selected_days = int(estimated_avg_expiry_df['Average Expiry Days'].max())

    # Text inputs for min and max selected days
    with col4:
        st.session_state.min_selected_days = st.number_input('**Min Average Expiry Days**', min_value=int(estimated_avg_expiry_df['Average Expiry Days'].min()), max_value=int(estimated_avg_expiry_df['Average Expiry Days'].max()), value=st.session_state.min_selected_days, key='min_days_input')
    
    with col5:
        st.session_state.max_selected_days = st.number_input('**Max Average Expiry Days**', min_value=int(estimated_avg_expiry_df['Average Expiry Days'].min()), max_value=int(estimated_avg_expiry_df['Average Expiry Days'].max()), value=st.session_state.max_selected_days, key='max_days_input')

    # Filter DataFrame based on user input
    filtered_df = estimated_avg_expiry_df[
        (estimated_avg_expiry_df['Average Expiry Days'] >= st.session_state.min_selected_days) &
        (estimated_avg_expiry_df['Average Expiry Days'] <= st.session_state.max_selected_days)
    ]

    if search_term_category:
        pattern = re.escape(search_term_category)
        filtered_df = filtered_df[
            filtered_df['Category'].str.contains(pattern, case=False, na=False) 
        ]
    if search_term_subcategory:
        pattern = re.escape(search_term_subcategory)
        filtered_df = filtered_df[
            filtered_df['Sub Category'].str.contains(pattern, case=False, na=False) 
        ]
    if search_term_product_name:
        pattern = re.escape(search_term_product_name)
        filtered_df = filtered_df[
            filtered_df['Product'].str.contains(pattern, case=False, na=False) 
        ]

    st.write("<br>", unsafe_allow_html=True)  

    cl1, cl2, cl3 = st.columns([1, 8, 1])  

    with cl2:
        st.dataframe(filtered_df)
    
except FileNotFoundError as e:
    st.error(f"File not found: {e}")
except Exception as e:
    st.error(f"An error occurred: {e}")

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
        <p>@Developed by SpicyBytes</p>
    </div>
""", unsafe_allow_html=True)
