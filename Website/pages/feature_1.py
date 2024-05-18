import streamlit as st
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace

# Set page config for a better appearance
st.set_page_config(page_title="Food Item Perishability", layout="wide")

# Title of the app
st.title('Perishability of Food Items')

# Specify the path to the GCS Parquet file
gcs_parquet_path = 'gs://formatted_zone/estimated_avg_expiry'

# Function to load data from GCS
@st.cache_data
def load_data_from_gcs(filepath):
    spark = SparkSession.builder \
        .appName("GCS Files Read") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2") \
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("google.cloud.auth.service.account.json.keyfile", "C:\\UPC\\BDM\\Project\\VBP_Joint_Project\\gcs_config.json") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    df = spark.read.parquet(filepath)
    
    # Convert PySpark DataFrame to Pandas DataFrame
    return df.toPandas()

try:
    estimated_avg_expiry_df = load_data_from_gcs(gcs_parquet_path)

    estimated_avg_expiry_df = estimated_avg_expiry_df[['category', 'sub_category', 'product_name', 'avg_expiry_days']]
    
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

    # Sidebar for filtering options
    search_term_category = st.sidebar.text_input('**Category**', value='Grains')
    search_term_subcategory = st.sidebar.text_input('**Sub Category**')
    search_term_product_name = st.sidebar.text_input('**Product Name**')

    # Initialize state
    if 'min_selected_days' not in st.session_state:
        st.session_state.min_selected_days = int(estimated_avg_expiry_df['Average Expiry Days'].min())
    if 'max_selected_days' not in st.session_state:
        st.session_state.max_selected_days = int(estimated_avg_expiry_df['Average Expiry Days'].max())

    # Text inputs for min and max selected days
    st.session_state.min_selected_days = st.sidebar.number_input('**Min Average Expiry Days**', min_value=int(estimated_avg_expiry_df['Average Expiry Days'].min()), max_value=int(estimated_avg_expiry_df['Average Expiry Days'].max()), value=st.session_state.min_selected_days, key='min_days_input')
    st.session_state.max_selected_days = st.sidebar.number_input('**Max Average Expiry Days**', min_value=int(estimated_avg_expiry_df['Average Expiry Days'].min()), max_value=int(estimated_avg_expiry_df['Average Expiry Days'].max()), value=st.session_state.max_selected_days, key='max_days_input')

    # Filter DataFrame based on user input
    filtered_df = estimated_avg_expiry_df[
        (estimated_avg_expiry_df['Average Expiry Days'] >= st.session_state.min_selected_days) &
        (estimated_avg_expiry_df['Average Expiry Days'] <= st.session_state.max_selected_days)
    ]

    if search_term_category:
        filtered_df = filtered_df[
            filtered_df['Category'].str.contains(search_term_category, case=False, na=False) 
        ]
    if search_term_subcategory:
        filtered_df = filtered_df[
            filtered_df['Sub Category'].str.contains(search_term_subcategory, case=False, na=False) 
        ]
    if search_term_product_name:
        filtered_df = filtered_df[
            filtered_df['Product'].str.contains(search_term_product_name, case=False, na=False) 
        ]

    # Display the filtered DataFrame
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
