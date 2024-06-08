
import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd
import os
import json
import configparser
from datetime import datetime


# st.set_page_config(page_title="Sentiment Analysis", layout="wide")

root_dir = os.path.abspath(os.path.join(os.getcwd()))

# Specify the path to config file
config_file_path = os.path.join(root_dir, "config.ini")
config = configparser.ConfigParser()
config.read(config_file_path)

config_file_path_json = os.path.join(root_dir, "config.json")
with open(config_file_path_json) as f:
    config_json = json.load(f)

# Specify the path to the GCS Parquet file
business_reviews_path = 'gs://formatted_zone/business_sentiment_*'
customer_reviews_path = 'gs://formatted_zone/customer_sentiment_*'


@st.cache_data
def load_data_from_gcs(filepath):
    spark = SparkSession.builder \
        .appName("GCS Connection") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2") \
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("google.cloud.auth.service.account.json.keyfile", os.path.join(root_dir,"gcs_config.json")) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    df = spark.read.parquet(filepath)
    
    # Convert PySpark DataFrame to Pandas DataFrame
    return df.toPandas()

# Load data from GCS
business_reviews_df = load_data_from_gcs(business_reviews_path)
customer_reviews_df = load_data_from_gcs(customer_reviews_path)

# 'date' columns are in datetime format
business_reviews_df['date'] = pd.to_datetime(business_reviews_df['date'])
customer_reviews_df['date'] = pd.to_datetime(customer_reviews_df['date'])



business_reviews_df['verified'] = business_reviews_df['verified'].astype(str)
customer_reviews_df['verified'] = customer_reviews_df['verified'].astype(str)


business_reviews_df['sentiment_label'] = business_reviews_df['sentiment_label'].astype(str)
customer_reviews_df['sentiment_label'] = customer_reviews_df['sentiment_label'].astype(str)


# Function to display dataframe
def display_reviews(df):
    df.rename(columns={
        "business_id":"Business Id",
        "business_name":"Business Name",
        "rating": "Rating",
        "verified": "Verified",
        "date": "Date",
        "review": "Review",
        "Sentiment": "Sentiment",
        "sentiment_label": "Sentiment Label",
        "sentiment_score": "Sentiment Score"
    }, inplace=True)
    # Exclude 'time' column from display
    columns_to_display = [col for col in df.columns if col != 'time']
    st.write(df[columns_to_display])


def sentiment_analysis():
    # Streamlit App
    st.write("<br>", unsafe_allow_html=True) 
    st.header("Sentiment Analysis")

    col1, col2, col3,  col4, col5, col6, col7, col8, col9 = st.columns(9)

    # Sidebar for selecting review type
    with col1:
        review_type = st.selectbox("Review Type", ["Business Reviews", "Customer Reviews"])

    # Sidebar for selecting sentiment
    with col2:
        sentiments = ["All", "positive", "neutral", "negative"]
        selected_sentiment = st.selectbox("Sentiment", sentiments)

    # Sidebar for selecting verified status
    with col3:
        verified_status = st.selectbox("Verified", ["All", "Yes", "No"])

    # Sidebar for selecting score range
    with col4:
        min_score = st.number_input("Minimum Score", min_value=0.0, max_value=5.0, value=0.0, step=0.1)
        
    with col5:
        max_score = st.number_input("Maximum Score", min_value=0.0, max_value=5.0, value=5.0, step=0.1)

    # Sidebar for selecting rating range
    with col6:
        min_rating = st.number_input("Minimum Rating", min_value=1, max_value=5, value=1, step=1)

    with col7:
        max_rating = st.number_input("Maximum Rating", min_value=1, max_value=5, value=5, step=1)

    # Sidebar for selecting date range
    with col8:
        start_date = st.date_input("Start Date", datetime(2024, 5, 1))

    with col9:
        end_date = st.date_input("End Date")

    # Convert date inputs to pandas datetime
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Filter data based on selections
    if review_type == "Business Reviews":
        df = business_reviews_df
    else:
        df = customer_reviews_df
        
        
    if selected_sentiment != "All":
        df = df[df['sentiment_label'].str.lower() == selected_sentiment.lower()]
        
        
    if verified_status == "Yes":
        df = df[df['verified'] == "Yes"]
        
    elif verified_status == "No":
        df = df[df['verified'] == "No"]
    
    df = df[(df['sentiment_score'] >= min_score) & (df['sentiment_score'] <= max_score)]
    df = df[(df['rating'] >= min_rating) & (df['rating'] <= max_rating)]
    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

    df['business_name'] = df['business_name'].apply(lambda x: x.title())
    df['sentiment_label'] = df['sentiment_label'].apply(lambda x: x.title())
    df['review'] = df['review'].apply(lambda x: x.capitalize())

    # Display filtered data
    display_reviews(df)


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