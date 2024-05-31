
import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd


st.set_page_config(page_title="Sentiment Analysis", layout="wide")


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
        .config("google.cloud.auth.service.account.json.keyfile", "/home/pce/Documents/VBP_Joint_Project-main/formal-atrium-418823-7fbbc75ebbc6.json") \
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
    # Exclude 'time' column from display
    columns_to_display = [col for col in df.columns if col != 'time']
    st.write(df[columns_to_display])


# Streamlit App
st.title("Sentiment Analysis")

# Sidebar for selecting review type
review_type = st.sidebar.selectbox("Select Review Type", ["Business Reviews", "Customer Reviews"])

# Sidebar for selecting sentiment
sentiments = ["All", "positive", "neutral", "negative"]
selected_sentiment = st.sidebar.selectbox("Select Sentiment", sentiments)

# Sidebar for selecting verified status
verified_status = st.sidebar.selectbox("Verified", ["All", "Yes", "No"])

# Sidebar for selecting score range
min_score = st.sidebar.slider("Select minimum score", min_value=0.0, max_value=5.0, value=0.0, step=0.1)
max_score = st.sidebar.slider("Select maximum score", min_value=0.0, max_value=5.0, value=5.0, step=0.1)

# Sidebar for selecting rating range
min_rating = st.sidebar.slider("Select minimum rating", min_value=1, max_value=5, value=1, step=1)
max_rating = st.sidebar.slider("Select maximum rating", min_value=1, max_value=5, value=5, step=1)

# Sidebar for selecting date range
start_date = st.sidebar.date_input("Start date")
end_date = st.sidebar.date_input("End date")

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