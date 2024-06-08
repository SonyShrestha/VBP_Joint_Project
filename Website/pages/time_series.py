import streamlit as st
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
from prophet import Prophet

root_dir = os.path.abspath(os.path.join(os.getcwd()))

# Specify the path to config file
config_file_path = os.path.join(root_dir, "config.ini")
config = configparser.ConfigParser()
config.read(config_file_path)

config_file_path_json = os.path.join(root_dir, "config.json")
with open(config_file_path_json) as f:
    config_json = json.load(f)

# gcs cinfiguration
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(root_dir,"gcs_config.json")
# os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-11.0.7"
# os.environ["SPARK_HOME"] = "C:/spark"
# findspark.init()
spark = SparkSession.builder.getOrCreate()

def show_feature5():
    st.write("<br>", unsafe_allow_html=True) 
    st.header("Time Series Analysis")

    # Dropdown for selecting B2B or B2C
    option = st.selectbox("Select Business Type", ("C2C", "C2C with expected price", "C2C with dynamic pricing", "B2C"))

    # Date picker for selecting a date after 31st May 2024
    selected_date = st.date_input(
        "Select a date after 31st May 2024 for the forecast", 
        value=datetime(2024, 6, 1),
        min_value=datetime(2024, 6, 1)
    )

    if selected_date:
        # defining dates
        start_date= datetime(2024, 1, 1)
        end_date= datetime(2024, 5, 31)

        # Convert selected_date to datetime.datetime
        selected_date = datetime.combine(selected_date, datetime.min.time())

        # Convert selected_date to string format "29 March 2024"
        selected_date_str = selected_date.strftime("%d %B %Y")
        
        # Display the selected date in string format
        st.write(f"Selected Date for Forecast: {selected_date_str}")

        # Calculate the forecast period
        total_period = (selected_date - start_date).days
        forecast_period = (selected_date - end_date).days
        st.write(f"Total Period: {total_period} days")
        st.write(f"Forecast Period: {forecast_period} days")

        if st.button("Forecast"):
            with st.spinner("Loading data and preprocessing..."):
            # Load and preprocess data based on the selected option
                st.spinner("Loading and preprocessing data...")
                if option == "C2C":
                    c2c_parquet= "gs://spicy_1/customer_purchase*"
                    df_sales = preprocess_data_C2C(c2c_parquet)
                elif option == "C2C with dynamic pricing":
                    c2c_parquet= "gs://formatted_zone/platform_customer_pricing_data_output*"
                    df_sales = preprocess_data_C2C_dynamic(c2c_parquet)
                elif option == "C2C with expected price":
                    c2c_parquet= "gs://formatted_zone/platform_customer_pricing_data_output*"
                    df_sales = preprocess_data_C2C_expected(c2c_parquet)
                elif option == "B2C":
                    b2c_parquet= "gs://formatted_zone/b2c_time_series*"
                    df_sales = preprocess_data_B2C(b2c_parquet)
                
                st.write("Data preprcoessed and read for forecast")

            with st.spinner("Performing time series forecasting..."):
                # Perform time series forecasting
                st.spinner("Performing time series forecasting...")
                forecast_results = perform_time_series(df_sales, total_period, forecast_period)
                st.write("Forecasting completed.")

            # Show the forecast plots
            st.header("Forecast Plots")
            st.subheader(f"Sales forecast till {selected_date_str}")
            st.pyplot(forecast_results['model_plot'])
            st.subheader(f"Components of the time series")
            st.pyplot(forecast_results['components_plot'])

            # Plot aggregated forecasts
            st.subheader("Aggregated Forecast Plots")
            plot_aggregated_forecasts(df_sales, forecast_results['forecast'])

# Function to load data from GCS
def load_data_from_gcs(filepath):
    spark = SparkSession.builder \
        .appName("time series") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22") \
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("google.cloud.auth.service.account.json.keyfile", os.path.join(root_dir,"gcs_config.json")) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    df = spark.read.parquet(filepath)

    return df

def test_gcs():
    gcs_parquet_path = 'gs://spicy_1/bigbasket_products*'
    bigbasket_products_df = load_data_from_gcs(gcs_parquet_path)
    bigbasket_products_df.show()

@st.cache_data
def preprocess_data_C2C_expected(data):
    df_c2c = load_data_from_gcs(data)

    # Print the shape of the DataFrame
    print("Shape of DataFrame: ({}, {})".format(df_c2c.count(), len(df_c2c.columns)))

    df_c2c_expected= df_c2c.select("purchase_date", "unit_price", "quantity", "expected_price")
    df_c2c_expected= df_c2c_expected.dropna()
    print("Shape of DataFrame: ({}, {})".format(df_c2c_expected.count(), len(df_c2c_expected.columns)))

    # Show the first few rows of the DataFrame
    df_c2c.show(5)
    df_c2c_expected.show(5)

    st.write("Data loaded")

    df_c2c_expected = df_c2c_expected.withColumn("total_price", col("expected_price") * col("quantity"))

    # Aggregate the data by date
    daily_sales = df_c2c_expected.groupBy("purchase_date").agg(sum("total_price").alias("total_sales"))

    daily_sales= daily_sales.withColumn("total_sales", round(daily_sales['total_sales'], 2))

    daily_sales.show()

    # Convert to Pandas DataFrame for time series analysis
    df_sales = daily_sales.toPandas()
    df_sales['purchase_date'] = pd.to_datetime(df_sales['purchase_date'])
    df_sales= df_sales.sort_values("purchase_date")

    # Define the full date range
    full_date_range = pd.date_range(start='2024-01-01', end='2024-05-31')

    # Reindex the DataFrame to include all dates in the range
    df_sales.set_index('purchase_date', inplace=True)
    df_sales = df_sales.reindex(full_date_range)

    # Reset the index to have a proper DataFrame structure
    df_sales.reset_index(inplace=True)

    # Rename the columns
    df_sales.columns = ['purchase_date', 'total_sales']

    # Fill NaN values in total_sales with 0
    df_sales['total_sales'].fillna(0, inplace=True)

    # round off total sales to 2 decimal places
    df_sales['total_sales'] = df_sales['total_sales'].round(2)

    print(df_sales.head(100))

    return df_sales

@st.cache_data
def preprocess_data_C2C_dynamic(data):
    df_c2c = load_data_from_gcs(data)

    # Print the shape of the DataFrame
    print("Shape of DataFrame: ({}, {})".format(df_c2c.count(), len(df_c2c.columns)))

    df_c2c_dynamic= df_c2c.select("purchase_date", "unit_price", "quantity", "dynamic_price")
    df_c2c_dynamic= df_c2c_dynamic.dropna()
    print("Shape of DataFrame: ({}, {})".format(df_c2c_dynamic.count(), len(df_c2c_dynamic.columns)))

    # Show the first few rows of the DataFrame
    df_c2c.show(5)
    df_c2c_dynamic.show(5)

    st.write("Data loaded")

    df_c2c_dynamic = df_c2c_dynamic.withColumn("total_price", col("dynamic_price") * col("quantity"))

    # Aggregate the data by date
    daily_sales = df_c2c_dynamic.groupBy("purchase_date").agg(sum("total_price").alias("total_sales"))

    daily_sales= daily_sales.withColumn("total_sales", round(daily_sales['total_sales'], 2))

    daily_sales.show()

    # Convert to Pandas DataFrame for time series analysis
    df_sales = daily_sales.toPandas()
    df_sales['purchase_date'] = pd.to_datetime(df_sales['purchase_date'])
    df_sales= df_sales.sort_values("purchase_date")

    # Define the full date range
    full_date_range = pd.date_range(start='2024-01-01', end='2024-05-31')

    # Reindex the DataFrame to include all dates in the range
    df_sales.set_index('purchase_date', inplace=True)
    df_sales = df_sales.reindex(full_date_range)

    # Reset the index to have a proper DataFrame structure
    df_sales.reset_index(inplace=True)

    # Rename the columns
    df_sales.columns = ['purchase_date', 'total_sales']

    # Fill NaN values in total_sales with 0
    df_sales['total_sales'].fillna(0, inplace=True)

    # round off total sales to 2 decimal places
    df_sales['total_sales'] = df_sales['total_sales'].round(2)

    print(df_sales.head(100))

    return df_sales

@st.cache_data
def preprocess_data_B2C(data):
    df_b2c = load_data_from_gcs(data)

    # Print the shape of the DataFrame
    print("Shape of DataFrame: ({}, {})".format(df_b2c.count(), len(df_b2c.columns)))

    # Show the first few rows of the DataFrame
    df_b2c.show()

    st.write("Data loaded")
    # Calculate total price for each row
    df_b2c = df_b2c.withColumn("total_price", col("unit_price") * col("quantity"))

    df_b2c= df_b2c.withColumnRenamed("date", "purchase_date")

    # Aggregate the data by date
    daily_sales = df_b2c.groupBy("purchase_date").agg(sum("total_price").alias("total_sales"))

    daily_sales= daily_sales.withColumn("total_sales", round(daily_sales['total_sales'], 2))

    daily_sales.show()

    # Convert to Pandas DataFrame for time series analysis
    df_sales = daily_sales.toPandas()
    df_sales['purchase_date'] = pd.to_datetime(df_sales['purchase_date'])
    df_sales= df_sales.sort_values("purchase_date")
    print(df_sales.head())

    return df_sales

@st.cache_data
def preprocess_data_C2C(data):
    df_c2c = load_data_from_gcs(data)

    # Print the shape of the DataFrame
    print("Shape of DataFrame: ({}, {})".format(df_c2c.count(), len(df_c2c.columns)))

    # Show the first few rows of the DataFrame
    df_c2c.show()

    st.write("Data loaded")
    # Calculate total price for each row
    df_c2c = df_c2c.withColumn("total_price", col("unit_price") * col("quantity"))

    # Aggregate the data by date
    daily_sales = df_c2c.groupBy("purchase_date").agg(sum("total_price").alias("total_sales"))

    daily_sales= daily_sales.withColumn("total_sales", round(daily_sales['total_sales'], 2))

    daily_sales.show()

    # Convert to Pandas DataFrame for time series analysis
    df_sales = daily_sales.toPandas()
    df_sales['purchase_date'] = pd.to_datetime(df_sales['purchase_date'])
    df_sales= df_sales.sort_values("purchase_date")
    print(df_sales.head())

    return df_sales

def perform_time_series(df, total_period, forecast_period):
    prophet_df = df.copy()
    prophet_df.rename(columns={'purchase_date': 'ds', 'total_sales': 'y'}, inplace=True)

    # Initialize the model with tuned parameters
    model = Prophet(growth='linear', yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False)
    model.add_seasonality(name='weekly', period=7, fourier_order=5)
    model.add_seasonality(name='monthly', period=30, fourier_order=5)
    model.add_seasonality(name='daily', period=1, fourier_order=5)
    model.fit(prophet_df)

    # Create a DataFrame to hold future dates
    future = model.make_future_dataframe(periods=forecast_period)

    # Print intermediate step
    st.write("Making future predictions...")

    # Predict future sales
    forecast = model.predict(future)

    # Post-process to ensure non-negative predictions
    forecast['yhat'] = forecast['yhat'].apply(lambda x: max(0, x))
    forecast['yhat_lower'] = forecast['yhat_lower'].apply(lambda x: max(0, x))
    forecast['yhat_upper'] = forecast['yhat_upper'].apply(lambda x: max(0, x))

    # Plot the forecast
    # Print intermediate step
    st.write("Creating forecast plots...")

    fig1 = model.plot(forecast)
    fig2 = model.plot_components(forecast)

    # now we need to plot different forecasts over the days, weeks, months and years
    # Extract the forecasted data for the next year
    future_forecast = forecast[['ds', 'yhat']].tail(total_period+1)

    return {'forecast': future_forecast, 'model_plot': fig1, 'components_plot': fig2}

def plot_aggregated_forecasts(df_sales, forecast):

    # original data over the dates
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(df_sales['purchase_date'], df_sales['total_sales'], label='Original Sales', color='blue')
    ax.plot(forecast['ds'], forecast['yhat'], label='Predicted Sales', color='orange')
    ax.set_title('Daily Sales')
    ax.set_xlabel('Date')
    ax.set_ylabel('Sales')
    ax.legend()
    ax.grid()
    st.pyplot(fig)
    
    # Weekly aggregation
    forecast['week'] = forecast['ds'].dt.isocalendar().week
    forecast['year'] = forecast['ds'].dt.year
    weekly_sales_total = forecast.groupby(['year', 'week']).agg({'yhat': 'sum'}).reset_index()
    
    df_sales['week'] = df_sales['purchase_date'].dt.isocalendar().week
    df_sales['year'] = df_sales['purchase_date'].dt.year
    weekly_sales_original = df_sales.groupby(['year', 'week']).agg({'total_sales': 'sum'}).reset_index()

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(weekly_sales_original['week'], weekly_sales_original['total_sales'], label='Original Sales', color='blue', marker="o")
    for year in weekly_sales_total['year'].unique():
        year_data = weekly_sales_total[weekly_sales_total['year'] == year]
        ax.plot(year_data['week'], year_data['yhat'], label=f'Year {year}', marker="o")

    ax.set_title('Toatal Sales Over Weeks')
    ax.set_xlabel('Week')
    ax.set_ylabel('Sales')
    ax.legend()
    ax.grid()
    st.pyplot(fig)

    # Monthly aggregation
    forecast['month'] = forecast['ds'].dt.month
    monthly_sales_forecast = forecast.groupby(['year', 'month']).agg({'yhat': 'sum'}).reset_index()

    df_sales['month'] = df_sales['purchase_date'].dt.month
    monthly_sales_original = df_sales.groupby(['year', 'month']).agg({'total_sales': 'sum'}).reset_index()

    # Plotting
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(monthly_sales_original['month'], monthly_sales_original['total_sales'], label='Original Sales', color='blue', marker="o")
    for year in monthly_sales_forecast['year'].unique():
        year_data = monthly_sales_forecast[monthly_sales_forecast['year'] == year]
        ax.plot(year_data['month'], year_data['yhat'], label=f'Year {year}', marker="o")

    ax.set_title('Total Sales Over Months')
    ax.set_xlabel('Month')
    ax.set_ylabel('Sales')
    ax.legend()
    ax.grid()
    st.pyplot(fig)

    # # Yearly aggregation
    # # Monthly aggregation
    # yearly_sales_forecast = forecast.groupby(['year']).agg({'yhat': 'sum'}).reset_index()
    # yearly_sales_original = df_sales.groupby(['year']).agg({'total_sales': 'sum'}).reset_index()

    # # Plotting
    # fig, ax = plt.subplots(figsize=(10, 6))
    # ax.plot(yearly_sales_original['year'], yearly_sales_original['total_sales'], label='Original Sales', color='blue', marker='o', linestyle='-')
    # for year in yearly_sales_forecast['year'].unique():
    #     year_data = yearly_sales_forecast[yearly_sales_forecast['year'] == year]
    #     ax.plot(year_data['year'], year_data['yhat'], label=f'Year {year}', marker='o', linestyle='-')

    # ax.set_title('Total Sales Over Years')
    # ax.set_xlabel('Year')
    # ax.set_ylabel('Sales')
    # ax.legend()
    # st.pyplot(fig)