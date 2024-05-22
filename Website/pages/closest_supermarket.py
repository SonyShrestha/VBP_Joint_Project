import streamlit as st
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace
import configparser
import json
import math
import folium
from streamlit_folium import st_folium, folium_static



root_dir = os.path.abspath(os.path.join(os.getcwd()))

# Specify the path to config file
config_file_path = os.path.join(root_dir, "config.ini")
config = configparser.ConfigParser()
config.read(config_file_path)

config_file_path_json = os.path.join(root_dir, "config.json")
with open(config_file_path_json) as f:
    config_json = json.load(f)

# Set page config for a better appearance
st.set_page_config(page_title="Shortest Path", layout="wide")

# Title of the app
st.title('Shortest Path Filter')

# Specify the path to the GCS Parquet file
formatted_zone_bucket = config["GCS"]["formatted_bucket_name"]
gcs_parquet_path = 'gs://'+formatted_zone_bucket+'/estimated_avg_expiry'

# Function to load data from GCS
@st.cache_data
def load_data_from_gcs(filepath):
    spark = SparkSession.builder \
        .appName("Feature 5") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2") \
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("google.cloud.auth.service.account.json.keyfile", os.path.join(root_dir,"gcs_config.json")) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    df = spark.read.parquet(filepath)
    
    # Convert PySpark DataFrame to Pandas DataFrame
    return df.toPandas()


def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of the Earth in kilometers
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

def closest_supermarkets(df_supermarkets, df_chosen_customer):
    df_supermarkets['distance_from_customer'] = df_supermarkets.apply(
    lambda row: haversine(df_chosen_customer['latitude'], 
                          df_chosen_customer['longitude'], 
                          row['latitude'], row['longitude']),
    axis=1)
    return df_supermarkets.sort_values(by='distance_from_customer').head(5)

def show_supermarkets(df_all_supermarket_location):
    df_plot = df_all_supermarket_location.sample(n=50, random_state=42)

    # Initialize the map centered around the mean latitude and longitude
    mymap = folium.Map(location=[df_plot['latitude'].mean(), df_plot['longitude'].mean()], zoom_start=13)

    # Add points to the map
    for _, row in df_plot.iterrows():
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=row['commercial_name']
        ).add_to(mymap)
    folium_static(mymap, width=1000, height=600)


def add_marker_supermarket(df, color, mymap):
    for _, row in df.iterrows():
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=row['store_name'],
            icon=folium.Icon(color=color)
        ).add_to(mymap)


def add_marker_customer(df, color, mymap):
    for _, row in df.iterrows():
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=row['customer_name'],
            icon=folium.Icon(color=color)
        ).add_to(mymap)

def add_supermarket_line_customer(mymap, customer, supermarket):
    # Function to add a supermarket marker and a line between the customer and the supermarket with distance popup
    # Add supermarket marker
    folium.Marker(
        location=[supermarket['latitude'], supermarket['longitude']],
        popup=supermarket['store_name'] + '\n' + str(supermarket['distance_from_customer']),
        icon=folium.Icon(color='blue')
    ).add_to(mymap)
    
    # Add a line between the customer and the supermarket
    line = folium.PolyLine(
        locations=[
            [customer['latitude'], customer['longitude']],
            [supermarket['latitude'], supermarket['longitude']]
        ],
        color='blue',
        dash_array = '10'
    ).add_to(mymap)

def display_closest_supermarkets(df_chosen_customer, df_closest_supermarkets):
    # Initialize the map centered around the mean latitude and longitude of the customer

    mymap = folium.Map(location=[df_chosen_customer['latitude'].mean(), df_chosen_customer['longitude'].mean()], zoom_start=13)
    add_marker_customer(df_chosen_customer, 'red', mymap) # plot customer

    # add_marker_supermarket(df_closest_supermarkets, 'blue', mymap) # this is just for the marker for supermarket

    # Add supermarkets and lines to the map
    for _, supermarket in df_closest_supermarkets.iterrows():
        add_supermarket_line_customer(mymap, df_chosen_customer.iloc[0], supermarket)

    # Add a line between the customer and the closest supermarket
    line = folium.PolyLine(
        locations=[
            [df_chosen_customer['latitude'].iloc[0], df_chosen_customer['longitude'].iloc[0]],
            [df_closest_supermarkets['latitude'].iloc[0], df_closest_supermarkets['longitude'].iloc[0]]
        ],
        color='green',
    ).add_to(mymap)

    folium_static(mymap, width=1000, height=600)

try:
    root_dir = os.path.abspath(os.path.join(os.getcwd()))

    # Specify the path to the local Pandas folder
    data_file_path = os.path.join(root_dir,'data', 'aryan_pandas')

    df_supermarket_products = pd.read_csv(os.path.join(data_file_path, 'supermarket_products.csv'),encoding='cp1252')
    df_all_supermarket_location = pd.read_csv(os.path.join(data_file_path,'establishment_catalonia.csv'))
    df_customer_info = pd.read_csv(os.path.join(data_file_path,'customers.csv'))
    df_cust_location = pd.read_csv(os.path.join(data_file_path,'location.csv'))
    df_cust_loc_mapping = pd.read_csv(os.path.join(data_file_path,'customer_location.csv'))

    df_supermarkets = pd.merge(df_supermarket_products, df_all_supermarket_location, left_on='store_id', right_on='id', how='left')
    # just for the sake of the query - removing the same locations
    df_supermarkets = df_supermarkets.drop_duplicates(subset=['latitude', 'longitude'])
    df_supermarkets.reset_index(drop=True, inplace=True)
    st.write('Supermarket')
    st.write(df_supermarkets.head())

    # Plot supermarkets
    show_supermarkets(df_all_supermarket_location)

    # For customers
    df_customer = df_customer_info.merge(df_cust_loc_mapping, on='customer_id')
    df_customer = df_customer.merge(df_cust_location, on='location_id')
    # just for the sake of the query - removing the same locations
    df_customer = df_customer.drop_duplicates(subset=['latitude', 'longitude'])
    df_customer.reset_index(drop=True, inplace=True)
    st.write('Customers')
    st.write(df_customer.head())


    # Create a dropdown to select a name
    selected_name = st.selectbox('Choose Customer', df_customer['customer_name'])

    # Filter the DataFrame based on the selected name
    df_chosen_customer = df_customer[df_customer['customer_name'] == selected_name]
    st.write(df_chosen_customer)

    # Find the closest supermarkets
    st.write('Closest Supermarkets to chosen customer')
    df_closest_supermarkets = closest_supermarkets(df_supermarkets, df_chosen_customer)
    st.write(df_closest_supermarkets)

    # Display closest supermarkets to a customer
    display_closest_supermarkets(df_chosen_customer, df_closest_supermarkets)

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