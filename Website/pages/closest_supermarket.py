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
from bokeh.models import CustomJS, Button
from streamlit_bokeh_events import streamlit_bokeh_events

# # Not being used currently because the parquet to pandas doesn't work in my system.

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

# # The above is not being used.

def haversine(lat1, lon1, lat2, lon2):
    # calculates distance between two coordinates
    R = 6371  # Radius of the Earth in kilometers
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance


def closest_supermarkets(df_supermarkets, df_user_location):
    # Returns dataframe containing closest supermarkets to chosen customer
    df_supermarkets['distance_from_customer'] = df_supermarkets.apply(
    lambda row: haversine(df_user_location['latitude'], 
                          df_user_location['longitude'], 
                          row['latitude'], row['longitude']),
    axis=1)
    return df_supermarkets.sort_values(by='distance_from_customer').head(5)

def get_current_location():

    # Define the Bokeh button for triggering the geolocation
    loc_button = Button(label="Use Current Location")
    loc_button.js_on_event("button_click", CustomJS(code="""
    navigator.geolocation.getCurrentPosition(
        (location) => {
            const latitude = location.coords.latitude;
            const longitude = location.coords.longitude;
            document.dispatchEvent(new CustomEvent("GET_LOCATION", {detail: {latitude, longitude}}))
        }
    )
    """))

    # Create a Bokeh event for geolocation
    result = streamlit_bokeh_events(
        loc_button,
        events="GET_LOCATION",
        key="get_location",  # Unique identifier for this event
        refresh_on_update=False,
        override_height=75,
        debounce_time=0
    )

    # Display the latitude and longitude only if the button is clicked
    if result and "GET_LOCATION" in result:
            latitude = result["GET_LOCATION"]["latitude"]
            longitude = result["GET_LOCATION"]["longitude"]
            # st.write(latitude, longitude)
            return (latitude, longitude)
    return None


def add_marker_customer(df, color, mymap):
    # Adds marker for customer
    for _, row in df.iterrows():
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup="Usted esta aqui!",
            icon=folium.Icon(color=color)
        ).add_to(mymap)

def plot_supermarkets(df_all_supermarket_location):
    df_plot = df_all_supermarket_location.sample(n=50, random_state=42) # Taking a sample because there are too many points.

    # Initialize the map centered around the mean latitude and longitude
    mymap = folium.Map(location=[df_plot['latitude'].mean(), df_plot['longitude'].mean()], zoom_start=13)

    # Add points to the map
    for _, row in df_plot.iterrows():
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=row['commercial_name']
        ).add_to(mymap)
    folium_static(mymap, width=1000, height=600)


def add_supermarket_marker_line_customer(mymap, customer, supermarket):
    # Function to add a supermarket marker and a line between the customer and the supermarket with distance popup
    # Add supermarket marker

    marker = folium.Marker(
        location=[supermarket['latitude'], supermarket['longitude']],
        popup=supermarket['store_name'] + '\n' + str(supermarket['distance_from_customer']) + ' km',
        icon=folium.Icon(color='blue')
        )
    marker.add_to(mymap)

    
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

    # Add supermarkets and dashed lines to the map from the customer
    for _, supermarket in df_closest_supermarkets.iterrows():
        add_supermarket_marker_line_customer(mymap, df_chosen_customer.iloc[0], supermarket)

    # Add a line between the customer and the closest supermarket
    line = folium.PolyLine(
        locations=[
            [df_chosen_customer['latitude'].iloc[0], df_chosen_customer['longitude'].iloc[0]],
            [df_closest_supermarkets['latitude'].iloc[0], df_closest_supermarkets['longitude'].iloc[0]]
        ],
        color='green',
    ).add_to(mymap)

    folium_static(mymap, width=1000, height=600)


def im_feeling_lucky():
    st.write("Feeling lucky! Here’s a random supermarket deal for you...")


def main():

    root_dir = os.path.abspath(os.path.join(os.getcwd()))
    data_file_path = os.path.join(root_dir,'data', 'aryan_pandas')


    # Uncomment this when you want to read from gcs
    # establishment_catalonia = 'gs://'+formatted_zone_bucket+'/establishments_catalonia*'
    # df_all_supermarket_location = load_data_from_gcs(establishment_catalonia)

    # import data
    df_supermarket_products = pd.read_csv(os.path.join(data_file_path, 'supermarket_products.csv'),encoding='cp1252')
    df_all_supermarket_location = pd.read_csv(os.path.join(data_file_path,'establishment_catalonia.csv'))
    df_customer_info = pd.read_csv(os.path.join(data_file_path,'customers.csv'))
    df_cust_location = pd.read_csv(os.path.join(data_file_path,'location.csv'))
    df_cust_loc_mapping = pd.read_csv(os.path.join(data_file_path,'customer_location.csv'))

    # merge data for supermarkets
    df_supermarkets = pd.merge(df_supermarket_products, df_all_supermarket_location, left_on='store_id', right_on='id', how='left')
    df_supermarkets_location = df_supermarkets.drop_duplicates(subset=['store_id']) # keeping unique locations
    df_supermarkets_location = df_supermarkets_location[['store_id', 'store_name', 'full_address', 'latitude', 'longitude']]
    df_supermarkets_location.reset_index(drop=True, inplace=True)
    df_supermarkets = df_supermarkets[['store_id', 'store_name', 'product_id', 'product_name', 'product_price', 'quantity',
                                                      'expiry_date']]
    df_supermarkets.reset_index(drop=True, inplace=True)


    # st.write(df_supermarkets)

    # df_supermarkets has all the near expiry products being sold by supermarkets
    # df_supermarkets_location has the location of the above supermarkets
    # df_all_supermarket_location has all the available supermarkets in the region

    # choose option
    option = st.selectbox(
    'Choose an option',
    ('Find Supermarket deals near me', 'I\'m feeling lucky'))

    if option == 'Find Supermarket deals near me':
        location = get_current_location()
        if location:
            df_user_location = pd.DataFrame({'latitude': [location[0]], 'longitude': [location[1]]})
            df_user_location = df_user_location.iloc[[0]] # only keep the first row for the user in the dataframe
            st.write(f"Latitude: {df_user_location['latitude'][0]}, Longitude: {df_user_location['longitude'][0]}")
            st.write("Searching for supermarket deals near you..")
            df_closest_supermarkets = closest_supermarkets(df_supermarkets_location, df_user_location)
            # st.write(df_closest_supermarkets)
            display_closest_supermarkets(df_user_location, df_closest_supermarkets)
            selected_market = st.selectbox("Select supermarket", df_closest_supermarkets['store_name'])
            st.dataframe(df_supermarkets.query('store_name == @selected_market').reset_index(drop=True)[['product_id', 'product_name', 'product_price', 'quantity', 'expiry_date']],
                         use_container_width=True)
            # st.write('done')

        else:
            st.write('Click the button to allow access to your location.')

    elif option == 'I\'m feeling lucky':
        im_feeling_lucky()

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


if __name__ == '__main__':
    main()


