import streamlit as st
import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
import configparser
import os
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

# Get base directory
root_dir = os.path.abspath(os.path.join(os.getcwd()))

# Specify the path to config file
config_file_path = os.path.join(root_dir, "config.ini")
config = configparser.ConfigParser()
config.read(config_file_path)

config_file_path_json = os.path.join(root_dir, "config.json")
with open(config_file_path_json) as f:
    config_json = json.load(f)


# st.set_page_config(page_title="Dynamic Pricing", layout="wide")

# Title of the Streamlit app
st.title("Dynamic Pricing Model Results")

# Specify the path to the GCS Parquet file
platform_customer_pricing_data_path = 'gs://formatted_zone/platform_customer_pricing_data_output'


@st.cache(allow_output_mutation=True)
def load_data_from_gcs(filepath):
    gcs_config = config["GCS"]["credentials_path"]

    spark = SparkSession.builder \
        .appName("Feature 4") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2") \
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("google.cloud.auth.service.account.json.keyfile", gcs_config) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(filepath)
    return df

def dynamic_pricing_streamlit():
    # # Load the data from the parquet file
    # @st.cache
    # def load_data(parquet_path):
    #     return pd.read_parquet(parquet_path)
    #
    # parquet_path = "platform_customer_pricing_data_output"
    df_pandas= load_data_from_gcs(platform_customer_pricing_data_path)

    df = df_pandas.toPandas()

    # Calculate the percentage decrease from the unit price
    df['percentage_decrease'] = ((df['unit_price'] - df['dynamic_price']) / df['unit_price']) * 100

    # Display the DataFrame
    st.write("## Dynamic Pricing")

    # Enhanced Filtering
    st.write("### Advanced Filters")
    with st.expander("Filter Options"):
        days_to_expiry = st.slider("Days to Expiry", 0, 365, (0, 365))
        consumption_rate = st.slider("Consumption Rate", 0.0, 1.0, (0.0, 1.0))
        min_price = st.number_input("Minimum Price", value=0.0)
        max_price = st.number_input("Maximum Price", value=df['dynamic_price'].max())

    filtered_df = df[
        (df['days_to_expiry'] >= days_to_expiry[0]) &
        (df['days_to_expiry'] <= days_to_expiry[1]) &
        (df['percentage_consumed'] >= consumption_rate[0]) &
        (df['percentage_consumed'] <= consumption_rate[1]) &
        (df['dynamic_price'] >= min_price) &
        (df['dynamic_price'] <= max_price)
    ]

    # Summary Statistics
    st.write("### Summary Statistics")

    filtered_df.rename(columns={
      'id': 'Id',
      'customer_id': 'Customer Id',
      'customer_name': 'Customer Name',
      'email_id': 'Email Id',
      'unit_price': 'Unit Price',
      'quantity': 'Quantity',
      'purchase_date': 'Purchase Date',
      'product_name': 'Product Name',
      'expected_expiry_date': 'Expected Expiry Date',
      'expiry_date': 'Expiry Date',
      'avg_expiry_days': 'Average Expiry Days',
      'score': 'Score',
      'percentage_consumed': 'Percentage Consumed',
      'expected_price': 'Expected Price',
      'days_to_expiry': 'Days to Expiry',
      'longevity_scale': 'Longevity Scale',
      'dynamic_price': 'Dynamic Price',
      'percentage_decrease': 'Percentage Decrease',
      'buying_customer_id': 'Buying Customer Id',
      'selling_date': 'Selling Date'
    }, inplace=True)

    st.write(filtered_df.describe())

    # Key Metrics
    st.write("### Key Metrics")
    total_items = len(filtered_df)
    average_price = filtered_df['Dynamic Price'].mean()
    st.metric("Total Items", total_items)
    st.metric("Average Price", f"${average_price:.2f}")

    # Data Visualizations
    st.write("### Dynamic Price Distribution")
    fig = px.histogram(filtered_df, x='Dynamic Price', nbins=50, title='Dynamic Price Distribution')
    st.plotly_chart(fig)

    st.write("### Price vs Days to Expiry")
    fig = px.scatter(filtered_df, x='Days to Expiry', y='Dynamic Price', color='Percentage Consumed', title='Price vs Days to Expiry')
    st.plotly_chart(fig)

    st.write("### Average Price per Consumption Rate")
    avg_price_per_consumption = filtered_df.groupby('Percentage Consumed')['Dynamic Price'].mean().reset_index()
    fig = px.bar(avg_price_per_consumption, x='Percentage Consumed', y='Dynamic Price', title='Average Price per Consumption Rate')
    st.plotly_chart(fig)

    # Correlation between Days to Expiry and Percentage Decrease
    st.write("### Correlation between Days to Expiry and Percentage Decrease")
    fig = px.scatter(filtered_df, x='Days to Expiry', y='Percentage Decrease', trendline='ols', title='Days to Expiry vs Percentage Decrease')
    st.plotly_chart(fig)

    # Export Filtered Data
    st.write("### Export Filtered Data")
    @st.cache
    def convert_df_to_csv(df):
        return df.to_csv(index=False).encode('utf-8')

    csv = convert_df_to_csv(filtered_df)
    st.download_button(
        label="Download filtered data as CSV",
        data=csv,
        file_name='filtered_data.csv',
        mime='text/csv',
    )

    # Interactive Widgets
    st.write("### Select Columns to Display")
    all_columns = filtered_df.columns.tolist()
    filtered_df = filtered_df[filtered_df['Score']==100]
    selected_columns = st.multiselect("Select Columns", all_columns, default=all_columns)

    st.dataframe(filtered_df[selected_columns])

    # if __name__ == "__main__":
    #     st.write("Streamlit app is running. Adjust the sliders or filters to explore the data.")


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