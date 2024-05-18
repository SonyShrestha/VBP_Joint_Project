import streamlit as st
import pandas as pd

# Title of the app
st.title('Customer Purchase Details')

# Specify the path to the local Parquet file
parquet_file_path = 'C:\\UPC\\BDM\\Project\\VBP_Joint_Project\\data\\formatted_zone\\purchases_nearing_expiry'

try:
    # Read the parquet file into a DataFrame
    df = pd.read_parquet(parquet_file_path)
    
    # Display the DataFrame
    st.write(df)
    
except FileNotFoundError:
    st.error(f"File not found: {parquet_file_path}")
except Exception as e:
    st.error(f"An error occurred: {e}")
