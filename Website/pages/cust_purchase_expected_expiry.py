import streamlit as st
import pandas as pd
import os
import re 

# Get the path to the parent parent directory
root_dir = os.path.abspath(os.path.join(os.getcwd()))

# Set page config for a better appearance
st.set_page_config(page_title="Local Parquet Data Viewer", layout="wide")

# Title of the app
st.title('Customer Purchase with Expected Expiry')

# Function to load data from local Parquet file
def load_data(filepath):
    return pd.read_parquet(filepath)

# Specify the path to the local Parquet file
parquet_file_path = os.path.join(root_dir,'data', 'formatted_zone', 'purchases_nearing_expiry')

try:
    col1, col2, col3,  col4, col5, col6 = st.columns(6)
    # Read the Parquet file into a DataFrame
    df = load_data(parquet_file_path)

    df = df[["customer_name", "product_name", "purchase_date", "expected_expiry_date"]]
    df['customer_name'] = df['customer_name'].str.strip()
    df['product_name'] = df['product_name'].str.strip()

    # Convert 'purchase_date' and 'expected_expiry_date' to datetime
    df['purchase_date'] = pd.to_datetime(df['purchase_date'])
    df['expected_expiry_date'] = pd.to_datetime(df['expected_expiry_date'])
    

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
    st.dataframe(df)
    
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