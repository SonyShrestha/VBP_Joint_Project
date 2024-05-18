import streamlit as st
import pandas as pd

# Set page config for a better appearance
st.set_page_config(page_title="Local Parquet Data Viewer", layout="wide")

# Title of the app
st.title('Customer Purchase with Expected Expiry')

# Function to load data from local Parquet file
def load_data(filepath):
    return pd.read_parquet(filepath)

# Specify the path to the local Parquet file
parquet_file_path = 'C:\\UPC\\BDM\\Project\\VBP_Joint_Project\\data\\formatted_zone\\purchases_nearing_expiry'

try:
    # Read the Parquet file into a DataFrame
    df = load_data(parquet_file_path)

    df = df[["customer_name", "product_name", "purchase_date", "expected_expiry_date"]]
    
    # Convert 'purchase_date' and 'expected_expiry_date' to datetime
    df['purchase_date'] = pd.to_datetime(df['purchase_date'])
    df['expected_expiry_date'] = pd.to_datetime(df['expected_expiry_date'])
    
    # Calculate the number of products expiring in the next 30 days
    upcoming_expiry_count = df[df['expected_expiry_date'] <= pd.Timestamp.now() + pd.Timedelta(days=30)].shape[0]

    # Display the small scorecard at the top
    st.markdown(f"""
        <style>
        .scorecard {{
            border-radius: 10px;
            padding: 10px;
            background-color: #f9f9f9;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            text-align: center;
            width: 250px;
            height: 100px;
            margin-bottom: 20px;
        }}
        .scorecard h2 {{
            margin: 0;
            font-size: 2em;
            color: #4CAF50;
        }}
        .scorecard p {{
            margin: 0;
            font-size: 1em;
            color: #666;
        }}
        </style>
        <div class="scorecard">
            <p>Products Expiring in 30 days</p>
            <h2>{upcoming_expiry_count}</h2>
        </div>
    """, unsafe_allow_html=True)

    # Filters for customer name and product name
    customer_name_filter = st.sidebar.text_input('**Customer Name**')
    product_name_filter = st.sidebar.text_input('**Product Name**')

    # Filters for purchase date range
    st.sidebar.markdown("**Purchase Date**")
    col1, col2 = st.sidebar.columns(2)
    min_purchase_date = col1.date_input('Min Purchase Date', value=df['purchase_date'].min().date(), key='min_purchase_date')
    max_purchase_date = col2.date_input('Max Purchase Date', value=df['purchase_date'].max().date(), key='max_purchase_date')

    # Filters for expected expiry date range
    st.sidebar.markdown("**Expected Expiry Date**")
    col3, col4 = st.sidebar.columns(2)
    min_expiry_date = col3.date_input('Min Expiry Date', value=df['expected_expiry_date'].min().date(), key='min_expiry_date')
    max_expiry_date = col4.date_input('Max Expiry Date', value=df['expected_expiry_date'].max().date(), key='max_expiry_date')

    # Apply filters
    if customer_name_filter:
        df = df[df['customer_name'].str.contains(customer_name_filter, case=False, na=False)]
    if product_name_filter:
        df = df[df['product_name'].str.contains(product_name_filter, case=False, na=False)]
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
    
    st.write(df)
    
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
        <p>Developed by SpicyBytes</p>
    </div>
""", unsafe_allow_html=True)
