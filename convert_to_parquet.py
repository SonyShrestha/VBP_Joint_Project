import pandas as pd

# Load the CSV file into a pandas DataFrame
df = pd.read_csv('./data/raw/customer_purchase.csv')

# Specify the output Parquet file path
output_file = './data/gcs_raw_parquet/customer_purchase.parquet'

# Convert the DataFrame to Parquet format
df.to_parquet(output_file)
