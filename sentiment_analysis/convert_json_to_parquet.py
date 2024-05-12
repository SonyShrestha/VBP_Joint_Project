import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

# paths
root_path= "D:/BDMA/UPC/BDM/P1/VBP_Joint_Project"
bus_rev_json_path= os.path.join(root_path, "data/raw/business_reviews.json")
cust_rev_json_path= os.path.join(root_path, "data/raw/individual_reviews.json")

with open(bus_rev_json_path) as f:
  bus_json_data= f.read()

with open(cust_rev_json_path) as f:
  cust_json_data= f.read()

# Read JSON into DataFrame
df_bus_rev = pd.read_json(bus_json_data)
print(df_bus_rev.shape)
print(df_bus_rev.head())
df_cust_rev= pd.read_json(cust_json_data) 
print(df_cust_rev.shape)
print(df_cust_rev.head())

# Convert DataFrame to Arrow Table
table_bus_rev = pa.Table.from_pandas(df_bus_rev)
table_cust_rev = pa.Table.from_pandas(df_cust_rev)

# Write Arrow Table to Parquet file
pq.write_table(table_bus_rev, os.path.join(root_path, 'data/gcs_raw_parquet/business_reviews.parquet'))
pq.write_table(table_cust_rev, os.path.join(root_path, 'data/gcs_raw_parquet/individual_reviews.parquet'))