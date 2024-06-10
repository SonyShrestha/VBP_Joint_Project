from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, rand, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType

import numpy as np
from datetime import datetime, timedelta
from pyspark.sql.functions import struct


spark = SparkSession.builder \
    .appName("Data Processing with Spark") \
    .getOrCreate()
   

df = spark.read.parquet('purchases_nearing_expiry*')


def generate_expiry_date(purchase_date):
    purchase_datetime = datetime.strptime(purchase_date, '%Y-%m-%d')
    added_days = np.random.randint(30, 365)
    return (purchase_datetime + timedelta(days=added_days)).strftime('%Y-%m-%d')

expiry_udf = udf(generate_expiry_date, StringType())
df = df.withColumn("unit_price", col("unit_price").cast(FloatType()))

df = df.withColumn('expiry_date', expiry_udf(col('purchase_date')))


def calculate_expected_price(unit_price, percentage_consumed):
    discount_factor = 1 - (percentage_consumed * np.random.uniform(0.1, 0.5))
    return float(unit_price) * discount_factor

# Register the UDF with the appropriate return type

price_udf = udf(calculate_expected_price, DoubleType())

# Apply the UDF
df = df.withColumn('percentage_consumed', rand())
df = df.withColumn('expected_price', price_udf(col('unit_price'), col('percentage_consumed')))


# def generate_optional_fields(customer_id, expiry_date):
#     if np.random.rand() > 0.2:
#         expiry_datetime = datetime.strptime(expiry_date, '%Y-%m-%d')
#         subtract_days = np.random.randint(1, 15)
#         selling_date = (expiry_datetime - timedelta(days=subtract_days)).strftime('%Y-%m-%d')
#         return customer_id, selling_date  # Modify logic to generate a different customer ID
#     return None, None

def generate_optional_fields(customer_id, expiry_date):
    if np.random.rand() > 0.2:
        expiry_datetime = datetime.strptime(expiry_date, '%Y-%m-%d')
        subtract_days = np.random.randint(1, 15)
        selling_date = (expiry_datetime - timedelta(days=subtract_days)).strftime('%Y-%m-%d')
        # Simulate a different customer ID; ensure logic here is valid for your use case
        new_customer_id = str(int(customer_id) + 1)  # Example modification
        return (new_customer_id, selling_date)
    return (None, None)

# fields_udf = udf(generate_optional_fields, StringType())
# Register UDF with a struct return type
fields_udf = udf(generate_optional_fields, StructType([
    StructField("buying_customer_id", StringType(), True),
    StructField("selling_date", StringType(), True)
]))

# df = df.withColumn('new_fields', fields_udf(col('customer_id'), col('expiry_date')))
# df = df.withColumn('buying_customer_id', col('new_fields').getItem(0))
# df = df.withColumn('selling_date', col('new_fields').getItem(1))
# df = df.drop('new_fields')

df = df.withColumn('new_fields', fields_udf(col('customer_id'), col('expiry_date')))
df = df.withColumn('buying_customer_id', col('new_fields').getItem('buying_customer_id'))
df = df.withColumn('selling_date', col('new_fields').getItem('selling_date'))
df = df.drop('new_fields')

df.show()
df.write.mode('overwrite').parquet('platform_customer_pricing_data_output')
spark.stop()


