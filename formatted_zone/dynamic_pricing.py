from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, isnull, lit
from pyspark.sql.types import DoubleType, IntegerType
import datetime
import json
import requests
import numpy as np
import shutil
import os

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("Dynamic Pricing Model") \
    .getOrCreate()

# Load configuration
with open("business_config.json", "r") as config_file:
    config = json.load(config_file)

# Function to check if today is a holiday
def is_today_a_holiday():
    country_code = config["country_code"]
    current_year = datetime.datetime.now().year
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    url = f"https://date.nager.at/api/v3/publicholidays/{current_year}/{country_code}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        holidays = response.json()
        return any(holiday['date'] == today for holiday in holidays)
    except requests.RequestException as e:
        print(f"Error fetching holiday data: {e}")
        return False

# Check if today is a holiday
is_holiday_today = is_today_a_holiday()

# Define the UDF to calculate days to expiry based on today's date
def get_days_to_expiry(expiry_date):
    today_date = datetime.date.today()
    if isinstance(expiry_date, datetime.date):
        return (expiry_date - today_date).days
    else:
        expiry_date = datetime.datetime.strptime(str(expiry_date), "%Y-%m-%d").date()
        return (expiry_date - today_date).days

days_to_expiry_udf = udf(get_days_to_expiry, IntegerType())

# Define the UDF for longevity factor
# def longevity_factor(avg_expiry_days):
#     return float(1 - np.exp(-avg_expiry_days / 365))


def longevity_factor(avg_expiry_days):
    return float(np.exp(-avg_expiry_days / 365)*2)

longevity_factor_udf = udf(longevity_factor, DoubleType())

# Define the UDF for rule-based pricing

def rule_based_pricing(days_to_expiry, consumption_rate, base_price, avg_expiry_days):
    # Configuration for discounts and thresholds
    holiday_discount = config["pricing_rules"]['holiday_discount']
    threshold_days_high = config["pricing_rules"]["threshold_days_high"]
    discount_high = config["pricing_rules"]["discount_high"]
    threshold_days_medium = config["pricing_rules"]["threshold_days_medium"]
    discount_medium = config["pricing_rules"]["discount_medium"]
    discount_low_high_consumption = config["pricing_rules"]["discount_low_high_consumption"]
    discount_low_low_consumption = config["pricing_rules"]["discount_low_low_consumption"]
    min_price = config["pricing_rules"].get('min_price', 0)  # Minimum price floor

    # Determine if it's a holiday for possible holiday discount or premium
    base_discount = holiday_discount if is_holiday_today else 1.0

    # Calculate the longevity scale to adjust pricing based on average expiry days
    longevity_scale = longevity_factor(avg_expiry_days)

    # Determine discount factor based on expiry thresholds
    if days_to_expiry > threshold_days_high:
        discount_factor = discount_medium
    elif days_to_expiry <= threshold_days_high and days_to_expiry > threshold_days_medium:
        discount_factor = discount_high
    else:
        discount_factor = discount_low_high_consumption if consumption_rate > 0.5 else discount_low_low_consumption

    # Calculate the total discount to be applied
    # total_discount = discount_factor * longevity_scale * base_discount
    total_discount = discount_factor * longevity_scale * base_discount


    # Ensure total discount does not exceed 100%
    total_discount = min(total_discount, 1)

    # Calculate final price ensuring it does not fall below the minimum price
    final_price = max(base_price * (1 - total_discount) , min_price)

    return final_price




pricing_udf = udf(rule_based_pricing, DoubleType())

# Define the path to the parquet file
parquet_path = "platform_customer_pricing_data_output"
df = spark.read.parquet(parquet_path)

df.show(20)
# Filter and modify specific rows
df_to_update = df.filter(isnull("selling_date") & isnull("buying_customer_id"))
df_to_update = df_to_update.withColumn("days_to_expiry", days_to_expiry_udf(col("expected_expiry_date")))
df_to_update = df_to_update.withColumn("longevity_scale", longevity_factor_udf(col("avg_expiry_days")))
df_to_update = df_to_update.withColumn("dynamic_price", pricing_udf(col("days_to_expiry"), col("percentage_consumed"), col("unit_price"), col("longevity_scale")))
# df_to_update = df_to_update.drop("days_to_expiry")  # Drop
# Extract unchanged rows
df_unchanged = df.filter(~(isnull("selling_date") & isnull("buying_customer_id")))

df_unchanged = df_unchanged.withColumn("days_to_expiry", lit(None).cast(IntegerType()))
df_unchanged = df_unchanged.withColumn("longevity_scale", lit(None).cast(DoubleType()))
df_unchanged = df_unchanged.withColumn("dynamic_price", lit(None).cast(DoubleType()))
# Combine updated and unchanged data
df_final = df_to_update.union(df_unchanged)
df_final.show()
# Write the combined DataFrame to a temporary location
temp_output_path = "temp_dynamic_pricing"
df_final.write.mode("overwrite").parquet(temp_output_path)

# Replace the original file with the updated file
def replace_original_with_temp(original_path, temp_path):
    try:
        if os.path.exists(original_path):
            shutil.rmtree(original_path)
        os.rename(temp_path, original_path)
    except Exception as e:
        print(f"Failed to replace original file with updated file: {e}")
        raise

replace_original_with_temp(parquet_path, temp_output_path)

# Stop the Spark session
spark.stop()
