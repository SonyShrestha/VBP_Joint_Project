from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
import logging 
import os 
import configparser
from pyspark.sql.functions import explode, expr
import json
from pyspark.sql.functions import udf, array
from pyspark.sql import functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col,lit, lower,regexp_extract,trim,when, initcap
from pyspark.sql.functions import split, coalesce
from pyspark.sql.types import ArrayType, StringType
import re

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

# Get the path to the parent parent directory
config_dir = os.path.abspath(os.path.join(os.getcwd()))

# Specify the path to config file
config_file_path = os.path.join(config_dir, "config.ini")
config = configparser.ConfigParser()
config.read(config_file_path)


if __name__ == "__main__":
    gcs_config = config["GCS"]["credentials_path"]
    raw_bucket_name = config["GCS"]["raw_bucket_name"]
    item_desc_filter_out = config["EAT_BY_DATE"]["item_desc_filter_out"]

    spark = SparkSession.builder \
    .appName("Read Parquet File") \
    .getOrCreate()

    # Specify the path to the Parquet file
    parquet_file_path = "./data/parquet/eat_by_date.parquet"

    # Read the Parquet file into a DataFrame
    eat_by_date_df = spark.read.parquet(parquet_file_path)

    # Drop duplicates
    eat_by_date_df = eat_by_date_df.dropDuplicates()

    # Item description consists of string such as lasts for, last which needs to be cleaned
    pattern = r'\b(?:' + item_desc_filter_out.replace(',','|') + r')\b'
    eat_by_date_df = eat_by_date_df.withColumn("item_description", regexp_replace(eat_by_date_df["item_description"],pattern, ''))


    all_columns = eat_by_date_df.columns

    df = eat_by_date_df.withColumn("values", array(*[col(column) for column in eat_by_date_df.columns]))
    df = df.withColumn("kept_place",F.split(F.lit(','.join(all_columns)),","))


    # All columns except "item_description", "category","sub_category","type" will appear in the form of two columns 
    # one with kept_place as column name 
    # another with value as value for corresponding column name
    df = df.withColumn("new", F.arrays_zip("kept_place", "values"))\
       .withColumn("new", F.explode("new"))\
       .select("item_description", "category","sub_category","type", F.col("new.kept_place").alias("kept_place"), F.col("new.values").alias("values"))
    df = df.filter(col("values").isNotNull())
    df = df.filter((col("kept_place") != 'item_description') & 
                 (col("kept_place") != 'category') & 
                 (col("kept_place") != 'sub_category') & 
                 (col("kept_place") != 'type'))

    # If certain column contains values in the form of sth1/sth2, sth1 or sth2 split it into two rows with all remainiing values identical except that column with values sth1 and sth2
    df = df.withColumn("type", split(col("type"), "/")) \
             .withColumn("type", explode(col("type")))
    
    df = df.withColumn("type", split(col("type"), " or ")) \
             .withColumn("type", explode(col("type")))
    
    df = df.withColumn("kept_place", split(col("kept_place"), "/")) \
             .withColumn("kept_place", explode(col("kept_place")))
    
    # Replace special characters in type with empty string
    df = df.withColumn("type", lower(regexp_replace(df["type"], "[^a-zA-Z0-9\s]", "")))

    # Filter out records whose expected expiry date is not provided
    df = df.filter((col("values") != '-') & 
                 (col("values") != '--'))
    
    regex_duration = r'(\d+\*?\s*(?:-\s*\d+)?\s*(?:\+)?\s*(?i:Years?|Months?|Days?|Weeks?|Hours?)?)'


    # Extract average expiry date in terms of number of days
    df = df.withColumn("values", regexp_extract(df["values"], regex_duration, 1))
    df = df.withColumn("values", 
                     regexp_replace(regexp_replace(regexp_replace("values", " - ", "-"), "\*", ""), "\+", ""))

    df = df.withColumn("values", trim(df["values"]))

    df = df.withColumn("interval", lower(split(df["values"], ' ')[1]))
    df = df.withColumn("num", split(split(df["values"], ' ')[0],'-')[0])

    conditions_mult = [
        ((df["interval"] == "days") | (df["interval"] == "day"), 1),
        ((df["interval"] == "weeks") | (df["interval"] == "week"), 7),
        ((df["interval"] == "months") | (df["interval"] == "month"), 30),
        ((df["interval"] == "years") | (df["interval"] == "year"), 365)
    ]

    mult_val = (
        when(conditions_mult[0][0], conditions_mult[0][1])
        .when(conditions_mult[1][0], conditions_mult[1][1])
        .when(conditions_mult[2][0], conditions_mult[2][1])
        .when(conditions_mult[3][0], conditions_mult[3][1])
        .otherwise(1)  # Default value if none of the conditions are met
    )

    df = df.withColumn("avg_expiry_date_days", df["num"] * mult_val)


    


    # In item_description, replace anything that appreas within ()
    df = df.withColumn("item_description", trim(regexp_replace(df["item_description"], "\s*\([^()]*\)", "")))

    df = df.withColumn("category", initcap(df["category"]))

    # Clean up values in type field
    df = df.withColumn("type", when(col("type").like("%unopen%"), 'unopened')
                       .when(col("type").like("%uncook%"), 'uncooked')
                       .when(col("type").like("%unprepare%"), 'uncooked')
                       .when(col("type").like("%cook%"), 'cooked')
                       .when(col("type").like("%prepare%"), 'cooked')
                       .when(col("type").like("%open%"), 'opened')
                       .when(col("type").like("%seal%"), 'unopened')
                       .otherwise('unspecified'))


    # Clean up values in kept_place field
    df = df.withColumn("kept_place",
                   trim(regexp_replace(
                       when(col("kept_place").like("%refrigerator%"), "refrigerator")
                       .when(col("kept_place").like("%pantry%"), "pantry")
                       .when(col("kept_place").like("%outside%"), "outside")
                       .when(col("kept_place").like("%counter%"), "counter")
                       .when(col("kept_place").like("%freezer%"), "freezer")
                       .when(col("kept_place").like("%fridge%"), "fridge")
                       .when(col("kept_place").like("%past printed date%"), "outside")
                       .when(col("kept_place").like("%unopened%"), "outside")
                       .when(col("kept_place").like("%opened%"), "outside")
                       .otherwise(col("kept_place")),
                       " - beyond printed date",
                       ""
                   )
                ))
    
    # Filter out records whose expected expiry date is not provided
    df = df.filter(col("avg_expiry_date_days").isNotNull())

    # Cast avg_expiry_date_days value to int
    df = df.withColumn("avg_expiry_date_days", col("avg_expiry_date_days").cast("int"))

    # df = df.orderBy(df.category.desc(), df.sub_category.desc())
    # df.write.option("indent", 4).json("./data/cleaned/eat_by_date.json")

    # Drop unwanted fields
    columns_to_drop = ["values","num","interval"]

    df = df.drop(*columns_to_drop)

    product_avg_expiry_date = df.groupBy("item_description").agg(
        F.min("avg_expiry_date_days").alias("min_avg_expiry_days")
    )
    
    
    
    # distinct_values = df.select("avg_expiry_date_days").distinct().collect()
    # print("Distinct values in item_description column:")
    # for row in distinct_values:
    #     print(row["avg_expiry_date_days"])

    product_avg_expiry_date.show()