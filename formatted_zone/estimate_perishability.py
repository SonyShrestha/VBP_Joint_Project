from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
import logging 
import os 
import configparser
from pyspark.sql.functions import explode, array, split, regexp_replace, col, lit, lower, regexp_extract, trim, when, to_date, date_format, datediff
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

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



def preprocess_flipkart(flipkart_df):
    logger.info('-----------------------------------------------------')
    logger.info("Cleaning data for flipkart")
    # Drop duplicates
    flipkart_df = flipkart_df.dropDuplicates()

    flipkart_df = flipkart_df.select("prod_heirarachy", "expiry_date", "manufacturing_date")

    # Convert date format 23 Feb 2025 to 2025-02-23
    flipkart_df = flipkart_df.withColumn("expiry_date", to_date("expiry_date", "d MMM yyyy"))\
       .withColumn("expiry_date", date_format("expiry_date", "yyyy-MM-dd"))
    
    flipkart_df = flipkart_df.withColumn("manufacturing_date", to_date("manufacturing_date", "d MMM yyyy"))\
       .withColumn("manufacturing_date", date_format("manufacturing_date", "yyyy-MM-dd"))
    
    flipkart_df = flipkart_df.withColumn("avg_expiry_date_days", datediff("expiry_date","manufacturing_date"))

    flipkart_df = flipkart_df.select("avg_expiry_date_days", col("prod_heirarachy").getItem(0).alias("category"),
               col("prod_heirarachy").getItem(1).alias("sub_category"),
               col("prod_heirarachy").getItem(2).alias("item_description"))
    
    flipkart_df = flipkart_df.withColumn("category", lower(regexp_replace(col("category"), "-", " ")))
    flipkart_df = flipkart_df.withColumn("sub_category", lower(regexp_replace(col("sub_category"), "-", " ")))
    flipkart_df = flipkart_df.withColumn("item_description", lower(regexp_replace(col("item_description"), "-", " ")))

    product_avg_expiry_date = flipkart_df.groupBy("category","sub_category","item_description").agg(
        F.min("avg_expiry_date_days").alias("avg_expiry_days")
    )

    product_avg_expiry_date = product_avg_expiry_date.withColumnRenamed("item_description", "product_name")

    return product_avg_expiry_date



def preprocess_eat_by_date(eat_by_date_df,item_desc_filter_out):
    logger.info('-----------------------------------------------------')
    logger.info("Cleaning data for eat_by_date")
    # Drop duplicates
    eat_by_date_df = eat_by_date_df.dropDuplicates()
    
    # Main item_description is present in text - anything inside () was not required
    patt = r"\([^)]*\)"

    eat_by_date_df = eat_by_date_df.withColumn("item_description", regexp_replace(col("item_description"), patt, ""))

    # eat_by_date_df = eat_by_date_df.withColumn("item_description", regexp_replace(col("item_description"), r"\b\d+\w*\b", ""))

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


    # Drop unwanted fields
    columns_to_drop = ["values","num","interval"]

    df = df.drop(*columns_to_drop)


    # In item_description, replace anything that appreas within ()
    df = df.withColumn("item_description", trim(regexp_replace(df["item_description"], "\s*\([^()]*\)", "")))

    df = df.withColumn("category", lower(df["category"]))\
    .withColumn("sub_category", lower(df["sub_category"]))\
    .withColumn("item_description", lower(df["item_description"]))

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

    product_avg_expiry_date = df.groupBy("category","sub_category","item_description").agg(
        F.min("avg_expiry_date_days").alias("avg_expiry_days")
    )

    product_avg_expiry_date = product_avg_expiry_date.withColumnRenamed("item_description", "product_name")

    return product_avg_expiry_date



def preprocess_approved_food(approved_food_df, scrapped_date):
    logger.info('-----------------------------------------------------')
    logger.info("Cleaning data for approved_food")
    # Drop duplicates
    approved_food_df = approved_food_df.dropDuplicates()

    # patt1 = r"\(\d+(\.\d+)? (g|kg|L)\)"
    patt1 = r"\([^)]*\)"
    patt2 = r"\b\d+\s*[xX]\s*\d+\s*\D*\b"
    patt3 = r"\b\d+\s*(g|kg|L|ml)\b"

    approved_food_df = approved_food_df.withColumn("Product_name", regexp_replace(regexp_replace(regexp_replace(col("Product_name"), patt1, ""),patt2,""),patt3,""))

    # Convert date format 23 Feb 2025 to 2025-02-23
    approved_food_df = approved_food_df.withColumn("Expiry_Date", to_date("Expiry_Date", "d MMM yyyy"))\
       .withColumn("Expiry_Date", date_format("Expiry_Date", "yyyy-MM-dd"))
    
    approved_food_df = approved_food_df.withColumn("scrapped_date", lit(scrapped_date))
    
    approved_food_df = approved_food_df.withColumnRenamed("Expiry_Date", "expiry_date")\
        .withColumnRenamed("Product_name", "product_name")\
        .withColumnRenamed("Price", "price")\
        .withColumnRenamed("Product_Description", "product_description")

    approved_food_df = approved_food_df.withColumn("avg_expiry_date_days", datediff("expiry_date","scrapped_date"))

    approved_food_df = approved_food_df.filter(col("avg_expiry_date_days")>0)

    #approved_food_df.write.csv("./data/cleaned/approved_food.csv")

    product_avg_expiry_date = approved_food_df.groupBy("product_name").agg(
        F.min("avg_expiry_date_days").alias("avg_expiry_days")
    )

    product_avg_expiry_date = product_avg_expiry_date.withColumn("category", lit(" ").cast("string"))
    product_avg_expiry_date = product_avg_expiry_date.withColumn("sub_category", lit(" ").cast("string"))

    product_avg_expiry_date = product_avg_expiry_date.select("category", "sub_category", "product_name", "avg_expiry_days")

    return product_avg_expiry_date



if __name__ == "__main__":
    gcs_config = config["GCS"]["credentials_path"]
    raw_bucket_name = config["GCS"]["raw_bucket_name"]
    item_desc_filter_out = config["EAT_BY_DATE"]["item_desc_filter_out"]
    scrapped_date= '2024-01-01'

    spark = SparkSession.builder \
    .appName("Read Parquet File") \
    .getOrCreate()

    # Read the Parquet file into a DataFrame
    flipkart_df = spark.read.parquet("./data/gcs_raw_parquet/flipkart.parquet")

    avg_expiry_date_flipkart_df = preprocess_flipkart(flipkart_df)

    # Read the Parquet file into a DataFrame
    eat_by_date_df = spark.read.parquet("./data/gcs_raw_parquet/eat_by_date.parquet")

    avg_expiry_date_eat_by_date_df = preprocess_eat_by_date(eat_by_date_df, item_desc_filter_out)

    # Read the Parquet file into a DataFrame
    approved_food_df = spark.read.parquet("./data/gcs_raw_parquet/approved_food.parquet")

    avg_expiry_date_approved_food_df = preprocess_approved_food(approved_food_df,scrapped_date)

    avg_expiry_date_df = avg_expiry_date_flipkart_df.union(avg_expiry_date_eat_by_date_df).union(avg_expiry_date_approved_food_df)

    avg_expiry_date_df = avg_expiry_date_df.withColumn("product_name", regexp_replace(trim(col("product_name")), "\\s+", " "))

    logger.info('-----------------------------------------------------')
    logger.info("Calculating average expiry date of food items")
    
    avg_expiry_date_df = avg_expiry_date_df.groupBy("category","sub_category","product_name").agg(
        F.min("avg_expiry_days").alias("avg_expiry_days")
    )
    
    # avg_expiry_date_df.write.parquet("./data/parquet/estimated_avg_expiry.parquet")
    avg_expiry_date_df.write.mode('overwrite').parquet("./data/formatted_zone/estimated_avg_expiry")
    