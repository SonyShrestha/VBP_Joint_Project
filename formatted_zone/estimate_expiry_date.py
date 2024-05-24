from pyspark import SparkConf
import logging 
import os 
import configparser
import json
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_replace, udf, lower, trim, monotonically_increasing_id, col, expr, lit, row_number
from pyspark.sql.types import IntegerType
from fuzzywuzzy import fuzz
from pyspark.sql.window import Window
import spacy
from pyspark.sql import SparkSession
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag


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

config_file_path_json = os.path.join(config_dir, "config.json")
with open(config_file_path_json) as f:
    config_json = json.load(f)


nlp = spacy.load("en_core_web_sm")



@udf(IntegerType())
def fuzzy_match_score(fuzzy_score_calc_method, fuzzy_threshold, s1, s2):
    word1 = word_tokenize(s1)
    # Part-of-speech tagging
    tagged = pos_tag(word1)
    # Extract Proper Nouns (NNP)
    nnp = [word for word, tag in tagged if tag == 'NNS' or tag == 'NN']
    
    s1 =' '.join(x for x in nnp)

    if fuzzy_score_calc_method == "partial_token_sort_ratio":
        score = fuzz.partial_token_sort_ratio(s1, s2)
    elif fuzzy_score_calc_method == "partial_token_set_ratio":
        score = fuzz.partial_token_set_ratio(s1, s2)
    elif fuzzy_score_calc_method == "ratio":
        score = fuzz.ratio(s1, s2)
    elif fuzzy_score_calc_method == "partial_ratio":
        score = fuzz.partial_ratio(s1, s2)

    if int(score) < int(fuzzy_threshold):
        score = 0
    return score



@udf(IntegerType())
def count_tokens(s1, s2):
    if s1 is None or s2 is None:
        return 0
    
    s1_tokens = set(s1.split())
    s2_tokens = set(s2.split())
    
    return len(s1_tokens.intersection(s2_tokens))


if __name__ == "__main__":
    gcs_config = config["GCS"]["credentials_path"]
    raw_bucket_name = config["GCS"]["raw_bucket_name"]
    formatted_bucket_name = config["GCS"]["formatted_bucket_name"]

    fuzzy_score_calc_method = config_json["product_matching"]["fuzzy_matching"]["score_calc_method"]
    fuzzy_threshold = config_json["product_matching"]["fuzzy_matching"]["threshold"]

    spark = SparkSession.builder \
    .appName("GCS Files Read") \
    .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("google.cloud.auth.service.account.json.keyfile", "C:\\UPC\\BDM\\Project\\VBP_Joint_Project\\gcs_config.json") \
    .getOrCreate()

    # Specify the path to the Parquet file
    cust_purachase = f'gs://{formatted_bucket_name}/customer_purchase*'
    cust_email = f'gs://{formatted_bucket_name}/customers*'
    expected_avg_expiry = 'gs://'+formatted_bucket_name+'/estimated_avg_expiry*'

    # Read the Parquet file into a DataFrame
    cust_purachase_df = spark.read.parquet(cust_purachase)
    cust_email_df = spark.read.parquet(cust_email)
    expected_avg_expiry_df = spark.read.parquet(expected_avg_expiry)

    cust_email_df = cust_email_df.select("customer_id","email_id")


    customer_purachase_df = cust_purachase_df.join(cust_email_df, 'customer_id', 'inner')
    customer_purachase_df = customer_purachase_df.select("id","customer_id","customer_name","email_id","product_name","unit_price","quantity","purchase_date")
    cust_purchase_dist_prod = customer_purachase_df.select("product_name").withColumnRenamed("product_name","product").dropDuplicates()


    fuzzy_match = expected_avg_expiry_df.crossJoin(cust_purchase_dist_prod)

    fuzzy_match = fuzzy_match.withColumn("score", fuzzy_match_score(lit(fuzzy_score_calc_method), lit(fuzzy_threshold), fuzzy_match["product_name"], fuzzy_match["product"]))
    windowSpec = Window.partitionBy("product_name").orderBy(col("score").desc())
    fuzzy_match_with_rnk = fuzzy_match.withColumn("row_number", row_number().over(windowSpec))
    fuzzy_match_result = fuzzy_match_with_rnk.filter((col("row_number") == 1) & (col("score") >= lit(fuzzy_threshold))).drop("row_number")

    fuzzy_match_result = fuzzy_match_result.select("product","avg_expiry_days")

    joined_df = customer_purachase_df.join(fuzzy_match_result,customer_purachase_df.product_name==fuzzy_match_result.product,"left").filter(col("avg_expiry_days")>0)

    joined_df = joined_df.select("customer_id","customer_name","email_id","product_name","unit_price","quantity","purchase_date","avg_expiry_days")

    joined_df = joined_df.withColumn("expected_expiry_date", expr("date_add(purchase_date, cast(ceil(avg_expiry_days/2) AS INT))"))

    joined_df.show()

    joined_df.write.mode('overwrite').parquet(f'gs://{formatted_bucket_name}/purchases_nearing_expiry')