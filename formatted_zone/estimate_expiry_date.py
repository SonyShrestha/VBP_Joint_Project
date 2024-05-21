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


@udf(IntegerType())
def spacy_match_score(spacy_filter, spacy_threshold, s1, s2):
    s1 = nlp(s1)
    s2 = nlp(s2)

    s1_verbs = " ".join([token.lemma_ for token in s1 if token.pos_ == spacy_filter])
    s2_verbs = " ".join([token.lemma_ for token in s1 if token.pos_ == spacy_filter])

    doc1 = nlp(s1_verbs)
    doc2 = nlp(s2_verbs)

    # Calculate similarity
    score = doc1.similarity(doc2)

    if score < spacy_threshold:
        score = 0
    return score



if __name__ == "__main__":
    gcs_config = config["GCS"]["credentials_path"]
    raw_bucket_name = config["GCS"]["raw_bucket_name"]
    formatted_bucket_name = config["GCS"]["formatted_bucket_name"]

    fuzzy_score_calc_method = config_json["product_matching"]["fuzzy_matching"]["score_calc_method"]
    fuzzy_threshold = config_json["product_matching"]["fuzzy_matching"]["threshold"]

    spark = SparkSession.builder \
        .appName("Estimate Expiry Date") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_config) \
        .getOrCreate()
    
    # Specify the path to the Parquet file
    cust_purachase = f'gs://{formatted_bucket_name}/customer_purchase*'

    cust_email = f'gs://{formatted_bucket_name}/customers*'

    expected_avg_expiry = 'gs://'+formatted_bucket_name+'/estimated_avg_expiry*'

    # Read the Parquet file into a DataFrame
    cust_purachase_df = spark.read.parquet(cust_purachase)
    cust_email_df = spark.read.parquet(cust_email)
    cust_email_df = cust_email_df.select("customer_id","email_id")


    customer_purachase_df = cust_purachase_df.join(cust_email_df, 'customer_id', 'inner')
    customer_purachase_df = customer_purachase_df.select("id","customer_id","customer_name","email_id","product_name","unit_price","quantity","purchase_date")

    customer_purachase_df = customer_purachase_df.withColumn("original_product_name", customer_purachase_df["product_name"])

    customer_purachase_df = customer_purachase_df.withColumn("id", monotonically_increasing_id())
    customer_purachase_df = customer_purachase_df.select("id","customer_id","customer_name","email_id","product_name","unit_price","quantity","purchase_date","original_product_name")\
            .withColumn("product_name", lower(regexp_replace(customer_purachase_df["product_name"], "[^a-zA-Z ]", "")))\
            .withColumn("product_name", regexp_replace(trim(col("product_name")), "\\s+", " "))
    
    expected_avg_expiry_df = spark.read.parquet(expected_avg_expiry)
    expected_avg_expiry_df = expected_avg_expiry_df.select("product_name","avg_expiry_days")\
            .withColumn("product_name", lower(regexp_replace(expected_avg_expiry_df["product_name"], "[^a-zA-Z ]", "")))\
            .withColumn("product_name", regexp_replace(trim(col("product_name")), "\\s+", " "))
    expected_avg_expiry_df = expected_avg_expiry_df.withColumnRenamed("product_name", "product_in_avg_expiry_file")

    available_products = [row["product_in_avg_expiry_file"] for row in expected_avg_expiry_df.select("product_in_avg_expiry_file").collect()]

    joined_df = customer_purachase_df.crossJoin(expected_avg_expiry_df)
    
    filtered_df = joined_df.select("id","customer_id","customer_name","email_id","product_name","unit_price","quantity","purchase_date","original_product_name","product_in_avg_expiry_file","avg_expiry_days")

    filtered_df = filtered_df.withColumn("score", fuzzy_match_score(lit(fuzzy_score_calc_method), lit(fuzzy_threshold), filtered_df["product_name"], filtered_df["product_in_avg_expiry_file"]))

    filtered_df = filtered_df.filter(filtered_df.score != 0)

    filtered_df = filtered_df.withColumn("token_count", count_tokens(filtered_df["product_name"], filtered_df["product_in_avg_expiry_file"]))

    windowSpec = Window.partitionBy("id") \
                  .orderBy(filtered_df["score"].desc(), filtered_df["token_count"].desc())

    # Add a row number column
    df_with_rn = filtered_df.withColumn("row_number", row_number().over(windowSpec))

    # Filter rows where row number is 1 (which corresponds to the row with the maximum fuzzy score for each product)
    df_with_rn = df_with_rn.filter(df_with_rn["row_number"] == 1).drop("row_number", "product_name", "token_count")
    df_with_rn = df_with_rn.withColumnRenamed("original_product_name", "product_name")

    df_with_rn = df_with_rn.withColumn("expected_expiry_date", expr("date_add(purchase_date, cast(ceil(avg_expiry_days/2) AS INT))"))

    # df_with_rn.write.mode('overwrite').parquet("./data/formatted_zone/purchases_nearing_expiry")
    df_with_rn.write.mode('overwrite').parquet(f'gs://{formatted_bucket_name}/purchases_nearing_expiry')