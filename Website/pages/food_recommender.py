import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, concat_ws, lower
from pyspark.sql.types import StringType
import re
from datetime import datetime
import streamlit as st
from dotenv import load_dotenv
import json
from transformers import pipeline, AutoTokenizer, AutoModelForCausalLM
import torch
import logging
import configparser

# Set environment variables
# os.environ["PYSPARK_PYTHON"] = "/home/pce/anaconda3/envs/spark_env/bin/python3.11"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/pce/anaconda3/envs/spark_env/bin/python3.11"
# os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

load_dotenv()
# Set page config for a better appearance
# st.set_page_config(page_title="Food Recommender System", layout="wide")

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

# Get base directory
root_dir = os.path.abspath(os.path.join(os.getcwd()))

# Specify the path to config file
config_file_path = os.path.join(root_dir, "config.ini")
config = configparser.ConfigParser()
config.read(config_file_path)

config_file_path_json = os.path.join(root_dir, "config.json")
with open(config_file_path_json) as f:
    config_json = json.load(f)



def create_spark_session():
    gcs_config = config["GCS"]["credentials_path"]
    spark = SparkSession.builder \
        .appName("RecipeProcessing") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_config) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def clean_ingredients(ingredient_list):
    ingredient_string = ', '.join(ingredient_list)
    cleaned_ingredients = re.sub(r'\d+\s*(g|oz|ml|tsp|tbs|cups|pint|quart|l|lb|kg|teaspoon|tablespoon|medium|cup|/|-)?\s*|¼\s*|½\s*|¾\s*|to serve|Handful\s*', '', ingredient_string, flags=re.IGNORECASE)
    cleaned_ingredients = re.sub(r'[\s,-]*$', '', cleaned_ingredients).strip()
    return cleaned_ingredients

clean_ingredients_udf = udf(clean_ingredients, StringType())

def preprocess_data(input_path):
    spark = create_spark_session()
    try:
        df = spark.read.parquet(input_path)
        
        # Convert ingredients array to a single string
        df = df.withColumn("ingredients_string", concat_ws(", ", col("ingredients")))
        
        # Apply lower function and clean_ingredients function
        processed_df = df.withColumn("clean_ingredients", clean_ingredients_udf(col("ingredients"))) \
                         .withColumn("ingredients_lower", lower(col("ingredients_string"))) \
                         .drop("ingredients", "ingredients_string")
        
        return processed_df
    except Exception as e:
        print(f"Error during processing: {e}")
        return None

def setup_langchain():
    model_id = "meta-llama/Meta-Llama-3-8B"
    # Create a pipeline for text generation
    text_generator = pipeline("text-generation", model=model_id, model_kwargs={"torch_dtype": torch.bfloat16}, device_map="auto")
                              
    def generate_recipe(ingredients):
        prompt = f"As a food recommender, create different delicious food from these ingredients: {ingredients}. Include the food name, detailed steps, and estimated time."
        response = text_generator(prompt, max_length=500, num_return_sequences=1)
        return response[0]['generated_text']
    
    return generate_recipe

generate_recipe = setup_langchain()

def find_or_generate_recipes(processed_df, ingredients):
    pattern = '|'.join([f"(?i){re.escape(ingredient)}" for ingredient in ingredients])
    matched_recipes = processed_df.filter(processed_df.clean_ingredients.rlike(pattern))
    if matched_recipes.count() > 0:
        recipes = [{"Food name": row.food_name, "Ingredients": row.clean_ingredients, "Description": row.description} for row in matched_recipes.collect()]
        return recipes
    else:
        generated_recipe = generate_recipe(', '.join(ingredients))
        return [{"generated_recipe": generated_recipe}]

def initialize():
    input_path = "gs://spicy_1/mealdb_*"
    processed_df = preprocess_data(input_path)
    return processed_df


def food_recommender():
    processed_df = initialize()

    st.title("Food Generator")

    user_ingredients = st.text_input("Enter ingredients, separated by commas", "rice, tomatoes")
    ingredients_list = [ingredient.strip() for ingredient in user_ingredients.split(',')]

    if st.button("Generate Recipe"):
        if processed_df is not None:
            recipes_or_generated = find_or_generate_recipes(processed_df, ingredients_list)
            if 'generated_recipe' in recipes_or_generated[0]:
                st.write("Generated Recipe:")
                st.write(recipes_or_generated[0]['generated_recipe'])
            else:
                st.write("Recipes Found:")
                for recipe in recipes_or_generated:
                    st.subheader(recipe['Food name'])
                    st.write("Description:", recipe['Description'])
        else:
            st.error("Failed to load data.")
            
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