import pandas as pd
import random
import os
import configparser
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

# Get the path to the parent parent directory
config_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir, os.pardir, os.pardir))

# Specify the path to config file
config_file_path = os.path.join(config_dir, "config.ini")

config = configparser.ConfigParser()
config.read(config_file_path)



def extract_customer_location(customer_file, location_file):
    # Read customer and location data into pandas DataFrames
    customers_df = pd.read_csv(customer_file)
    locations_df = pd.read_csv(location_file)

    # Shuffle the locations DataFrame to randomize the selection
    locations_df = locations_df.sample(frac=1).reset_index(drop=True)

    # Select a random location for each customer
    random_locations = [random.choice(locations_df['location_id']) for _ in range(len(customers_df))]

    # Create a new DataFrame with customer IDs and their corresponding random locations
    customer_location = pd.DataFrame({'customer_id': customers_df['customer_id'], 'location_id': random_locations})

    return customer_location



if __name__ == "__main__":
    raw_data_dir = config["COMMON"]["raw_data_dir"]
    num_of_reviews = int(config["SENTIMENT_REVIEWS"]["num_of_reviews"])

    # Save the paired data to a new CSV file
    customer_file = os.path.join(raw_data_dir,'customers.csv')
    location_file = os.path.join(raw_data_dir,'location.csv')
    output_file = os.path.join(raw_data_dir,'customer_location.csv')

    logger.info('-----------------------------------------------------')
    logger.info("Generating data for Customer Location")
    customer_location_df=extract_customer_location(customer_file, location_file)
    customer_location_df.to_csv(output_file, index=False)
