from faker import Faker
import pandas as pd
import numpy as np
import os
from zipfile import ZipFile
import os
import requests
import logging
import configparser


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


def generate_customer_purchase(num_customers, num_purchases):
    # Read products file from Big Basket Dataset
    product_df = pd.read_csv(os.path.join(config_dir,'landing_zone/collectors/big_basket/BigBasket Products.csv'))

    # Initialize Faker
    fake = Faker()

    # Generate fake customer information
    customer_ids = range(1, num_customers + 1)
    customer_names = [fake.name() for _ in range(num_customers)]
    customer_df = pd.DataFrame({'customer_id': customer_ids, 'customer_name': customer_names})
    customer_df.to_csv("customers.csv", index=False)

    # Generate synthetic purchase data
    purchase_data = []
    for _ in range(num_purchases):
        customer_id = np.random.choice(customer_ids)
        customer_name = customer_names[customer_id - 1]
        product_index = np.random.randint(len(product_df))
        product_id = product_df.loc[product_index, 'index']
        product_name = product_df.loc[product_index, 'product']
        product_category = product_df.loc[product_index, 'category']
        product_subcategory = product_df.loc[product_index, 'sub_category']
        product_brand = product_df.loc[product_index, 'brand']
        unit_price = product_df.loc[product_index, 'sale_price']
        quantity = np.random.randint(1, 5)  # Adjust quantity range as needed
        purchase_date = fake.date_between(start_date="-1y", end_date="now")
        purchase_data.append({'customer_id': customer_id,
                            'customer_name': customer_name,
                            'product_id': product_id,
                            'product_name': product_name,
                            'product_category': product_category,
                            'product_subcategory': product_subcategory,
                            'product_brand': product_brand,
                            'unit_price': unit_price,
                            'quantity': quantity,
                            'purchase_date':purchase_date})

    # Create DataFrame for purchase data
    purchase_df = pd.DataFrame(purchase_data)
    purchase_df.to_csv("customer_purchase.csv", index=False)



if __name__ == "__main__":
    num_of_customers = int(config["CUSTOMER_PURCHASE"]["num_of_customers"])
    num_of_purchases = int(config["CUSTOMER_PURCHASE"]["num_of_purchases"])
    logger.info('-----------------------------------------------------')
    logger.info("Generating fake dataset for customers and customer purchases")
    generate_customer_purchase(num_of_customers, num_of_purchases)
