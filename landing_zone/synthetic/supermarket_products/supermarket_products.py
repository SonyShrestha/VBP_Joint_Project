import csv
import json
import random
import configparser
import os
import logging



logger = logging.getLogger()

# Load configuration
config_path = os.path.join(os.getcwd(), '../../..', 'config.ini')
config = configparser.ConfigParser()
config.read(config_path)
logging.info(f'Configuration loaded from {config_path}')


# Base directory for files
raw_data_dir = config.get('COMMON', 'raw_data_dir')

number_of_products = config.getint('SUPERMARKET_PRODUCT_PARAMS', 'number_of_products')
quantity_min = config.getint('SUPERMARKET_PRODUCT_PARAMS', 'quantity_min')
quantity_max = config.getint('SUPERMARKET_PRODUCT_PARAMS', 'quantity_max')

# Reading file paths from config file
products_json = os.path.join(raw_data_dir,'flipkarts_products.json' )
stores_csv = os.path.join(raw_data_dir, 'establishments_catalonia.csv')
output_csv = os.path.join(raw_data_dir, 'assigned_products.csv')

# Load JSON data from file
with open(products_json, 'r') as file:
    products = json.load(file)

# Load CSV data from file and filter rows
supermarkets = []
with open(stores_csv, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        if "supermercat" in row['Activity_description'].lower():
            supermarkets.append(row)

# Assign products randomly
assigned_products = []
for supermarket in supermarkets:
    selected_products = random.sample(products, number_of_products)
    for product in selected_products:
        assigned_products.append({
            "store_id": supermarket['Id'],
            "store_name": supermarket['Commercial_name'],
            "product_id": product['product_id'],
            "product_name": product['name'],
            "manufacture_date": product['manufacturing_date'],
            "expiry_date": product['expiry_date'],
            "quantity": random.randint(quantity_min, quantity_max)  # Random quantity between 1 and 100
        })

# Output to CSV
try:
    with open(output_csv, 'w', newline='') as file:
        fieldnames = ['store_id', 'store_name', 'product_id', 'product_name', 'manufacture_date', 'expiry_date', 'quantity']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for item in assigned_products:
            writer.writerow(item)
    logger.info(f"Data has been processed and output to {output_csv}.")
except Exception as e:
    logger.error("Failed to write data to CSV", exc_info=True)
