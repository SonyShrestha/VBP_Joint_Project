from faker import Faker
import pandas as pd
import numpy as np
import os

current_directory = os.getcwd()
print(current_directory)
# Load your product dataset (replace 'product_data.csv' with your actual filename)
product_df = pd.read_csv('dataset/BigBasket_Products.csv')

# Initialize Faker
fake = Faker()

# Generate fake customer information
num_customers = 10000  # Adjust as needed
customer_ids = range(1, num_customers + 1)
customer_names = [fake.name() for _ in range(num_customers)]
customer_df = pd.DataFrame({'customer_id': customer_ids, 'customer_name': customer_names})
customer_df.to_csv("landing_zone/customers.csv", index=False)

# Generate synthetic purchase data
num_purchases = 100000  # Adjust as needed
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
purchase_df.to_csv("landing_zone/customer_purchase.csv", index=False)

# Display the synthetic purchase dataset
print(purchase_df.head())
