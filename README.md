# SpicyBytes P1 Delivery : Landing Zone

## Introduction
SpicyBytes is an innovative food and grocery management platform aimed at reducing food wastage by offering a sustainable shopping and selling experience for groceries nearing their expiration date. 

## Structure of Data - Add Tree (s not working)
E
├───.github
│   └───workflows
├───data
│   └───raw
├───landing_zone
│   ├───collectors
│   │   ├───approved_food_uk
│   │   ├───big_basket
│   │   ├───catalonia_establishment_location
│   │   ├───customers
│   │   ├───eat_by_date
│   │   ├───Flipkart
│   │   ├───meal_db
│   │   └───OCR
│   │       ├───images
│   │       └───output
│   └───synthetic
│       ├───customer_location
│       ├───customer_purchase
│       ├───sentiment_reviews
│       └───supermarket_products
└───recommendation_system


## Data Sources

The `data` folder stores the raw data scraped from the data sources present in the `landing_zone`.

The `landing zone` directory is used to generate the data required. The `collectors` stores the scripts to extract the web sources provided in the `config.ini` file. 
The `synthetic` directory uses a combination of data from `collectors` to synthetically produce the required dataset.

## How to run the code

## Data Pipeline
