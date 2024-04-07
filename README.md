# SpicyBytes P1 Delivery : Landing Zone

## Introduction
SpicyBytes is an innovative food and grocery management platform aimed at reducing food wastage by offering a sustainable shopping and selling experience for groceries nearing their expiration date. 

## Structure of Data in the repository

```plaintext
├── .github
│   └── workflows
│       └── project.yml
├── data
│   └── raw
├── landing_zone
│   ├── collectors
│   │   ├── approved_food_uk
│   │   ├── big_basket
│   │   ├── catalonia_establishment_location
│   │   ├── customers
│   │   ├── eat_by_date
│   │   ├── Flipkart
│   │   ├── meal_db
│   │   │   └── mealscraper
│   │   └── OCR
│   │       ├── images
│   │       └── output
│   └── synthetic
│       ├── customer_location
│       ├── customer_purchase
│       ├── sentiment_reviews
│       └── supermarket_products
└── recommendation_system

```

## Data Sources

The `data` folder stores the raw data scraped using the scripts present in the `landing_zone`. The `landing_zone` consists of 2 types of data generation scripts:
- `collectors` consist of data sources that have either been scraped or extracted through API requests from the corresponding webpages.
- `synthetic` directory consists of data generated synthetically; using a composite of collected data sources and fake data generated using the python [Faker](https://pypi.org/project/Faker/0.7.4/) library.


## How to run the code
