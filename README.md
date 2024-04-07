# SpicyBytes P1 Delivery : Landing Zone

## Introduction
SpicyBytes is an innovative food and grocery management platform aimed at reducing food wastage by offering a sustainable shopping and selling experience for groceries nearing their expiration date. 

## Structure of Data in the repository

```plaintext
├───.github
│   └───workflows
├───dags
├───data
│   └───raw
├───landing_zone
│   ├───collectors
│   │   ├───approved_food_uk
│   │   │   └───approvedfood_scraper
│   │   │       └───approvedfood_scraper
│   │   ├───big_basket
│   │   ├───catalonia_establishment_location
│   │   ├───customers
│   │   ├───eat_by_date
│   │   ├───flipkart
│   │   │   └───JSON_files
│   │   ├───meal_db
│   │   │   └───mealscraper
│   │   │       └───mealscraper
│   │   └───OCR
│   │       ├───images
│   │       └───output
│   └───synthetic
│       ├───customer_location
│       ├───customer_purchase
│       ├───sentiment_reviews
│       └───supermarket_products
└───readme_info
```

## Data Sources

The `data` folder stores the raw data scraped using the scripts present in the `landing_zone`. The `landing_zone` consists of 2 types of data generation scripts:
- `collectors` consist of data sources that have either been scraped or extracted through API requests from the corresponding webpages.
- `synthetic` directory consists of data generated synthetically; using a composite of collected data sources and fake data generated using the python [Faker](https://pypi.org/project/Faker/0.7.4/) library.


## How to run the code

- To execute the program, clone the repository.
- Install the requirements using `pip install -r requirements.txt`.
- **Configure Airflow** : Set up your Airflow environment by configuring settings such as the executor, database, and authentication method. Refer to the Airflow documentation for detailed instructions on configuring Airflow.
- Verify that `Apache Airflow` is installed in your local machine and is running.
- Start the Airflow webserver and scheduler using the following commands:
  ```
  airflow webserver --port 8080
  airflow scheduler
  ```
- **Access the Airflow UI**: Open your web browser and navigate to http://localhost:8080.
- Enable your DAG.

The `collector.py` DAG collects data on a monthly basis, while the `synthetic.py` DAG collects data on a daily basis.


## High Level Data Architecture

![High Level Architecture](./readme_info/three-tier.PNG)

The proposed high level architecture is employed for the P1 delivery methodology.


