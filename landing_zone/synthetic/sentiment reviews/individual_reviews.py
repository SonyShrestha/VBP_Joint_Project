
import time
from random import randint, getrandbits, choice
import json
import csv
from faker import Faker
import pandas as pd
import os
import configparser
import logging

fake = Faker()

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

# Splitting sentiments into positive and negative for simplicity
positive_sentiments = [
    "Love the fresh produce!",
    "Excellent packaging, ensuring all the delicate greens arrived in perfect condition.",
    "Some of the best organic tomatoes I've tasted. Will definitely order again!",
    "Great prices and friendly staff.",
    "Awesome selection of organic foods!",
    "Best place for vegan groceries.",
    "Excellent customer service!",
    "Could be cleaner, but the produce is top-notch.",
    "Found everything I needed and more!",
    "Impressed by the local produce selection.",
    "They always have the best seasonal fruits.",
    "Meat and fish sections are well-maintained.",
    "The selection of locally sourced honey and jams is not only extensive but brings a taste of the region's best flavors directly to our table.",
    "I was pleasantly surprised by their commitment to sustainability, with eco-friendly packaging and a wide range of bulk purchase options reducing plastic waste.",
    "The staff went above and beyond when I inquired about gluten-free options, guiding me through the aisles and providing recommendations on products and brands.",
    "Their seasonal outdoor farmers' market is a must-visit, offering an exceptional variety of fresh produce, artisanal cheeses, and handmade goods from local vendors.",
    "I appreciate the store's effort in organizing cooking classes; it's a great way to engage with the community and learn new recipes using ingredients sold right there.",
    "The seafood counter offers an impressive array of fresh, sustainable options, and the knowledgeable staff can provide cooking tips and recipe ideas.",
    "Their in-house bakery produces some of the most delightful pastries and bread I've ever tasted - the early morning aroma of fresh baking is irresistible.",
    "I found their selection of international foods to be unparalleled, offering authentic ingredients from around the world that are hard to find elsewhere.",
    "Their loyalty program is really worth it.",
    "The deli section offers amazing sandwiches.",
    "Love their gluten-free options!",
    "The floral department is always a delight.",
    "Organic meat selection is unmatched.",
    "The cheese variety is incredible.",
    "The store brand products are top quality.",
    "Friendly and helpful pharmacy staff.",
    "The prepared foods section saves my dinners!",
    "Excellent wine and spirits selection.",
    "The aisles are always clutter-free.",
    "The wine and beer selection is curated with care, featuring local breweries and vineyards alongside well-known global brands, making it a go-to for any occasion.",
    "While the prices can be a bit higher than at big box stores, the quality and customer service offered here make it worth the extra cost.",
    "They've created a community bulletin board where local events and services are posted, fostering a strong sense of community engagement.",
    "The store's interior design and layout make shopping a pleasant experience, with wide aisles, clearly marked sections, and a welcoming atmosphere.",
    "Their commitment to reducing food waste, through partnerships with local food banks and offering discounts on near-expiration items, is truly commendable.",
    "The variety of plant-based and vegan products is impressive, catering to dietary needs that are often overlooked in traditional grocery stores.",
    "It's refreshing to see a grocery store that places such a high value on customer feedback, with suggestion boxes and responsive management.",
    "The butcher counter is a standout, offering custom cuts of meat that are always fresh and sourced from ethical farms.",
    "I had an issue with a product, and the way the customer service team handled my complaint was exemplary, offering a refund and a coupon for future use.",
    "A go-to for specialty gourmet products.",
    "The freshness guarantee is reassuring.",
    "The loyalty app is super convenient.",
    "Checkout is seamless with self-service options.",
    "They offer great cooking classes and demos.",
    "Pet-friendly shopping is a plus.",
    "Fresh sushi made daily is a treat.",
    "Bulk bin section is a money-saver.",
    "The parking lot is well-lit and safe.",
    "Online ordering process is smooth.",
    "In-store cafe makes shopping a pleasure.",
    "Free samples are always appreciated!",
    "Their holiday-themed events bring the community together, offering tastings, special deals, and festive decorations that enhance the shopping experience.",
    "The inclusion of a small cafe within the store is perfect for a quick break during shopping trips, offering excellent coffee and snacks.",
    "Although the selection of products is generally outstanding, I've noticed that staple items can be out of stock, which can be inconvenient for regular shopping.",
    "The loyalty program is not only rewarding but also includes personalized discounts based on shopping history, which I find to be a thoughtful touch.",
    "The store's effort to feature and promote products from women-owned and minority-owned businesses is a great way to support diverse entrepreneurs.",
    "They offer a remarkable range of dietary-specific products, from keto and paleo to gluten-free and sugar-free, making healthy living more accessible.",
    "One of the best aspects is the freshness and variety of the salad bar, offering a plethora of options for healthy and quick meal solutions.",
    "The kids' play area is a thoughtful addition, allowing parents to shop in peace while their children are safely entertained.",
    "I was impressed by the store's initiative to offer educational tours to schools, teaching children about nutrition and the importance of local farming.",
    "The availability of rare and exotic fruits and vegetables encourages culinary exploration and creativity in the kitchen.",
    "Their online shopping and home delivery service have been a game-changer, especially with the intuitive app that makes ordering easy and efficient."]

negative_sentiments= [ "Could use more variety in organic options.",
    "Not satisfied with the freshness of perishables.",
    "The bakery section is a bit lacking.",
    "Disappointed by the seafood selection.",
    "Checkout lines were too long.",
    "The store layout is confusing.",
    "Parking was a nightmare.",
    "I wish they had more cashiers on duty.",
    "Wish there were more discounts.",
    "Always out of my favorite items.",
    "More eco-friendly packaging needed.",
    "Customer service could be more attentive.",
    "Needs a better selection of non-dairy milk.",
    "Could improve on the organic snack options.",
    "Needs more checkout staff during peak hours.",
    "I was disappointed to find that some of the produce, while labeled organic, did not seem to meet the usual standards of freshness and quality I expected.",
    "Despite its many positives, the store could improve by extending its hours on weekends to accommodate those with busy work schedules.",
    "The health and beauty section is lacking.",
    "The fruit arrived overripe and unusable, a complete waste of money.",
    "My order was missing several items, and customer service was slow to respond.",
    "Received wilted vegetables that looked nothing like the online photos.",
    "The packaging was inadequate, leading to bruised and damaged goods upon arrival.",
    "Delivery was delayed multiple times without any communication from the seller.",
    "Found a foreign object in my food package, which is completely unacceptable.",
    "The quality of the meat was poor, with a lot of fat and gristle.",
    "Prices have skyrocketed recently without any noticeable improvement in quality or service.",
    "The description promised organic produce, but the items I received were clearly not.",
    "Attempted to return a spoiled product but the process was complicated and frustrating.",
    "The freshness guarantee seemed promising, but the reality did not live up to the hype.",
    "Customer service promised a refund that I never received.",
    "The app is glitchy and makes ordering a frustrating experience.",
    "Many products were out of stock, significantly limiting the variety available.",
    "Received an incorrect order and had to go through a lengthy process to get the right items.",
    "The 'next day delivery' took nearly a week, ruining my meal planning.",
    "Found the website difficult to navigate, with many broken links and outdated information.",
    "The product descriptions are misleading, giving a false impression of the item's quality.",
    "Charged for items that were never delivered, and it's been a hassle to get a refund.",
    "The subscription service is difficult to cancel, with customer service being unresponsive.",
    "Advertised discounts were not applied at checkout, leading to overcharging.",
    "The selection is not as curated as it claims to be, with many low-quality items.",
    "Received expired products but the company refused to acknowledge or rectify the issue.",
    "The estimated delivery times are never accurate, always expect delays.",
    "Packaging is often excessive and not environmentally friendly, contrary to their green claims.",
    "Tried to give feedback through their survey, but it seems like no one actually reads it.",
    "The produce selection is mediocre at best, you're better off going to a local farmer's market.",
    "Claims of 'farm to table' freshness seem exaggerated based on what I received.",
    "The specialty items are overpriced for what you actually get.",
    "There's a lack of transparency about where the food is sourced from.",
    "Customer loyalty seems undervalued, with no incentives or rewards for repeat purchases.",
    "The quality control on perishable goods is lacking, as evidenced by inconsistent product quality."]



def generate_review(ID, user_name, rating, text, verified=True):
    review = {
        'ID': ID,
        'user_name': user_name,
        # 'seller_name': seller_name,
        'time': int(time.time() * 1000),
        'rating': rating,
        'text': text,
        'verified': 'Yes' if verified else 'No',
        'date': time.strftime('%Y-%m-%d')
    }
    return review



def random_user_id(raw_data_dir):
    customer_df = pd.read_csv(os.path.join(raw_data_dir,'customers.csv'))
    random_row_index = customer_df.sample(n=1).index[0]

    # Select the random row using iloc
    random_row = customer_df.iloc[random_row_index]
    return str(random_row["customer_id"]), str(random_row["customer_name"])



def random_text_and_rating():
    """Return a random review text and corresponding rating."""
    # Decide first if the review will be positive or negative
    if randint(0, 1):  # Randomly choose between positive (1) and negative (0)
        text = choice(positive_sentiments)
        rating = randint(3, 5)  # Positive reviews have higher ratings
    else:
        text = choice(negative_sentiments)
        rating = randint(1, 3)  # Negative reviews have lower ratings
    return text, rating


def generate_reviews(n,raw_data_dir):
    reviews = []
    for _ in range(n):
        ID, user_name = random_user_id(raw_data_dir)
        #user_name = random_name()  # Generating fake user names
        # seller_name = random_name()  # Generating fake seller names
        text, rating = random_text_and_rating()  # Get both text and rating
        verified = fake.boolean()  # Randomly choose verified status using Faker
        review = generate_review(ID, user_name, rating, text, verified)
        reviews.append(review)
    return reviews



def save_to_json(reviews, filename):
    filename = os.path.join(raw_data_dir,'individual_reviews.json')
    with open(filename, "w") as f:
        json.dump(reviews, f, indent=2)



def save_to_csv(reviews, filename):
    filename = os.path.join(raw_data_dir,'individual_reviews.csv')
    fieldnames = ['ID', 'user_name', 'time', 'rating', 'text', 'verified', 'date']
    with open(filename, mode="w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for review in reviews:
            writer.writerow(review)



if __name__ == "__main__":
    raw_data_dir = config["COMMON"]["raw_data_dir"]
    num_of_reviews = int(config["SENTIMENT_REVIEWS"]["num_of_reviews"])

    logger.info('-----------------------------------------------------')
    logger.info("Generating synthetic data for Individual Sentiment Reviews")

    reviews = generate_reviews(num_of_reviews,raw_data_dir)
    save_to_json(reviews, raw_data_dir)
    
    logger.info('-----------------------------------------------------')
    logger.info("Generated and saved individual seller reviews completely")
