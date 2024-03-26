import time
from random import randint, choice, getrandbits
import json
import csv

def generate_review(ID, business_name, rating, text, verified=True):
    review = {
        'ID': ID,
        'Business_name': business_name,
        'time': int(time.time() * 1000),
        'rating': rating,
        'text': text,
        'verified': 'Yes' if verified else 'No',
        'date': time.strftime('2023-%m-%d')
    }
    return review

def random_user_id():
    """Generate a pseudo-random user ID."""
    return str(getrandbits(64))

def random_business_name():
    """Return a random business name from a large list."""
    businesses = ['Fresh & Green Groceries',
            'Daily Harvest Market',
            'Organic Essentials',
            'Sunshine Grocery',
            'City Market',
            'The Grocery Basket',
            'Urban Fresh Foods',
            'Nature"s Pantry',
            'Field to Fork Market',
            'Harvest Moon Grocers',
            'Purely Organic Produce',
            'The Local Grocer',
            'Season"s Best Market',
            'EcoFood Supermarket',
            'Neighborhood Fresh Market']
    return choice(businesses)

def random_text():
    """Return a random review text from a large list of sentiments."""
    sentiments = [
    'Love the fresh produce!', 'Could use more variety in organic options.',
    "Great prices and friendly staff.",
    "Not satisfied with the freshness of perishables.",
    "Awesome selection of organic foods!",
    "The bakery section is a bit lacking.",
    "Best place for vegan groceries.",
    "Disappointed by the seafood selection.",
    "Excellent customer service!",
    "Could be cleaner, but the produce is top-notch.",
    "Found everything I needed and more!",
    "Checkout lines were too long.",
    "Impressed by the local produce selection.",
    "The store layout is confusing.",
    "Parking was a nightmare.",
    "They always have the best seasonal fruits.",
    "Meat and fish sections are well-maintained.",
    "The selection of locally sourced honey and jams is not only extensive but brings a taste of the region's best flavors directly to our table.",
    "I was pleasantly surprised by their commitment to sustainability, with eco-friendly packaging and a wide range of bulk purchase options reducing plastic waste.",
    "The staff went above and beyond when I inquired about gluten-free options, guiding me through the aisles and providing recommendations on products and brands.",
    "Their seasonal outdoor farmers' market is a must-visit, offering an exceptional variety of fresh produce, artisanal cheeses, and handmade goods from local vendors.",
    "I appreciate the store's effort in organizing cooking classes; it's a great way to engage with the community and learn new recipes using ingredients sold right there.",
    "The seafood counter offers an impressive array of fresh, sustainable options, and the knowledgeable staff can provide cooking tips and recipe ideas.",
    "Their in-house bakery produces some of the most delightful pastries and bread I've ever tasted – the early morning aroma of fresh baking is irresistible.",
    "I found their selection of international foods to be unparalleled, offering authentic ingredients from around the world that are hard to find elsewhere.",
    "I wish they had more cashiers on duty.",
    "Their loyalty program is really worth it.",
    "The deli section offers amazing sandwiches.",
    "Love their gluten-free options!",
    "The floral department is always a delight.",
    "Organic meat selection is unmatched.",
    "Wish there were more discounts.",
    "The cheese variety is incredible.",
    "Always out of my favorite items.",
    "The store brand products are top quality.",
    "Friendly and helpful pharmacy staff.",
    "The prepared foods section saves my dinners!",
    "More eco-friendly packaging needed.",
    "Excellent wine and spirits selection.",
    "The aisles are always clutter-free.",
    "Customer service could be more attentive.",
    "The wine and beer selection is curated with care, featuring local breweries and vineyards alongside well-known global brands, making it a go-to for any occasion.",
    "While the prices can be a bit higher than at big box stores, the quality and customer service offered here make it worth the extra cost.",
    "They've created a community bulletin board where local events and services are posted, fostering a strong sense of community engagement.",
    "The store's interior design and layout make shopping a pleasant experience, with wide aisles, clearly marked sections, and a welcoming atmosphere.",
    "I was disappointed to find that some of the produce, while labeled organic, did not seem to meet the usual standards of freshness and quality I expected.",
    "Their commitment to reducing food waste, through partnerships with local food banks and offering discounts on near-expiration items, is truly commendable.",
    "The variety of plant-based and vegan products is impressive, catering to dietary needs that are often overlooked in traditional grocery stores.",
    "It's refreshing to see a grocery store that places such a high value on customer feedback, with suggestion boxes and responsive management.",
    "The butcher counter is a standout, offering custom cuts of meat that are always fresh and sourced from ethical farms.",
    "I had an issue with a product, and the way the customer service team handled my complaint was exemplary, offering a refund and a coupon for future use.",
    "A go-to for specialty gourmet products.",
    "Prices are a bit steep for everyday items.",
    "The freshness guarantee is reassuring.",
    "Needs a better selection of non-dairy milk.",
    "The loyalty app is super convenient.",
    "Checkout is seamless with self-service options.",
    "They offer great cooking classes and demos.",
    "Pet-friendly shopping is a plus.",
    "The health and beauty section is lacking.",
    "Fresh sushi made daily is a treat.",
    "Could improve on the organic snack options.",
    "Bulk bin section is a money-saver.",
    "The parking lot is well-lit and safe.",
    "Online ordering process is smooth.",
    "In-store cafe makes shopping a pleasure.",
    "Free samples are always appreciated!",
    "Needs more checkout staff during peak hours.",
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
    "Their online shopping and home delivery service have been a game-changer, especially with the intuitive app that makes ordering easy and efficient.",
    "Despite its many positives, the store could improve by extending its hours on weekends to accommodate those with busy work schedules."
    ]
    return choice(sentiments)

def generate_reviews(n=500):
    reviews = []
    for _ in range(n):
        ID = random_user_id()
        business_name = random_business_name()
        rating = randint(1, 5)
        text = random_text()
        verified = choice([True, False])  # Randomly choose verified status
        review = generate_review(ID, business_name, rating, text, verified)
        reviews.append(review)
    return reviews


def save_to_json(reviews, filename="/home/pce/Pictures/business_reviews.json"):
    with open(filename, "w") as f:
        json.dump(reviews, f, indent=2)

def save_to_csv(reviews, filename="/home/pce/Pictures/business_reviews.csv"):
    fieldnames = ['ID', 'Business_name', 'time', 'rating', 'text', 'verified', 'date']
    with open(filename, mode="w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for review in reviews:
            writer.writerow(review)

if __name__ == "__main__":
    reviews = generate_reviews(500)
    save_to_json(reviews)
    save_to_csv(reviews)
    print("Generated and saved Business reviews completely.")


