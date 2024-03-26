import time
from random import randint, choice, getrandbits
import json
import csv
from faker import Faker

fake = Faker()

def generate_review(ID, user_name, rating, text, verified=True):
    review = {
        'ID': ID,
        'user_name': user_name,
        # 'seller_name': seller_name,  # Now includes seller_name
        'time': int(time.time() * 1000),
        'rating': rating,
        'text': text,
        'verified': 'Yes' if verified else 'No',
        'date': time.strftime('%Y-%m-%d')
    }
    return review

def random_user_id():
    """Generate a pseudo-random user ID."""
    return str(getrandbits(64))

def random_name():
    """Generate a random first and last name using Faker."""
    return fake.name()

def random_text():
    """Return a random review text focused on individual sellers."""
    sentiments = [
        "The strawberries were fresh and juicy, delivered right on time!",
"Excellent packaging, ensuring all the delicate greens arrived in perfect condition.",
"Some of the best organic tomatoes I’ve tasted. Will definitely order again!",
"I had an issue with my order, but it was resolved quickly. Great customer service!",
"Offers incredible variety. The heirloom carrots were a hit at dinner.",
"Received a lovely note with my berry order. It’s those personal touches that make a difference.",
"The order was a bit delayed in shipping, but the quality of the products made up for it.",
"Absolutely delighted with the freshness. The kale was superb.",
"Reminded me of summer days. The peaches were perfectly ripe.",
"The exotic fruits were a hit in my tropical fruit salad. The passion fruit was exceptional.",
"Fantastic selection of artisan cheeses, each with a unique taste profile.",
"The fresh fish selection is unparalleled; it's like having a fish market at your fingertips.",
"Avocados were at the perfect stage of ripeness. Made the best guacamole!",
"The freshness of the herbs is noticeable. My dishes have never tasted better.",
"Blueberries were bursting with flavor, ideal for my morning smoothies.",
"The quality of the grass-fed beef is outstanding. It's now a staple in our home.",
"A wonderful assortment of organic nuts and seeds, great for snacking.",
"The variety of mushrooms available is impressive. Made for an exquisite risotto.",
"Ordered fresh oysters for a special dinner; they were a hit, fresh and delicious.",
"The packaging of leafy greens kept them fresh for days longer than expected.",
"Honeycrisp apples were crunchy and sweet, exactly how I like them.",
"The vegan chocolate selection is tempting and delicious, with so many options.",
"Their commitment to sustainable fishing practices makes me a loyal customer.",
"Ordered a variety of citrus fruits; each was juicy and packed with flavor.",
"The spice selection added so much life to my cooking. Fresh and aromatic!",
"Loved the quick delivery of fresh, organic milk. It tastes so much better.",
"The seasonal squash variety was a delightful addition to our autumn meals.",
"Received a beautifully arranged basket of mixed fruits, perfect for gifting.",
"The organic chicken was tender and flavorful, a significant difference from store-bought.",
"The freshness of the gourmet pasta made our Italian dinner night special.",
"Cherries were sweet and ripe, without a single bad one in the batch.",
"The assortment of fresh bread is a carb lover's dream, so soft and tasty.",
"Discovered new varieties of apples I'd never heard of, and each was a treat.",
"The freshness and quality of your leafy greens have turned my salads into a main course.",
"Your selection of farm-fresh eggs makes every breakfast feel gourmet.",
"I'm always excited to see what new, seasonal items you'll offer next.",
"The ripeness and flavor of your avocados are unmatched. Perfect every time!",
"Your farm-to-table approach is evident in the superior taste of your produce.",
"I appreciate the wide variety of gluten-free products that cater to my dietary needs.",
"The option to buy in bulk has made maintaining my healthy lifestyle more affordable.",
"Discovering your exotic fruit selection has been an adventure for my taste buds.",
"Your commitment to organic farming practices is why I shop exclusively with you.",
"I'm consistently impressed by the freshness and quality of your seafood selection.",
"Your prepared meals are a lifesaver on busy nights, without sacrificing quality.",
"The juiciness and flavor of your stone fruits make them a summer highlight.",
"I've never been disappointed by the tenderness and taste of your grass-fed meats.",
"Your dairy products, especially the artisanal cheeses, are absolutely divine.",
"The variety and quality of your organic vegetable selection are unparalleled.",
"Finding such a wide range of locally sourced products in one place is incredible.",
"Your online ordering system is seamless, making it easy to get fresh food fast.",
"The freshness of your salad mixes surpasses anything I can find in stores.",
"I love the convenience and quality of your meal kits, packed with fresh ingredients.",
"Your sustainable packaging not only keeps my order fresh but also aligns with my values.",
"The ripeness and taste of your tropical fruit selection bring the flavors of summer to my kitchen.",
"Discovering rare and unique vegetables in my order is always a delightful surprise.",
"Your commitment to no-waste packaging is commendable and a deciding factor for me.",
"The nutritional value and taste of your superfood selections are unmatched.",
"I'm always pleased with the variety and freshness of your root vegetables.",
"Your seasonal fruit baskets are my go-to gift for friends and family.",
"The quality and flavor of your organic coffee selection start my day off right.",
"Finding such a wide assortment of fresh, organic spices has elevated my cooking.",
"The efficiency and friendliness of your delivery service make every order a pleasure.",
"Your selection of fresh, organic juices is refreshing and full of flavor.",
"The tenderness and flavor of your farm-raised poultry are worth every penny.",
"I appreciate the detailed product descriptions that help me make informed choices.",
"Your quick responses to inquiries and order adjustments are top-notch customer service.",
"The variety of your whole grain selection has transformed my baking.",
"Your curated selection of local wines pairs perfectly with your gourmet offerings.",
"The convenience of having fresh, seasonal produce delivered to my door is unbeatable.",
"Your biodegradable packaging shows your commitment to the environment and quality.",
"I'm grateful for the option to subscribe and save on my favorite products.",
"Your fresh, organic nut butters are a staple in my pantry now.",
"The consistently high quality of your meats makes them the centerpiece of any meal.",
"Your exotic fruit selection has introduced me to new flavors and health benefits.",
"The clarity and accuracy of your product labeling make shopping with allergies easy.",
"Your farm's commitment to biodiversity is evident in the variety and quality of produce.",
"The flavor and freshness of your herbs and spices make every dish a masterpiece.",
"I admire your dedication to supporting local farmers and sustainable agriculture.",
"Your gluten-free bakery selection is a game-changer for those of us with dietary restrictions.",
"The option for contactless delivery has made getting fresh food convenient and safe.",
"Your attention to detail in packaging ensures that every item arrives in perfect condition.",
"The variety in your weekly produce boxes keeps our meals exciting and nutritious.",
"Your initiative to minimize food waste through creative recipes and tips is inspiring.",
"The balance between quality and price in your organic product range is exceptional.",
"Your seasonal newsletters with recipes and product highlights are a great touch.",
"The freshness and quality of your locally sourced honey are incomparable.",
"I'm impressed by the sustainability initiatives your farm practices, from soil health to water conservation.",
"Your selection of heirloom vegetables has brought new flavors and stories to our dinner table."
    ]
    return choice(sentiments)


def generate_reviews(n=500):
    reviews = []
    for _ in range(n):
        ID = random_user_id()
        user_name = random_name()  # Generating fake user names
        # seller_name = random_name()  # Generating fake seller names
        rating = randint(1, 5)
        text = random_text()
        verified = fake.boolean()  # Randomly choose verified status using Faker
        review = generate_review(ID, user_name, rating, text, verified)
        reviews.append(review)
    return reviews


def save_to_json(reviews, filename="/home/pce/Pictures/individual_reviews.json"):
    with open(filename, "w") as f:
        json.dump(reviews, f, indent=2)

def save_to_csv(reviews, filename="/home/pce/Pictures/individual_reviews.csv"):
    fieldnames = ['ID', 'user_name', 'time', 'rating', 'text', 'verified', 'date']
    with open(filename, mode="w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for review in reviews:
            writer.writerow(review)

if __name__ == "__main__":
    reviews = generate_reviews(500)
    save_to_json(reviews)
    save_to_csv(reviews)
    print("Generated and saved individual seller reviews completely.")
