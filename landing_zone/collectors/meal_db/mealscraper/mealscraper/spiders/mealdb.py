import scrapy
import configparser
import os

class MealdbSpider(scrapy.Spider):
    name = "mealdb"

    def __init__(self):
        # Initialize configuration
        self.config = configparser.ConfigParser()
        config_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir, os.pardir, os.pardir, os.pardir))
        config_file_path = os.path.join(config_dir, "config.ini")
        self.config.read(config_file_path)

        if 'MEALDB' in self.config:
            self.allowed_domains = [self.config['MEALDB']['allowed_domains']]
            start_url_base = self.config['MEALDB']['start_url']
            self.start_urls = [f"{start_url_base}{chr(i)}" for i in range(65, 91)]  # Generates URLs for letters A-Z
        else:
            self.logger.error("Config section 'MEALDB' not found. Using default settings.")
            self.allowed_domains = ['www.themealdb.com']
            self.start_urls = ['https://www.themealdb.com/browse/letter/A']

    def parse(self, response):
        # Process each meal link found on the page
        for meal_link in response.xpath('//div[contains(@class, "col-sm-3")]/a/@href').getall():
            yield response.follow(meal_link, callback=self.parse_meal)

    def parse_meal(self, response):
        # Extracting meal details
        food_name = response.xpath('//section[@id="feature"]//td[1]/h2/text()[2]').get()  
        ingredients = response.xpath('//figure/figcaption/text()').getall()
        description_list = response.xpath("//section[@id='feature']/div/div/text()").getall()
        description = ' '.join([d.strip() for d in description_list]).strip()
        yield {
            "food_name": food_name.strip() if food_name else "No name found", 
            "ingredients": ingredients,
            "description": description.strip() if description else "No description found"
        }
