import scrapy


class MealdbSpider(scrapy.Spider):
    name = "mealdb"
    allowed_domains = ['www.themealdb.com']
    start_urls = [f'https://www.themealdb.com/browse/letter/{chr(i)}' for i in range(65, 91)]  # A-Z

    def parse(self, response):
        # Iterate through all meal links on the current page
        for meal_link in response.xpath('//div[contains(@class, "col-sm-3")]/a/@href').getall():
            yield response.follow(meal_link, callback=self.parse_meal)

    def parse_meal(self, response):
        food_name = response.xpath('//section[@id="feature"]//td[1]/h2/text()[2]').get()  
 
        ingredients = response.xpath('//figure/figcaption/text()').getall()
    
        description_list = response.xpath("//section[@id='feature']/div/div/text()").getall()
        description = ' '.join([d.strip() for d in description_list]).strip()

        yield {
        "food_name": food_name.strip() if food_name else "No name found",  # Strip whitespace and provide default
        "ingredients": ingredients,
        "description": description.strip() if description else "No description found"  # Strip whitespace and provide default
    }