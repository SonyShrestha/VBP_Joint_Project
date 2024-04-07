

import scrapy
import configparser
import logging
import os

class ApprovedfoodGroceriesSpider(scrapy.Spider):
    name = 'approvedfood_groceries'

    # Initialize logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(name)

    # Configurations from config file
    config = configparser.ConfigParser()
    config_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir, os.pardir, os.pardir, os.pardir))
    config_file_path = os.path.join(config_dir, "config.ini")
    config.read(config_file_path)

    if 'APPROVEDFOOD' in config:
        allowed_domains = [config['APPROVEDFOOD'].get('allowed_domain', '')]
        start_urls = [config['APPROVEDFOOD'].get('start_url', '')]
    else:
        allowed_domains = []
        start_urls = []
        logger.error("Config section 'APPROVEDFOOD' not found.")

    def parse(self, response):
        # Extracting the product links...............................
        product_links = response.css('div#cat_products div > div > div.thumbnail > a::attr(href)').getall()
        for link in product_links:
            yield response.follow(link, self.parse_product)

        # Navigating to the next page
        next_page_url = response.css('a.label.pp-nxt.paginate-click::attr(href)').get()
        if next_page_url is not None:
            yield response.follow(next_page_url, self.parse)

    def parse_product(self, response):
        yield {
            'Product_name': response.xpath("//div[contains(@class, 'product_price')]/div/span/text()").get(),
            'Price': response.xpath("(//div[contains(@class, 'pdp_price')]/span/span/text())[1]").get(),
            'Expiry_Date': response.xpath("//tr[1]/td[2]/span/text()").get(),
            'Product_Description': response.xpath("//div[@class='pdp_desc']/span/text()").get()
        }

