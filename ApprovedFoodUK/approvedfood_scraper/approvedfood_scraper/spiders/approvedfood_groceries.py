
import logging
import scrapy
from scrapy.selector import Selector
from scrapy.utils.project import get_project_settings
            

class ApprovedfoodGroceriesSpider(scrapy.Spider):
    name = 'approvedfood_groceries'
    allowed_domains = ['store.approvedfood.co.uk']
    start_urls = ['https://store.approvedfood.co.uk/groceries']

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


