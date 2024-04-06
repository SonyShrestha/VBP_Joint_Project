import os
from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import re
import json
import configparser

# find the links for categories -> subcategories -> products
def starts_with_grocery(tag):
    return tag.name == 'a' and tag.get('href', '').startswith('/grocery/')

def get_product_heirarchy(links):
    heirarchy=[]
    for link in links:
        link_href= link.get("href")
        link= str(link)
        soup = bs(link, 'html.parser')
        a_tag_text = soup.a.get_text(strip=True)
        link_href_list= link_href.split("/")
        link_href_list.pop()
        link_href_list.append(a_tag_text)
        link_href_list= link_href_list[2:]
        heirarchy.append(link_href_list)
    for i in heirarchy:
        if i[-1]==i[-2]:
            i.pop()
    return heirarchy

def get_actual_links(base_url, links):
    href_list=[]
    actual_link_list=[]
    for i in range(len(links)):
        href_list.append(links[i].get('href'))
    for i in href_list:
        actual_link= base_url+i
        actual_link_list.append(actual_link)
    return actual_link_list

def extract_price(price):
    price_list= price.split("₹")
    selling_price= price_list[1]
    try:
        actual_price= price_list[2][:-7]
        discount= price_list[2][-7:][:2]
    except:
        actual_price= selling_price
        discount= 0
    numbers_list = [selling_price, actual_price, discount]
    return numbers_list

def clean_specs(specs_list):
    specs_list.pop()
    transformed_list = []

    for item in specs_list:
        items = item.split('\n')
        # Combine adjacent elements into a single string
        transformed_list.extend([' '.join(items[i:i+2]) for i in range(0, len(items), 2)])
    
    # return the transformed list
    return transformed_list

def append_to_json(data, filename):
    try:
        with open(filename, 'r') as json_file:
            existing_data = json.load(json_file)
    except FileNotFoundError:
        existing_data = []

    existing_data.append(data)

    with open(filename, "w+") as json_file:
        json.dump(existing_data, json_file, indent=4)

def get_links(driver):
    # _1zrN4q (category) -> _6WOcW9 (sub_category) -> _6WOcW9._2-k99T (selected link) -> _6WOcW9._3YpNQe (sub_sub_category)
    dropdown_menu_elements = driver.find_elements(By.CLASS_NAME, "_1zrN4q")
    links=[]
    link_num= 0
    is_broken=False
    for categories in dropdown_menu_elements:
        is_broken=False
        action = ActionChains(driver)
        action.move_to_element(categories)
        action.perform()

        sub_category_links= driver.find_elements(By.CLASS_NAME, '_6WOcW9')
        print(len(sub_category_links))

        for sub_sub_category in sub_category_links:
            action.move_to_element(sub_sub_category)
            action.perform()
            final_links= driver.find_elements(By.CLASS_NAME, '_6WOcW9._3YpNQe')
            for link in final_links:
                link_num+=1
                print(f"link{link_num}")
                href = link.get_attribute('href')
                href= str(href)
                href_list= href.split("/")
                # print(href_list[4], href_list[5], href_list[6])
                print(href)
                if link_num!=1 and href in links:
                    is_broken=True
                    print("broken")
                    break
                else:
                    links.append(href)
            if is_broken:
                break
    print(f"Numnber of original links = {len(links)}")
    return links

def get_filtered_links(link_list):
    filtered_link_list=[]
    for link in link_list:
        link_str_list= link.split("/")
        try:
            print(link_str_list[4], link_str_list[5], link_str_list[6])
            if (link_str_list[6][:6]!="pr?sid" and link_str_list[4]!="personal-baby-care" and link_str_list[4]!="household-care" and link_str_list[4]!="home-kitchen" and link_str_list[4]!="office-school-supplies"):
                filtered_link_list.append(link)
        except:
            pass
    print(f"Numnber of filtered links = {len(filtered_link_list)}")
    return filtered_link_list

def get_product_details(link_list, driver, json_folder_path):
    print(f" The number of links: {len(link_list)}")
    # print(f" The number of product heirarchies: {len(prod_heirarchy_list)}")

    # go over all the links in the links list and get the products from them
    all_products_list=[]
    # for i in link_list:
    #     print(i)
    iteration_num=0
    control_automation=0
    working_link_flag= True
    for i, link in enumerate(link_list):
        iteration_num+=1
        url= link
        url_page_list=[]
        for page_num in range(1,6):
            url_page_list.append(url+f"&page={page_num}")
        print(url_page_list)

        page_number=0
        for url in url_page_list:
          control_automation+=1
          page_number+=1
          # Open the URL
          driver.get(url)
          # Wait for dynamic content to load (you may need to adjust the wait time)
          driver.implicitly_wait(10)

          # put info in the edit text field for pin
          # pincode_box = driver.find_element(By.XPATH, '//*[@id="container"]/div/div[1]/div[1]/div[2]/div[3]/div[2]/div/div[2]/div[2]/div/div/div[1]/form/input')
        #   if control_automation==1:
        #       pincode_box= driver.find_element(By.CLASS_NAME, "_166SQN")
        #       pincode_box.send_keys('751020')
        #       pincode_box.send_keys(Keys.RETURN)

          url_link_list= url.split("/")
          each_product_heirarchy= [url_link_list[4], url_link_list[5], url_link_list[6]]
          each_url_product_list=[]

          # selenium dynamic web scrapping
          try:
            product_list = driver.find_elements(By.CLASS_NAME, "_2gX9pM")
          except:
            working_link_flag= False

          if(len(product_list)==0):
            working_link_flag= False
          else:
            working_link_flag= True

          print(f"WORKING PAGE LINK= {working_link_flag}")

          if working_link_flag==True:
            product_link_list= []
            for product in product_list:
                # Find the link element within each product element
                link_element = product.find_element(By.TAG_NAME, "a")
                # Get the href attribute value
                href = link_element.get_attribute("href")
                product_link_list.append(href)
                print(href)
            print(product_link_list)
            print(f"number of products is: {len(product_link_list)}")

            product_number=0
            for link in product_link_list:
                product_number+=1
                print("--------------------------------")
                print(f"PRODUCT NUMBER {product_number}")
                print("--------------------------------")

                # Navigate to the product page
                driver.get(link)
                
                # product details extraction
                try:
                    product_name= driver.find_element(By.CLASS_NAME, "B_NuCI").text
                except:
                    product_name="no name"
                print(product_name)
                try:
                    price= driver.find_element(By.CLASS_NAME, "_25b18c").text
                except:
                    price= "no price"
                print(price)
                try:
                  rating= driver.find_element(By.CLASS_NAME, "_3LWZlK").text
                except:
                  rating= "no ratings"
                print(rating)
                try:
                    expiry_info= driver.find_element(By.CLASS_NAME,"_1F59lZ").text
                except:
                    expiry_info= ""
                print(expiry_info)
                try:
                    manufactuting_info= driver.find_element(By.CLASS_NAME,"cdfcxs").text
                except:
                    manufactuting_info= ""
                print(manufactuting_info)
                try:
                    specifications= driver.find_elements(By.CLASS_NAME, "_14cfVK")
                    specs_list=[]
                    for specs in specifications:
                        specs= str(specs.text)
                        specs= specs.strip("\n")
                        specs_list.append(specs)
                    # print(specs_list)
                    specs_list= clean_specs(specs_list)
                except:
                    specs_list= []
                print(specs_list)

                # converting price to price string
                price= str(price)

                # handling the price string from the webpage
                try:
                    price_list= extract_price(price)
                    print(price_list)
                    print(f"selling price= {price_list[0]}, actual_price= {price_list[1]}, discount= {price_list[2]}% off")
                except:
                    price_list= ["no selling price", "no actual price", "no discounts"]

                each_product_dict={}
                each_product_dict['product_id']= f"{iteration_num}.{page_number}.{product_number}"
                each_product_dict['name']= product_name
                if price_list[0]>price_list[1]:
                    each_product_dict['selling_price']= price_list[1]
                    each_product_dict['actual_price']= price_list[0]
                else:
                    each_product_dict['selling_price']= price_list[0]
                    each_product_dict['actual_price']= price_list[1]
                each_product_dict['discount']= price_list[2]
                each_product_dict['rating']= rating
                each_product_dict['expiry_date']= str(expiry_info)[-11:]
                each_product_dict['manufacturing_date']= str(manufactuting_info)[-11:]
                each_product_dict['prod_heirarachy']= each_product_heirarchy
                each_product_dict['specifications']= specs_list
                
                # defining the json path to save the dict into the json on the go and append the results as we get a new one
                json_file_path= os.path.join(json_folder_path, "Flipkart_all_products.json")

                # appending the dict content to json file
                append_to_json(each_product_dict, json_file_path)

                # list containing details of all the products in the form of a list of dictionaries
                each_url_product_list.append(each_product_dict)

            print(each_url_product_list)
            all_products_list.extend(each_url_product_list)
          else:
              break
    print(all_products_list)  
    # Close the WebDriver
    driver.quit()

    return all_products_list

def save_json(list, json_folder_path):
    print("saving the products extracted to a json file...")
    json_file_path= os.path.join(json_folder_path, "product.json")
    with open(json_file_path, "w+") as json_file:
        json.dump(list, json_file, indent=4)

def save_txt(list):
    print("saving filtered links to a TXT file...")
    # Open a file in write mode
    with open("filtered_links.txt", "w") as file:
        # Iterate over the list and write each element to the file
        for item in list:
            file.write("%s\n" % item)

# Get the path to the parent parent directory
config_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir, os.pardir, os.pardir))

# Specify the path to config file
config_file_path = os.path.join(config_dir, "config.ini")

# reading the config file
config = configparser.ConfigParser()
config.read(config_file_path)

# define paths
# root_path= "D:/BDMA/UPC/BDM/PROJECT"
# json_folder_path= os.path.join(root_path,"JSON_files")
# if not os.path.exists(json_folder_path):
#   os.makedirs(json_folder_path)

# defining the flipkart grocery website base url and the headers
# URL= "https://www.flipkart.com/grocery-supermart-store?marketplace=GROCERY"
HEADERS= ({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36', 'Accept-Language': 'en, en-US, en;g=0.5'})

json_folder_path= config['COMMON']['raw_data_dir']
BASE_URL= config['FLIPKART']['BASE_URL']
URL= config['FLIPKART']['URL']
# HEADERS= config['FLIPKART']['HEADER']

# start chrome in headless mode
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_driver_path = ChromeDriverManager().install()
service = Service(chrome_driver_path)
driver = webdriver.Chrome(service= service, options=chrome_options)

# Open the URL
driver.get(URL)

# Wait for dynamic content to load (you may need to adjust the wait time)
driver.implicitly_wait(10)

# put info in the edit text field for pin
# pincode_box = driver.find_element(By.XPATH, '//*[@id="container"]/div/div[1]/div[1]/div[2]/div[3]/div[2]/div/div[2]/div[2]/div/div/div[1]/form/input')
pincode_box= driver.find_element(By.CLASS_NAME, "_166SQN")
pincode_box.send_keys('751020')
pincode_box.send_keys(Keys.RETURN)

extracted_link_list= get_links(driver)

filtered_link_list= get_filtered_links(extracted_link_list)    

save_txt(filtered_link_list)

product_list= get_product_details(filtered_link_list, driver, json_folder_path)

# save the list of dictionaries to json
# save_json(product_list, json_folder_path)