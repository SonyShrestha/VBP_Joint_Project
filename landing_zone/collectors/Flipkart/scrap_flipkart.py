import os
from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import re
import json

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

def get_actual_links(links):
    href_list=[]
    actual_link_list=[]
    for i in range(len(links)):
        href_list.append(links[i].get('href'))
    for i in href_list:
        actual_link= "https://www.flipkart.com"+i
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

def get_product_details(link_list, prod_heirarchy_list, driver):
    print(f" The number of links: {len(link_list)}")
    print(f" The number of product heirarchies: {len(prod_heirarchy_list)}")

    # go over all the links in the links list and get the products from them
    all_products_list=[]
    # for i in link_list:
    #     print(i)
    iteration_num=0
    for i, link in enumerate(link_list):
        iteration_num+=1
        url= link
        print(url)

        # Open the URL
        driver.get(url)
        # Wait for dynamic content to load (you may need to adjust the wait time)
        driver.implicitly_wait(10)

        # put info in the edit text field for pin
        # pincode_box = driver.find_element(By.XPATH, '//*[@id="container"]/div/div[1]/div[1]/div[2]/div[3]/div[2]/div/div[2]/div[2]/div/div/div[1]/form/input')
        if iteration_num==1:
            pincode_box= driver.find_element(By.CLASS_NAME, "_166SQN")
            pincode_box.send_keys('751020')
            pincode_box.send_keys(Keys.RETURN)

        each_product_heirarchy= prod_heirarchy[i]
        each_url_product_list=[]

        # selenium dynamic web scrapping
        product_list = driver.find_elements(By.CLASS_NAME, "_2gX9pM")

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
            product_name= driver.find_element(By.CLASS_NAME, "B_NuCI").text
            print(product_name)
            price= driver.find_element(By.CLASS_NAME, "_25b18c").text
            print(price)
            rating= driver.find_element(By.CLASS_NAME, "_3LWZlK").text
            print(rating)
            try:
                expiry_info= driver.find_element(By.CLASS_NAME,"_1F59lZ").text
                print(expiry_info)
            except:
                expiry_info= ""
            try:
                manufactuting_info= driver.find_element(By.CLASS_NAME,"cdfcxs").text
                print(manufactuting_info)
            except:
                manufactuting_info= ""
            specifications= driver.find_elements(By.CLASS_NAME, "_14cfVK")
            specs_list=[]
            for specs in specifications:
                specs= str(specs.text)
                specs= specs.strip("\n")
                specs_list.append(specs)
            # print(specs_list)
            specs_list= clean_specs(specs_list)
            print(specs_list)

            # dictionary containing product info
            price= str(price)
            price_list= extract_price(price)
            print(price_list)
            print(f"selling price- {price_list[0]}, actual_price- {price_list[1]}, discount- {price_list[2]}% off")
            each_product_dict={}
            each_product_dict['product_id']= f"{iteration_num}.{product_number}"
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
            json_file_path= os.path.join(json_folder_path, "flipkart.json")

            # appending the dict content to json file
            append_to_json(each_product_dict, json_file_path)

            # list containing details of all the products in the form of a list of dictionaries
            each_url_product_list.append(each_product_dict)

        print(each_url_product_list)
        all_products_list.extend(each_url_product_list)
    print(all_products_list)  
    # Close the WebDriver
    driver.quit()

    return all_products_list

def save_json(list, json_folder_path):
    print("saving the products extracted to a json file")
    json_file_path= os.path.join(json_folder_path, "product.json")
    with open(json_file_path, "w+") as json_file:
        json.dump(list, json_file, indent=4)

# define paths
root_path= "D:/BDMA/UPC/BDM/PROJECT"
json_folder_path= os.path.join(root_path,"JSON_files")
if not os.path.exists(json_folder_path):
  os.makedirs(json_folder_path)

# defining the flipkart grocery website base url and the headers
URL= "https://www.flipkart.com/grocery-supermart-store?marketplace=GROCERY"
HEADERS= ({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36', 'Accept-Language': 'en, en-US, en;g=0.5'})

# getting the base webpage
webpage= requests.get(URL, headers= HEADERS)
print(webpage)

# getting the soup for the html pages
soup= bs(webpage.content, 'html.parser')
print(soup)

# get the base links 
links= soup.find_all(starts_with_grocery)
print(links)

# Get the product heirarchy from the links
prod_heirarchy= get_product_heirarchy(links)

# get the actual usable links from the base links by appendin the https header
actual_link_list= get_actual_links(links)

# go to the website link above and then ge the product details from there
# initializig selenium for dynamic web scrapping
# Set up Chrome WebDriver using WebDriver Manager
s = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=s)

# going to base url and putting the pincode
# Open the URL
# driver.get(URL)

# # Wait for dynamic content to load (you may need to adjust the wait time)
# driver.implicitly_wait(10)

# # put info in the edit text field for pin
# pincode_box = driver.find_element(By.XPATH, '//*[@id="container"]/div/div[1]/div[1]/div[2]/div[3]/div[2]/div/div[2]/div[2]/div/div/div[1]/form/input')
# pincode_box.send_keys('751020')
# pincode_box.send_keys(Keys.RETURN)

# remove unnecessary links
food_product_link_list=[]
for link in actual_link_list:
    link_list= link.split("/")
    if link_list[4]== 'personal-baby-care' or link_list[4]== 'household-care':
        continue
    else:
        food_product_link_list.append(link)

# printing the links
for link in food_product_link_list:
    print(link)      

product_list= get_product_details(food_product_link_list, prod_heirarchy, driver)

# save the list of dictionaries to json
save_json(product_list, json_folder_path)