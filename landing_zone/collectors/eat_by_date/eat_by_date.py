from selenium import webdriver
import pandas as pd
from selenium.webdriver.common.by import By
import re
import json
import os
import logging
import configparser
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager



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


# Return all the category links
def extract_category_link(driver,url):
    category_links = []
    driver.get(url)

    div = driver.find_elements(By.CLASS_NAME, 'heading-link')

    for d in div:
        href_link = d.get_attribute('href')
        category_links.append(href_link)

    # We do not need to scrap data from contacts url
    if contact_url in category_links:
        category_links.remove(contact_url)

    # Link for vegetables was not properly being redirected 
    if 'https://eatbydate.com/vegetables//' in category_links:
        category_links.remove('https://eatbydate.com/vegetables//')

    category_links.extend(['https://eatbydate.com/vegetables/fresh-vegetables/'])
    return category_links



# Return all subcategory links
def extract_subcategory_link(driver, category_links):
    sub_category_links = []

    div = driver.find_elements(By.CLASS_NAME, 'heading-link')

    for d in div:
        href_link = d.get_attribute('href')
        sub_category_links.append(href_link)

    if contact_url in category_links:
        sub_category_links.remove(contact_url)
    return sub_category_links



# Scrap Eat by Date Data
def scrap_eat_by_date(driver,url):
    data_list=[]
    category_links = extract_category_link(driver,url)
    for category_link in category_links:
        
        category_match = re.compile(r"{}(.*?)/".format(url)).search(category_link)
        if category_match:
            category = category_match.group(1)
        else:
            category = 'Others'
        driver.get(category_link)
        sub_categories = extract_subcategory_link(driver, category_link)
        sub_categories = list(set(sub_categories))

        # This table was not properly structured so removed it 
        if 'https://eatbydate.com/dairy/spreads/butter-shelf-life-expiration-date//' in sub_categories:
            sub_categories.remove('https://eatbydate.com/dairy/spreads/butter-shelf-life-expiration-date//')

        for sub_category in sub_categories:
            logger.info('-----------------------------------------------------')
            logger.info("Scrapping data for {sub_category}".format(sub_category=sub_category))
            driver.get(sub_category)
            # Some of the pages were not working and showing error Unable to locate element: {"method":"css selector","selector":"table"}
            try:
                table = driver.find_element(By.TAG_NAME, 'table')
            except Exception as e:
                continue
            
            try:
                title = driver.find_element(By.CSS_SELECTOR, '.title.cntrtxt').text
                title_match = re.compile(r"How Long Do(?:es)? (.*?) Last?").search(title)
                if title_match:
                    title = title_match.group(1)
                else:
                    title = 'Unknown'
            except Exception as e:
                title = 'Unknown'

            # Get all rows from the table
            rows = table.find_elements(By.TAG_NAME, "tr")

            # Initialize empty lists to store data
            data = []

            # Iterate through rows
            for row in rows:
                # Extract each cell's text
                cells = row.find_elements(By.TAG_NAME, "th") + row.find_elements(By.TAG_NAME, "td")
                row_data = []
                for cell in cells:
                    cell_text = cell.text.strip()
                    row_data.append(cell_text)
                    if cell.tag_name == 'th':
                        row_data.append('Header')
                    else:
                        row_data.append('Data')
                        
                # If it's the first row, store column headers
                data.append(row_data)

            # Create a DataFrame with extracted data and column headers
            df = pd.DataFrame(data)

            # In some cases tables are not pre, skip those cases
            if df.empty:
                continue

            # Concatenate even columns and last column
            result_df = pd.concat([df.iloc[:, ::2], df.iloc[:, -1]], axis=1)
            result_df.reset_index(drop=True, inplace=True)
            result_df.columns = range(result_df.shape[1]) 

            #Initialize variables
            results = []

            # Iterate through DataFrame rows
            for index, row in result_df.iterrows():
                if row.iloc[-1] == 'Header':
                    headers = row.iloc[:-1].tolist()  # Extract header values
                    cpy = headers.copy()
                    headers[0]='item_description'
                elif row.iloc[-1] == 'Data':
                    data_dict = {headers[i].lower(): row[i] for i in range(len(headers))}  # Create dictionary for data row

                    data_dict['type']=cpy[0]

                    # Ignore lines with blank item description
                    if data_dict['item_description']!='':
                        results.append(data_dict)
                    data_dict['category']=category
                    data_dict['sub_category']=title

            data_list.extend(results)
    return data_list



if __name__ == "__main__":
    url = config["EAT_BY_DATE"]["url"]
    raw_data_dir = config["COMMON"]["raw_data_dir"]
    chrome_driver = config["COMMON"]["chrome_driver"]

    contact_url = "https://eatbydate.com/contact//"

    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Add other options as needed

    # Initialize the Chrome WebDriver with webdriver_manager
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    eat_by_date_json = scrap_eat_by_date(driver,url)

    with open(os.path.join(raw_data_dir,'eat_by_date.json'), 'w') as json_file:
        json.dump(eat_by_date_json, json_file, indent=4)
    driver.quit()