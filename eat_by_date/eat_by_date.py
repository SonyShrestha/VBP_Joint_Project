from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import json
import re
import requests

main_link = 'https://eatbydate.com'

def return_category_links(main_link):
    category_links = []
    html_text = requests.get(main_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    table = soup.find('div', class_ = "col-cody-12 boxed-cody-white boxed-botbor-green cody-dropshadow padding-zero")
    table_elements = table.find_all('div', class_='col-cody-3')
    for table_element in table_elements:
        link = main_link + table_element.find('a', class_ ='heading-link')['href']
        category_links.append(link)

    # Remove the link to contact
    category_links.remove('https://eatbydate.com/contact/')
    return category_links

def return_sub_category_links(category_link, main_link):
    sub_categories = []
    html_text = requests.get(category_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    table = soup.find('div', class_ = "col-cody-12 boxed-cody-white boxed-botbor-green cody-dropshadow padding-zero")
    table_elements = table.find_all('div', class_= lambda c: c and c.startswith('col-cody-4'))
    for table_element in table_elements:
        link = main_link + table_element.find('a', class_ ='heading-link')['href']
        sub_categories.append(link)
    return sub_categories


def scrap_eat_by_date():
    data_list = []

    # Get Category Links
    category_links=return_category_links(main_link)

    # Removing Vegetables from sub_categories and add new link to it
    category_links.remove('https://eatbydate.com/vegetables/')
    category_links.extend(['https://eatbydate.com/vegetables/fresh-vegetables/'])

    for category_link in category_links:
        category_match = re.compile(r"https://eatbydate.com/(.*?)/").search(category_link)
        if category_match:
            category = category_match.group(1)
        else:
            category = 'Others'
        sub_categories = return_sub_category_links(category_link, main_link)
        for sub_category in sub_categories:
            if sub_category == 'https://eatbydate.com/turkey-shelf-life-expiration-date//':
                continue
            html_text = requests.get(sub_category).text
            soup = BeautifulSoup(html_text, 'lxml')
            
            # Some links are redirected to new ones. If links are redirected to new ones, scrap data from new link
            if "You are being redirected to" in html_text:
                sub_category = soup.find("a").get_text(strip=True)
                if not sub_category.startswith("https://eatbydate.com/"):
                    sub_category = "https://eatbydate.com/"+sub_category
                html_text = requests.get(sub_category).text
                soup = BeautifulSoup(html_text, 'lxml')
            
            # Get sub category name from heading
            # Headings are sometimes present with class "title cntrtxt" or "title cntrtxt aligncenter"
            subcategory = soup.find('h2', class_='title cntrtxt') or soup.find('h2', class_='title cntrtxt aligncenter')
            if subcategory:
                sub_category = subcategory.get_text(strip=True).replace(' Expiration Date','')
            else:
                sub_category = ""
            table = soup.find('div', class_ = "col content-wrapper")
            if table:
                rows = table.find_all("tr")
                if rows:
                    second_heading_index = next((idx for idx, row in enumerate(rows) if row.find("th")), None)
                    if second_heading_index is not None:
                        headers_1 = [header.get_text(strip=True).lower() for header in rows[0].find_all("th")]
                        headers_1[0]='item_description'
                        for row in rows[1:second_heading_index]:
                            data_dict = {}
                            cells = row.find_all("td")
                            if cells:
                                for idx, cell in enumerate(cells):
                                    data_dict[headers_1[idx]] = cell.get_text(strip=True)
                                if any(data_dict.values()):
                                    data_list.append(data_dict)
                        headers_2 = [header.get_text(strip=True).lower() for header in rows[second_heading_index].find_all("th")]
                        headers_2[0]='item_description'
                        for row in rows[second_heading_index+1:]:
                            data_dict = {}
                            cells = row.find_all("td")
                            if cells:
                                for idx, cell in enumerate(cells):
                                    # This page has the case where headers are less than td elements, writing this try catch to handle this case
                                    try:
                                        data_dict[headers_2[idx]] = cell.get_text(strip=True)
                                    except Exception as e:
                                        pass
                                if any(data_dict.values()):
                                    data_list.append(data_dict)
                    else:
                        headers = [header.get_text(strip=True).lower() for header in rows[0].find_all("th")]
                        for row in rows[1:]:
                            data_dict = {}
                            cells = row.find_all("td")
                            if cells:
                                for idx, cell in enumerate(cells):
                                    data_dict[headers[idx]] = cell.get_text(strip=True)
                                if any(data_dict.values()):
                                    data_list.append(data_dict)
            for entry in data_list:
                new_entry = {"category": category,"sub_category":sub_category}
                for key, value in entry.items():
                    new_entry[key] = re.sub(r'\s*lasts\s*for\s*', '', re.sub(r'\s+', ' ', value.replace('\n',' ')) ) 
                entry.clear()
                entry.update(new_entry)
    return data_list


# Example usage:
if __name__ == "__main__":
    eat_by_date_json = scrap_eat_by_date()
    with open('eat_by_date.json', 'w') as json_file:
        json.dump(eat_by_date_json, json_file, indent=4)