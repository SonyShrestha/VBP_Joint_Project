from bs4 import BeautifulSoup
import requests
import pandas as pd
import logging
import os
import configparser

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

def extract_column_headings(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    table = soup.find('table', class_ = "table table-condensed table-striped table-hover text-nowrap bottom-scroll")
    
    # Save the column headings
    headings = []
    thead = soup.find('thead')
    heading_row = thead.find('tr')
    column_headings = heading_row.find_all('th')
    for column_heading in column_headings:
    #     print(column_heading)
        heading = column_heading.find('div', class_ = 'group-label')
        if heading:  # the first column is blank, so it doesn't contain div
    #         print(heading.text.strip())
            headings.append(heading.text.strip())
    # Add empty column heading at the start to match the data format in the table
    headings.insert(0, 'Empty')
    return headings


def page_scraper(link, headings):  # Scrape each page
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    table = soup.find('table', class_ = "table table-condensed table-striped table-hover text-nowrap bottom-scroll")
    tbody = soup.find('tbody')
    rows = tbody.find_all('tr')
    scraped_page = []
    for row_index, row in enumerate(rows):
        dict_row = {}
        cells = row.find_all('td')
        for index, cell in enumerate(cells):
            col_title = headings[index]
    #         print(cell.text.strip())
            dict_row[col_title] = cell.text.strip()
        scraped_page.append(dict_row) 
    return scraped_page

if __name__ == "__main__":

    link_default = config["CATALONIA_ESTABLISHMENTS"]["url"]
    raw_data_dir = config["COMMON"]["raw_data_dir"]
    headings = extract_column_headings(link_default)

    # 28000 in total
    scraped_data = []
    for i in range(0, 28000, 50):
        link = '='.join(link_default.split('=')[:-1]) + '=' + str(i)
    #     print(link)
        scraped_data.extend(page_scraper(link, headings))

    df = pd.DataFrame(scraped_data)

    # Remove the first column as there is no information there - no column name, so using index
    df.drop(df.columns[0], axis=1, inplace=True)


    # ID, UTMx & UTMy, Annex names don't have to be changed
    df.rename(columns={
    'Nom comercial' : 'Commercial_name', 
    'Nom raó Social' : 'Social_name', 
    'NIF raó social' : 'Company_NIF',        
    "Descripció de l'activitat" : 'Activity_description', 
    "Identificador de l'activitat" : 'Activity_identifier',      
    "Nom de l'ens": 'Registration_entity', 
    'Codi INE': 'INE_code', 
    'Ens relacionat':'ENS_relation', 
    'Codi de comarca':'County_code',  
    'Adreça completa': 'Full_address', 
    'Localització': 'Location', 
    'Tipus de via': 'Road_type',
    'Nom de la via' : 'Street_name',
    'Número via' : 'Street_number',
    'Escala': 'Scale', 
    'Pis' : 'Location_info_1',
    'Porta': 'Location_info_2',
    'Tipus de nucli': 'Core_type', 
    'Codi postal': 'Postage_pick_up', 
    'Municipi' : 'Municipality',
    'Altres activitats' : 'Other_activities', 
    'Sector econòmic': 'Economic_sector',
    'Codi NACE': 'Code_NACE', 
    'Descripció NACE': 'Description_NACE',
    'Tipus Industrial o productiva': 'Industrial_productive_type',
    'Codi CCAE': 'Raise_CCAE', 
    'Descripció CCAE': 'Description_CCAE',  
    'Tipus comercial': 'Commercial_type', 
    'Tipus EMA': 'EMA_type',
    'Data de modificació': 'Modification_date', 
    'Codi Ens': 'Code_Ens',
    'Data llicència': 'License_date', 
    'Data CI': 'Date_CI',
    'Conjunt de dades': 'Data_set',
    'Últim canvi': 'Last_change'
    }, inplace=True)

    # Remove those with missing values in following columns
    df = df[df['Commercial_name'].notna()]
    df = df[df['Location'].notna()]
    df = df[df['Full_address'].notna()]

    # Around 18000 rows
    df.to_csv(os.path.join(raw_data_dir,'establishments_catalonia.csv'), index=False)








