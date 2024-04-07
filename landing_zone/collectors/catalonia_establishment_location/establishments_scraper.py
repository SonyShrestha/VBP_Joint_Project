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
        heading = column_heading.find('div', class_ = 'group-label')
        if heading:  # the first column is blank, so it doesn't contain div
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
            dict_row[col_title] = cell.text.strip()
        scraped_page.append(dict_row) 
    return scraped_page

if __name__ == "__main__":

    link_default = config["CATALONIA_ESTABLISHMENTS"]["url"]
    total_records = int(config["CATALONIA_ESTABLISHMENTS"]["total_records"])
    records_per_page = int(config["CATALONIA_ESTABLISHMENTS"]["records_per_page"])
    raw_data_dir = config["COMMON"]["raw_data_dir"]
    headings = extract_column_headings(link_default)

    logger.info('-----------------------------------------------------')
    logger.info("Extracting supermarket data from Establishments Catalonia Website")

    scraped_data = []
    for i in range(0, total_records, records_per_page):
        link = '='.join(link_default.split('=')[:-1]) + '=' + str(i)
        scraped_data.extend(page_scraper(link, headings))

    df = pd.DataFrame(scraped_data)

    # Remove the first column as there is no information there - no column name, so using index
    df.drop(df.columns[0], axis=1, inplace=True)


    # UTMx & UTMy names don't have to be changed
    df.rename(columns={
    'Id':'id',
    'Nom comercial' : 'commercial_name', 
    'Nom raó Social' : 'social_name', 
    'NIF raó social' : 'company_NIF',        
    "Descripció de l'activitat" : 'activity_description', 
    "Identificador de l'activitat" : 'activity_identifier',      
    "Nom de l'ens": 'registration_entity', 
    'Codi INE': 'INE_code', 
    'Ens relacionat':'ENS_relation', 
    'Codi de comarca':'county_code',  
    'Adreça completa': 'full_address', 
    'Localització': 'location', 
    'Tipus de via': 'road_type',
    'Nom de la via' : 'street_name',
    'Número via' : 'street_number',
    'Escala': 'scale', 
    'Pis' : 'location_info_1',
    'Porta': 'location_info_2',
    'Tipus de nucli': 'core_type', 
    'Codi postal': 'postage_pick_up', 
    'Municipi' : 'municipality',
    'Altres activitats' : 'other_activities', 
    'Sector econòmic': 'economic_sector',
    'Codi NACE': 'code_NACE', 
    'Descripció NACE': 'description_NACE',
    'Tipus Industrial o productiva': 'industrial_productive_type',
    'Codi CCAE': 'raise_CCAE', 
    'Descripció CCAE': 'description_CCAE',  
    'Tipus comercial': 'commercial_type', 
    'Tipus EMA': 'EMA_type',
    'Data de modificació': 'modification_date', 
    'Codi Ens': 'code_ens',
    'Data llicència': 'license_date',
    'Annex':'annex', 
    'Data CI': 'date_CI',
    'Conjunt de dades': 'data_set',
    'Últim canvi': 'last_change'   
    }, inplace=True)

    # Remove those with missing values in following columns
    df = df[df['commercial_name'].notna()]
    df = df[df['location'].notna()]
    df = df[df['full_address'].notna()]

    # Around 18000 rows
    df.to_csv(os.path.join(raw_data_dir,'establishments_catalonia.csv'), index=False)

    logger.info('-----------------------------------------------------')
    logger.info("Filtering records with null commercial_name, location or full_address")

    dumped_df = pd.read_csv(os.path.join(raw_data_dir,'establishments_catalonia.csv'))
    dumped_df = dumped_df[dumped_df['commercial_name'].notna()]
    dumped_df = dumped_df[dumped_df['location'].notna()]
    dumped_df = dumped_df[dumped_df['full_address'].notna()]

    dumped_df.to_csv(os.path.join(raw_data_dir,'establishments_catalonia.csv'), index=False)







