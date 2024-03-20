from bs4 import BeautifulSoup
import requests
import pandas as pd

link_default = 'https://do.diba.cat/data/ds/establiments/graella?embed=true&filters=false&export-btn=false&iframe-btn=false&info=false&sort_field=descripcio_activitat&sort_ord=desc&start_item=0'

def extract_column_headings(link):
    html_text = requests.get(link_default).text
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

headings = extract_column_headings(link_default)

# 28000 in total
scraped_data = []
for i in range(0, 28000, 50):
    link = '='.join(link_default.split('=')[:-1]) + '=' + str(i)
#     print(link)
    scraped_data.extend(page_scraper(link, headings))

df = pd.DataFrame(scraped_data)
df.to_csv('establishments_Catalonia.csv', index=False)








