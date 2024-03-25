import requests
from bs4 import BeautifulSoup
import pandas as pd

url = 'https://do.diba.cat/data/ds/establiments/graella?embed=true&filters=false&export-btn=false&iframe-btn=false&info=false&sort_field=descripcio_activitat&sort_ord=desc&start_item=6400'

response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# Find the table containing the data
table = soup.find('table')

# Extract column headers
headers = [th.text.strip() for th in table.find_all('th')]

# Extract table rows
data = []
for row in table.find_all('tr'):
    row_data = [cell.text.strip() for cell in row.find_all(['td', 'th'])]
    data.append(row_data)

# Create DataFrame
df = pd.DataFrame(data[1:], columns=headers)

# Print DataFrame
print(df)
