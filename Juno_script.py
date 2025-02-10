from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
import pandas as pd
import requests
from dotenv import load_dotenv
import os 
import re

# Checking the status of the website
url = 'https://www.ebay.co.uk/'
response = requests.get(url)
print(response)

# Integrating requests with beautifulsoup
url = 'https://www.ebay.co.uk/'
response = requests.get(url)
soup = BeautifulSoup(response.text, "lxml")

# Data Extraction
url = 'https://www.ebay.co.uk/sch/i.html?_nkw=laptops&_sacat=0&_from=R40&_trksid=p2334524.m570.l1313&rt=nc&_odkw=generator&_osacat=0&LH_All=1'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'lxml')

# PRODUCTS EXTRACTION 
names = soup.find_all('span', role='heading' )

# PRODUCT NAMES
product_names = []
for name in names[2:]:
    product_names.append(name.get_text(strip=True))

# PRICES EXTRACTION 
current_price =  []
prices = soup.find_all('span', class_='s-item__price' ) #CURRENT PRICE
for price in prices:
    current_price.append(price.get_text(strip=True))

# FORMER PRICE
former_price = [] 
prices = soup.find_all('span', class_='STRIKETHROUGH' ) #FORMER PRICE
for price in prices:
    former_price.append(price.get_text(strip= True))

# ITEMS SOLD
all_items_solds = [] 
items = soup.find_all('span', class_="s-item__dynamic s-item__quantitySold" ) #ITEMS SOLD
for item in items:
    all_items_solds.append(item.get_text(strip= True))

# SELLER INFO
all_seller_info = [] 
items_seller = soup.find_all('span', class_="s-item__seller-info-text" ) #SELLER INFO 
for items in items_seller:
    all_seller_info.append(items.get_text(strip= True))
    
# BRAND CATEGORY
brand_category = [] 
brands= soup.find_all('span', class_="SECONDARY_INFO" ) # BRANDS
for brand in brands:
    brand_category.append(brand.get_text(strip= True))

# CREATING A DICTIONARY
data = {
    'title' : product_names,
    'current_price' : current_price,
    'former_price' : former_price,
    'all_items_solds' : all_items_solds,
    'brand_category': brand_category,
    'all_seller_info' : all_seller_info
}

# CREATING A DATAFRAME
laptop_df = pd.DataFrame.from_dict(data, orient= 'index')

# TRANSPOSING THE DATAFRAME
laptop_df = laptop_df.transpose()
print("succesesfully extracted data")

# FOR ALL PAGES
pages = [x for x in range(1, 20)]

product_names = []
current_price =  []
former_price = []
all_items_solds = [] 
all_seller_info = [] 
brand_category = [] 

for page in pages:
    url = f'https://www.ebay.co.uk/sch/i.html?_nkw=laptops&_sacat=0&_from=R40&_ipg=60&rt=nc&_pgn={page}'
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'lxml')
    
    # PRODUCT NAMES
    names = soup.find_all('span', role='heading' ) # PRODUCT NAME
    for name in names[2:]:
        product_names.append(name.get_text(strip=True))
    
    # PRICES EXTRACTION 
    prices = soup.find_all('span', class_='s-item__price' ) #CURRENT PRICE
    for price in prices:
        current_price.append(price.get_text(strip=True))

    # FORMER PRICE
    prices = soup.find_all('span', class_='STRIKETHROUGH' ) #FORMER PRICE
    for price in prices:
        former_price.append(price.get_text(strip= True))
    
    # ITEMS SOLD
    items = soup.find_all('span', class_="s-item__dynamic s-item__quantitySold" ) #ITEMS SOLD
    for item in items:
        all_items_solds.append(item.get_text(strip= True))
    
    # BRAND CATEGORY
    brands= soup.find_all('span', class_="SECONDARY_INFO" ) #ITEMS SOLD
    for brand in brands:
        brand_category.append(brand.get_text(strip= True))
    
    # SELLER INFO
    items_seller = soup.find_all('span', class_="s-item__seller-info-text" ) #SELLER INFO
    for items in items_seller:
        all_seller_info.append(items.get_text(strip= True))
    
data = {
'title' : product_names,
'current_price' : current_price,
'former_price' : former_price,
'all_items_solds' : all_items_solds,
'brand_category': brand_category,
'all_seller_info' : all_seller_info
}

laptop_df = pd.DataFrame.from_dict(data, orient= 'index')
laptop_df = laptop_df.transpose()
laptop_df
    
# SAVING THE DATA TO CSV
laptop_df.to_csv('Laptop Record.csv', index=False)

# COPY DATA TO KEEP THE RAW DATA
df = laptop_df.copy()

# fill missing values
df.fillna({
    'title' : 'Unknown',
    'former_price': 0.0,
    'all_items_solds' : 'Unknown',
    'brand_category' : 'Unknown',
    'all_seller_info' : 'Unknown'
}, inplace=True)

# LOADING DATA TO DATABASE
# ACCESSING THE DATABASE
load_dotenv(override=True)
db_user = os.getenv('DB_USER')
db_name = os.getenv('DB_NAME')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_password = os.getenv('DB_PASSWORD')

database_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(database_url)
display('Connected successfully')

# %%
laptop_records = df.copy()
laptop_records.to_sql('Laptop_Records',engine, if_exists='replace', index=False)
print('Data successfully loaded to the database')