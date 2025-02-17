from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
import pandas as pd
import requests
from dotenv import load_dotenv
import os 
import re
import logging

# ACCESSING THE DATABASE
load_dotenv(override=True)
db_user = os.getenv('DB_USER')
db_name = os.getenv('DB_NAME')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_password = os.getenv('DB_PASSWORD')

# Create engine to connect to the database
database_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(database_url)
print('Connected to the database successfully')

# logging configuration
logging.basicConfig(filename='etl_pipeline.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# EXTRACTING DATA
url = 'https://www.ebay.co.uk/sch/i.html?_nkw=laptops&_sacat=0&_from=R40&_trksid=p2334524.m570.l1313&rt=nc&_odkw=generator&_osacat=0&LH_All=1'
response = requests.get(url)

soup = BeautifulSoup(response.text, 'lxml')
print(response.status_code)
laptop_info = soup.find_all('div', class_="s-item__details clearfix")
def extract():
    try:
        # FOR ALL PAGES
        pages = [x for x in range(1, 5)]

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
            'product_name' : product_names,
            'current_price' : current_price,
            'former_price' : former_price,
            'all_items_solds' : all_items_solds,
            'brand_category': brand_category,
            'all_seller_info' : all_seller_info
            }

        laptop_records = pd.DataFrame.from_dict(data, orient= 'index')
        laptop_records = laptop_records.transpose()
        logging.info("Data converted to Dataframe successfully")
        print("Data converted to Dataframe successfully")
        
    except Exception as e:
        logging.error(f"Error in extracting data: {e}")
        print("Error in extracting data: {e}")
        
    if laptop_records.empty:
        logging.error("No data extracted")
        print("No data extracted")
    else:
        logging.info("Data extracted successfully")
        print("Data extracted successfully")
        
    return laptop_records

# TRANSFORMING DATA
def transform(laptop_records):
    try:
        laptop_records.fillna({
            'Product_name': 'Unknown',
            'Current_price': 0.0,
            'Former_price': 0.0,
            'All_items_solds' : 'Unknown',
            'Brand_category' : 'Unknown',
            'All_seller_info' : 'Unknown'
        }, inplace=True)
        laptop_records.to_csv('Laptop Record.csv', index=False)
        logging.info("Data transformed successfully")
        print("Data transformed successfully")
        return laptop_records
    except Exception as e:
        logging.error(f"Error in transforming data: {e}")
        print(f"Error in transforming data: {e}")
        return laptop_records

# LOADING INTO THE DATABASE
def load(laptop_records):
    try:
        laptop_records.to_sql('laptop_records', engine, if_exists='replace', index=False)
        logging.info("Data loaded successfully")
        print("Data loaded successfully")
    except Exception as e:
        logging.error(f"Error in loading data: {e}")
        print(f"Error in loading data: {e}")

# Main function
if __name__ == "__main__":
    # ETl process
    laptop_records = extract()
    if not laptop_records.empty:
        laptop_df = transform(laptop_records)
        load(laptop_records)
        print("ETL process completed")
    else:
        print("ETL process failed")