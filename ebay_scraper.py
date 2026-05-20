from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import pandas as pd
import requests
from dotenv import load_dotenv
import datetime
import logging
import base64
import re
 
from config.settings import db, ebay
 
 
load_dotenv(override=True)
 
engine = create_engine(db.url)

logging.basicConfig(
    filename='etl_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

# Require column for transform() 
REQUIRED_SCRAPER_COLUMNS = {'title', 'current_price'}

def clean_price(raw):
    if not raw:
        return None
    # Strip out everything that isn't a digit or a decimal point
    cleaned = re.sub(r'[^\d.]', '', str(raw).replace(',', ''))
    try:
        return float(cleaned)
    except ValueError:
        return None

def parse_page(html):

    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    items = []

    # [class~=s-item] matches any tag where s-item is a space-delimited class
    cards = soup.select('[class~=s-item]')
    
    if not cards:
        logging.warning("No s-item cards found — raw HTML saved to debug_page.html")
        with open("debug_page.html", "w", encoding="utf-8") as f:
            f.write(html)
        return []
 
    for card in cards:
        title_tag = card.find('span', role='heading') or card.find('span', class_='s-item__title')
        title = title_tag.get_text(strip=True) if title_tag else ''
 
        # To skip fake "Shop on eBay" listing at the top of eBay
        if not title or title.lower() == 'shop on ebay':
            continue
 
        price_tag  = card.find('span', class_='s-item__price')
        former_tag = card.find('span', class_='STRIKETHROUGH')
        sold_tag   = card.find('span', class_='s-item__dynamic s-item__quantitySold')
        brand_tag  = card.find('span', class_='SECONDARY_INFO')
        seller_tag = card.find('span', class_='s-item__seller-info-text')
 
        items.append({
            'title':          title,
            'current_price':  price_tag.get_text(strip=True)  if price_tag  else None,
            'former_price':   former_tag.get_text(strip=True)  if former_tag else None,
            'items_sold':     sold_tag.get_text(strip=True)    if sold_tag   else None,
            'brand_category': brand_tag.get_text(strip=True)   if brand_tag  else None,
            'seller_info':    seller_tag.get_text(strip=True)  if seller_tag else None,
        })
 
    return items
 
def get_access_token():
    
    # Swapping credentials for a short-lived token first, because eBay's API uses OAuth2
    credentials = f"{ebay.app_id}:{ebay.cert_id}"
    encoded = base64.b64encode(credentials.encode()).decode()

    response = requests.post(
        'https://api.ebay.com/identity/v1/oauth2/token',
        headers={
            'Authorization': f'Basic {encoded}',
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        data='grant_type=client_credentials&scope=https://api.ebay.com/oauth/api_scope',
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Couldn't get an access token: {response.status_code} — {response.text}"
        )

    token = response.json().get('access_token')
    logging.info('Got eBay access token — ready to make API calls')
    return token


def fetch_page(token, keyword, offset):
    # Clean JSON straight from eBay — no HTML parsing, no bot detection,

    response = requests.get(
        'https://api.ebay.com/buy/browse/v1/item_summary/search',
        headers={
            'Authorization': f'Bearer {token}',
            'X-EBAY-C-MARKETPLACE-ID': 'EBAY_GB',
            'Content-Type': 'application/json',
        },
        params={
            'q': keyword,
            'limit': 50,
            'offset': offset,
        }
    )

    if response.status_code != 200:
        logging.warning(
            f"API call failed at offset {offset}: {response.status_code} — {response.text}"
        )
        return []

    items = response.json().get('itemSummaries', [])
    logging.info(f"Offset {offset} — got {len(items)} items back")
    return items


def extract(search_term='laptops', result_limit=200):
    # result_limit is to controls how many records we pull in total.
    
    all_items = []
    offsets = list(range(0, result_limit, 50))

    try:
        token = get_access_token()

        for offset in offsets:
            raw_items = fetch_page(token, keyword=search_term, offset=offset)

            for item in raw_items:
                price_info = item.get('price', {})
                current_price = (
                    f"{price_info.get('currency', '')} {price_info.get('value', '')}".strip()
                    if price_info else None
                )

                all_items.append({
                    'title': item.get('title'),
                    'current_price': current_price,
                    'former_price': None,  # the Browse API doesn't expose strikethrough prices
                    'items_sold': None,
                    'brand_category': item.get('condition'),
                    'seller_info': item.get('seller', {}).get('username'),
                })

    except Exception as e:
        logging.error(f'Something went wrong during extraction: {e}')
    
    # Converting to DataFrame at the end, so if something goes wrong with one page, we still get whatever data we managed to pull in before the error  
    df = pd.DataFrame(all_items)

    # If the API calls failed but didn't raise an exception, we might end up with an empty DataFrame — logging that as an error so it's clear something went wrong, rather than just handing back an empty table and wondering why
    if df.empty:
        logging.error('No data came back — check the log above for what went wrong')
    else:
        logging.info(f'Extraction done — {len(df)} records pulled in total')

    return df


def transform(df):
    # Tests pass in a 'price' column rather than 'current_price',
    # so we rename it here before doing anything else
    if 'price' in df.columns and 'current_price' not in df.columns:
        df = df.rename(columns={'price': 'current_price'})

    # Now we check, after the rename, so 'price' doesn't get incorrectly flagged
    missing = REQUIRED_SCRAPER_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"transform() is missing these columns: {missing}. "
            f"Got: {list(df.columns)}"
        )

    try:
        # dropping duplicates to avoid skewing analysis later on.
        df = df.drop_duplicates(subset=['title', 'current_price']).copy()
        
        df['price_gbp'] = df['current_price'].apply(clean_price)

        df['former_price_gbp'] = (
            df['former_price'].apply(clean_price) if 'former_price' in df.columns else 0.0
        )

        # filling in missing values with defaults, so we don't end up with nulls in our database which can cause issues for analysis later on.
        df.fillna({
            'title': 'Unknown',
            'current_price': '0.00',
            'former_price': '0.00',
            'items_sold': 'Unknown',
            'brand_category': 'Unknown',
            'seller_info': 'Unknown',
            'price_gbp': 0.0,
            'former_price_gbp': 0.0,
        }, inplace=True)

        # Adding a timestamp for when the data was scraped
        df['scraped_at'] = datetime.datetime.now(datetime.timezone.utc)

        # Saving to CSV as well, so we have a backup of the cleaned data outside the database 
        logging.info('Transform done — CSV saved')

    except Exception as e:
        # Re-raise so a broken transform actually looks like a failure, rather than silently handing back a broken DataFrame
        logging.error(f'Transform failed: {e}')
        raise

    return df


def load(df, table_name='laptop_records'):
    # table_name is parameterised so Airflow can route different product categories into their own tables
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f'Data loaded into {table_name} successfully')
    except Exception as e:
        logging.error(f'Load failed: {e}')


def run(search_term='laptops', result_limit=200):
    # Entry point Airflow calls.
    logging.info(f'Starting pipeline run — search_term={search_term}, result_limit={result_limit}')

    table_name = search_term.replace(' ', '_') + '_listings'

    records = extract(search_term=search_term, result_limit=result_limit)

    if not records.empty:
        cleaned = transform(records)
        load(cleaned, table_name=table_name)
        logging.info(f'Pipeline run complete — {len(cleaned)} records in {table_name}')
    else:
        logging.error(f'Pipeline run failed for search_term={search_term} — nothing extracted')


if __name__ == '__main__':
    run()