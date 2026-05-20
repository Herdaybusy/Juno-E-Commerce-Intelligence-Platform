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

# Required column for transform() 
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
    # For parsing raw eBay HTML 
    soup = BeautifulSoup(html, 'lxml')
    items = []

    # [class~=s-item] matches any tag carrying that class
   
    cards = soup.select('[class~=s-item]')

    if not cards:
        logging.warning("No s-item cards found — raw HTML saved to debug_page.html")
        with open("debug_page.html", "w", encoding="utf-8") as f:
            f.write(html)
        return []

    for card in cards:
        title_tag = (
            card.find('span', role='heading')
            or card.find('span', class_='s-item__title')
        )
        title = title_tag.get_text(strip=True) if title_tag else ''

        # eBay injects a dummy "Shop on eBay" card at position 0 on every page
        if not title or title.lower() == 'shop on ebay':
            continue

        price_tag = card.find('span', class_='s-item__price')
        former_tag = card.find('span', class_='STRIKETHROUGH')
        sold_tag = card.find('span', class_='s-item__dynamic s-item__quantitySold')
        brand_tag = card.find('span', class_='SECONDARY_INFO')
        seller_tag = card.find('span', class_='s-item__seller-info-text')

        items.append({
            'title': title,
            'current_price': price_tag.get_text(strip=True) if price_tag else None,
            'former_price': former_tag.get_text(strip=True) if former_tag else None,
            'items_sold': sold_tag.get_text(strip=True) if sold_tag else None,
            'brand_category': brand_tag.get_text(strip=True) if brand_tag else None,
            'seller_info': seller_tag.get_text(strip=True) if seller_tag else None,
        })

    return items


def get_access_token():
    # swapping credentials for a short-lived token
    credentials = f"{ebay.app_id}:{ebay.cert_id}"
    encoded = base64.b64encode(credentials.encode()).decode()

    response = requests.post(
        ebay.token_url,
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
    # clean JSON with no HTML parsing or bot detection to deal with
    response = requests.get(
        ebay.search_url,
        headers={
            'Authorization': f'Bearer {token}',
            'X-EBAY-C-MARKETPLACE-ID': ebay.marketplace,
            'Content-Type': 'application/json',
        },
        params={
            'q': keyword,
            'limit': ebay.page_size,
            'offset': offset,
        }
    )

    if response.status_code != 200:
        logging.warning(
            f"API call failed at offset {offset}: "
            f"{response.status_code} — {response.text}"
        )
        return []

    items = response.json().get('itemSummaries', [])
    logging.info(f"Offset {offset} — got {len(items)} items back")
    return items


def extract(search_term='laptops', result_limit=200):
    # We calculate offsets from page_size in config rather than hardcoding
   
    all_items = []
    offsets = list(range(0, result_limit, ebay.page_size))

    try:
        token = get_access_token()

        for offset in offsets:
            raw_items = fetch_page(token, keyword=search_term, offset=offset)

            for item in raw_items:
                price_info = item.get('price', {})
                current_price = (
                    f"{price_info.get('currency', '')} "
                    f"{price_info.get('value', '')}".strip()
                    if price_info else None
                )

                all_items.append({
                    'title': item.get('title'),
                    'current_price': current_price,
                    'former_price': None, 
                    'brand_category': item.get('condition'),
                    'seller_info': item.get('seller', {}).get('username'),
                })

    except Exception as e:
        logging.error(f'Something went wrong during extraction: {e}')

    df = pd.DataFrame(all_items)

    if df.empty:
        logging.error('No data came back — check the log above for what went wrong')
    else:
        logging.info(f'Extraction done — {len(df)} records pulled in total')

    return df


def transform(df):
    # cleaning and preparing the data for loading.
    if 'price' in df.columns and 'current_price' not in df.columns:
        df = df.rename(columns={'price': 'current_price'})

    missing = REQUIRED_SCRAPER_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"transform() is missing these columns: {missing}. "
            f"Got: {list(df.columns)}"
        )

    try:
        df = df.drop_duplicates(subset=['title', 'current_price']).copy()

        df['price_gbp'] = df['current_price'].apply(clean_price)
        
        # former_price is optional because the API doesn't return it, but if it's there, it should clean it too
        df['former_price_gbp'] = (
            df['former_price'].apply(clean_price)
            if 'former_price' in df.columns
            else 0.0
        )

        # Using infer_objects() to avoid the pandas FutureWarning about
        
        fill_values = {
            'title': 'Unknown',
            'current_price': '0.00',
            'former_price': '0.00',
            'items_sold': 'Unknown',
            'brand_category': 'Unknown',
            'seller_info': 'Unknown',
            'price_gbp': 0.0,
            'former_price_gbp': 0.0,
        }
        df = df.fillna(fill_values).infer_objects(copy=False)

        df['scraped_at'] = datetime.datetime.now(datetime.timezone.utc)

# Saving the cleaned DataFrame to CSV as a backup in case something goes wrong with the database load. This also gives us a nice snapshot of the data at this stage for debugging and analysis.

        try:
            df.to_csv('data/Laptop Record.csv', index=False)
            logging.info('Transform done — CSV saved to data/')
        except OSError as csv_err:
            logging.warning(
                f'Data will still load into PostgreSQL.'
            )

    except Exception as e:
        logging.error(f'Transform failed: {e}')
        raise

    return df


def load(df, table_name='laptop_records'):
    # table_name is a parameter so Airflow can send laptops, phones,
    # and any future categories into their own separate tables
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f'Data loaded into {table_name} successfully')
    except Exception as e:
        logging.error(f'Load failed: {e}')


def run(search_term='laptops', result_limit=200):
    
    logging.info(
        f'Starting pipeline run — '
        f'search_term={search_term}, result_limit={result_limit}'
    )

    table_name = search_term.replace(' ', '_') + '_listings'
    records = extract(search_term=search_term, result_limit=result_limit)

    if not records.empty:
        cleaned = transform(records)
        load(cleaned, table_name=table_name)
        logging.info(
            f'Pipeline run complete — {len(cleaned)} records in {table_name}'
        )
    else:
        logging.error(
            f'Pipeline run failed for search_term={search_term} — nothing extracted'
        )


if __name__ == '__main__':
    run()