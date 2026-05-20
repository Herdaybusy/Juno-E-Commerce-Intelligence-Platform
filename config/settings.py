import os
from dotenv import load_dotenv

load_dotenv(override=True)


class DatabaseConfig:
    user     = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host     = os.getenv('DB_HOST', 'localhost')
    port     = os.getenv('DB_PORT', '5432')
    name     = os.getenv('DB_NAME')

    @property
    def url(self):
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


class EbayApiConfig:
    # Credentials for the eBay Browse API — loaded from .env so they're
    # never hardcoded or committed to the repo
    app_id  = os.getenv('EBAY_APP_ID')
    cert_id = os.getenv('EBAY_CERT_ID')

    # OAuth2 token endpoint 
    token_url = 'https://api.ebay.com/identity/v1/oauth2/token'

    # Browse API search endpoint
    search_url = 'https://api.ebay.com/buy/browse/v1/item_summary/search'

    # Pulling from the UK marketplace 
    marketplace = 'EBAY_GB'

    # Results to return per API call 
    page_size = 50


# Single instances imported everywhere 
db   = DatabaseConfig()
ebay = EbayApiConfig()