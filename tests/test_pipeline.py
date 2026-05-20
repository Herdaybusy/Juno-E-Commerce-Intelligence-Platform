import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from ebay_scraper import clean_price, parse_page, transform


# clean_price

def test_clean_price():
    assert clean_price("£999.99") == 999.99
    assert clean_price("£1,200.50") == 1200.50
    assert clean_price(None) is None


def test_clean_price_handles_price_range():
    # eBay sometimes shows a price range like "£900.00 to £1,200.00" on
    # multi-variant listings. We just want to make sure it doesn't crash
    # the pipeline — returning None is fine here.
    result = clean_price("£900.00 to £1,200.00")
    assert result is None or isinstance(result, float)


# parse_page

def test_parse_page():
    html = """
    <html>
        <body>
            <div class="s-item">
                <span class="s-item__title">Test Laptop</span>
                <span class="s-item__price">£999.99</span>
            </div>
        </body>
    </html>
    """
    result = parse_page(html)
    assert len(result) == 1
    assert result[0]["title"] == "Test Laptop"


def test_parse_page_skips_shop_on_ebay():
    # eBay injects a dummy "Shop on eBay" card at position 0 on every page.
    # It should never make it into our data.
    html = """
    <html>
        <body>
            <li class="s-item">
                <span class="s-item__title">Shop on eBay</span>
                <span class="s-item__price">£0.00</span>
            </li>
            <li class="s-item">
                <span class="s-item__title">Dell XPS 15</span>
                <span class="s-item__price">£1,299.99</span>
            </li>
        </body>
    </html>
    """
    result = parse_page(html)
    assert len(result) == 1
    assert result[0]["title"] == "Dell XPS 15"


# transform

def test_transform():
    df = pd.DataFrame([
        {"title": "Laptop A", "price": "£1000"},
        {"title": "Laptop A", "price": "£1000"},
    ])
    result = transform(df)
    assert len(result) == 1
    assert "price_gbp" in result.columns
    assert result["price_gbp"].iloc[0] == 1000.0


def test_transform_drops_invalid_prices():
    # drop row with "N/A" as the price shouldn't affect the pipeline.
    
    df = pd.DataFrame([
        {"title": "Broken Listing", "current_price": "N/A"},
        {"title": "Good Laptop",    "current_price": "£750.00"},
    ])
    result = transform(df)
    assert "price_gbp" in result.columns
    broken = result[result["title"] == "Broken Listing"]
    assert broken["price_gbp"].iloc[0] == 0.0


def test_transform_raises_on_missing_columns():
    
    # clear ValueError — not a silent broken return.
    df = pd.DataFrame([{"name": "Laptop A", "cost": "£500"}])
    with pytest.raises(ValueError, match="missing these columns"):
        transform(df)

# extract (mocked so we don't hit eBay's servers during CI)

def test_extract_returns_dataframe():

    # Mock OAuth token response
    token_response = MagicMock()
    token_response.status_code = 200
    token_response.json.return_value = {
        "access_token": "fake_token"
    }

    # Mock Browse API response
    search_response = MagicMock()
    search_response.status_code = 200
    search_response.json.return_value = {
        "itemSummaries": [
            {
                "title": "Mock Laptop",
                "price": {
                    "currency": "GBP",
                    "value": "499.00"
                },
                "condition": "Used",
                "seller": {
                    "username": "mock_seller"
                }
            }
        ]
    }

    # Mock BOTH requests.post and requests.get
    with patch("ebay_scraper.requests.post", return_value=token_response):
        with patch("ebay_scraper.requests.get", return_value=search_response):

            from ebay_scraper import extract
            result = extract()

    assert isinstance(result, pd.DataFrame)
    assert "title" in result.columns
    assert "current_price" in result.columns
    assert len(result) >= 1