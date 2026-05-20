Juno E-Commerce Intelligence Platform

A production-style data engineering pipeline that extracts product listings from the eBay Browse API, processes them through an ETL layer, and loads structured data into a PostgreSQL data warehouse, orchestrated using Apache Airflow inside a Docker environment.


🚀 Overview

This project demonstrates a complete end-to-end data engineering workflow:

Extracts product data from the eBay Browse API (OAuth2 authentication)
Transforms raw JSON into structured analytical datasets
Loads cleaned data into PostgreSQL
Automates execution using Apache Airflow
Runs fully inside Docker containers for portability and scalability


🏗️ Architecture
eBay Browse API
      ↓
Airflow DAG (Scheduled Orchestration)
      ↓
Python ETL Layer
      ↓
PostgreSQL Data Warehouse
      ↓
Analytics / BI Tools


⚙️ Tech Stack
Layer	Technology
Data Source	eBay Browse API (OAuth2)
Orchestration	Apache Airflow
Backend	Python 3.13
Data Processing	Pandas
Database	PostgreSQL
Containerisation	Docker Compose
Testing	pytest
Logging	Python logging


📁 Project Structure
JUNO-ECOMMERCE/
│
├── dags/                     # Airflow DAGs (pipeline orchestration)
│   └── juno_dag.py
│
├── config/                   # Configuration management
│   └── settings.py
│
├── data/                    # Data storage
│   ├── raw/
│   └── cleaned_data/
│
├── tests/                   # Unit tests (pytest)
│   └── test_pipeline.py
│
├── utils/                   # Helper utilities (DB, logging, etc.)
│   └── db.py
│
├── ebay_scraper.py         # Core ETL pipeline (entry point)
├── docker-compose.yml      # Full Airflow + Postgres stack
├── requirements.txt        # Dependencies
├── pytest.ini              # Test configuration
└── README.md


🔄 ETL Pipeline
1. Extract
Connects to eBay Browse API using OAuth2
Retrieves product listings (laptops, mobile phones, etc.)
Handles pagination and API rate limits
2. Transform
Cleans price fields (currency removal, type conversion)
Removes duplicates
Handles missing values
Adds ingestion timestamp
3. Load
Loads structured data into PostgreSQL using SQLAlchemy
Creates category-based tables dynamically


🐳 Running with Docker
Start full system
docker-compose up --build
Access Airflow
http://localhost:8080

⚡ Running Locally
pip install -r requirements.txt
python ebay_scraper.py

🧪 Running Tests
python -m pytest -v

Test coverage includes:

price cleaning logic
transformation pipeline
API extraction (mocked)
error handling scenarios


🔐 Environment Variables

Create a .env file:

DB_USER=your_user
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_db

EBAY_APP_ID=your_app_id
EBAY_CERT_ID=your_cert_id


🔁 Airflow Pipeline

Two parallel tasks run daily:

fetch_laptops
fetch_phones

Scheduled at:

06:00 UTC daily


📌 Key Improvements Over Traditional Scraping
Uses official API instead of HTML scraping
Avoids bot detection issues
More stable and scalable
Structured JSON response
Rate-limit aware design

🧭 Roadmap
 Add Slack + email failure alerts
 Add dbt transformation layer
 Add dashboard (Power BI / Metabase)
 Add price trend tracking over time
 Add data quality checks (Great Expectations)


👤 Author
Ahmed Adebola Adebisi
GitHub: Herdaybusy