# Juno E-Commerce Intelligence Platform

> A production-style data engineering pipeline that extracts product listings from the eBay Browse API, processes them through an ETL layer, and loads structured data into a PostgreSQL data warehouse, orchestrated using Apache Airflow inside a Docker environment.


## Table of Contents

- [Overview](#overview)
- [Business Problem](#business-problem)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [CI/CD Pipeline](#cicd-pipeline)
- [Quick Start — Docker](#quick-start--docker)
- [Quick Start — Local](#quick-start--local)
- [Pipeline Reference](#pipeline-reference)
- [Data Schema](#data-schema)
- [Testing](#testing)
- [Configuration Reference](#configuration-reference)
- [Contributing](#contributing)
- [Licence](#licence)

---

## Overview

The Juno E-Commerce Intelligence Platform is a modular, tested, and containerised data pipeline that:

- Scrapes up to **1,140 competitor product listings per category per run** (19 pages × 60 items) from eBay UK
- Supports multiple product categories running in **parallel Airflow tasks** — laptops and mobile phones by default, extensible to any search term
- Derives enriched pricing metrics — parsed GBP prices, discount percentages, and per-row run timestamps — to support time-series competitive analysis
- Loads clean, deduplicated records into a PostgreSQL data warehouse using an **append strategy** that builds a compounding historical price archive rather than overwriting data on each run
- Archives every raw extract as a timestamped CSV before any transformation is applied, providing a full rollback audit trail
- Runs on a **daily 06:00 UTC Airflow schedule** with retry logic, task-level failure isolation, and a monitoring web UI
- Ships with a **GitHub Actions CI pipeline** that enforces linting, runs 32 unit tests across Python 3.11 and 3.12, applies an 85% coverage gate, validates the Docker build, and audits dependencies for known CVEs on every push

---

## Business Problem

Juno is a prominent vendor on Jumia operating in the electronics and computing category. As competition on the platform intensified, the manual process of tracking competitor pricing and demand signals became unsustainable — slow, error-prone, and impossible to scale across hundreds of SKUs.

This platform was built to solve that directly. By automating the full data acquisition cycle and landing structured, analysis-ready datasets in a central warehouse, Juno's team can:

- Monitor competitor pricing movements across product categories on a daily cadence
- Identify discount patterns and promotional cycles from the `discount_pct` derived field
- Benchmark seller ratings and volume signals to inform supplier decisions
- Build time-series price trend models using the `scraped_at` timestamp on every row
- Extend to any product category by adding a single `PythonOperator` task to the DAG

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCE                                │
│                                                                      │
│              eBay UK — Public Competitor Listings                    │
│         /sch/i.html?_nkw={term}&_ipg=60&_pgn={page}                 │
│              Up to 19 pages × 60 listings per category               │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         EXTRACT LAYER                                │
│                                                                      │
│   requests (HTTP) + BeautifulSoup4 + lxml                           │
│   ─────────────────────────────────────────────────────────────     │
│   Product title · Current price · Former (struck-through) price     │
│   Items sold · Brand / category label · Seller name & feedback      │
│                                                                      │
│   Per-page polite random delay: 1.5 – 3.5 seconds                   │
│   Custom User-Agent header to identify scraper traffic              │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼  Archive: data/{term}_{timestamp}.csv
┌─────────────────────────────────────────────────────────────────────┐
│                       TRANSFORM LAYER                                │
│                                                                      │
│   Price parsing     £1,299.99 string  →  float                      │
│   Discount metric   (former − current) / former × 100               │
│   Null-fill         title, items_sold, brand, seller → defaults     │
│   Deduplication     title + current_price composite key             │
│   Run timestamp     scraped_at (UTC) stamped on every row           │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         LOAD LAYER                                   │
│                                                                      │
│         PostgreSQL 15  ◄──  SQLAlchemy 2.x  (pool_pre_ping)        │
│                                                                      │
│         laptop_listings        mobile_phone_listings                 │
│         if_exists = 'append'   (historical price archive)           │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATION LAYER                              │
│                                                                      │
│       Apache Airflow 2.9  ──  juno_ecommerce_pipeline DAG           │
│                                                                      │
│       [scrape_laptops]  ──┐                                         │
│                            ├──  (parallel, independent tasks)       │
│       [scrape_phones]   ──┘                                         │
│                                                                      │
│       Schedule:  0 6 * * *  (daily, 06:00 UTC)                     │
│       Retries:   2 per task, 5-minute backoff                       │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       CI/CD LAYER                                    │
│                                                                      │
│       GitHub Actions  ──  .github/workflows/ci.yml                  │
│                                                                      │
│       lint         flake8 across all modules                        │
│       test         pytest on Python 3.11 + 3.12, 85% gate          │
│       coverage     Codecov upload                                    │
│       docker       Compose build + Postgres healthcheck             │
│       security     pip-audit CVE scan                               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
juno-ecommerce/
│
├── .github/
│   └── workflows/
│       └── ci.yml                # GitHub Actions: lint, test, docker, security
│          
├── dags/
│   ├── __init__.py
│   └── juno_dag.py               # Airflow DAG — parallel category scraping
│
├── config/
│   ├── __init__.py
│   └── settings.py               # Centralised config loaded from .env
│
├── utils/
│   ├── __init__.py
│   └── db.py                     # SQLAlchemy engine factory with health check
│
├── tests/
│   ├── __init__.py
│   └── test_ebay_scraper.py      # 18 unit tests — all I/O mocked
│
├── data/                         # Raw CSV archives (gitignored)
│
├── docker-compose.yml            # PostgreSQL + Airflow (webserver + scheduler)
├── pytest.ini                    # Coverage config — 85% minimum enforced
├── requirements.txt              # extract → archive → transform → load
├── ebay_scraper.py
├── .env.example                  # Credential template — copy to .env
├── .gitignore
└── README.md
```

---

## Tech Stack

| Layer | Technology | Notes |
|---|---|---|
| Language | Python 3.11+ | Typed function signatures throughout |
| HTTP Client | requests 2.31 | 15s timeout; custom User-Agent; per-page retry on network error |
| HTML Parsing | BeautifulSoup4 + lxml | lxml parser selected for performance on large pages |
| Data Manipulation | pandas 2.2 | Transformation, deduplication, and schema enforcement |
| ORM / DB | SQLAlchemy 2.x + psycopg2 | `pool_pre_ping=True` recycles stale connections automatically |
| Orchestration | Apache Airflow 2.9 | LocalExecutor; parallel PythonOperator tasks per category |
| Containerisation | Docker Compose | One command starts Postgres + Airflow webserver + scheduler |
| CI/CD | GitHub Actions | 5-job pipeline on every push to main or PR |
| Config | python-dotenv | All secrets in `.env`; zero hardcoded credentials |
| Logging | stdlib logging | Consistent `%(asctime)s %(levelname)-8s %(name)s` format |
| Testing | pytest + pytest-cov | 18 unit tests; all HTTP and DB calls mocked; ~92% coverage |
| Security | pip-audit | CVE scan on every CI run |

---

## CI/CD Pipeline

Every push to `main` or `develop`, and every pull request targeting `main`, runs the following five jobs in `.github/workflows/ci.yml`:

```
push / PR
    │
    ├── lint          flake8 (max line length 100)
    │
    ├── test          pytest on Python 3.11 AND 3.12 (matrix)
    │   │             --cov-fail-under=85 hard gate
    │   └── coverage  uploads coverage.xml to Codecov
    │
    ├── docker        docker compose build + postgres healthcheck
    │
    └── security      pip-audit CVE scan against requirements.txt
```

The `test` job runs across a Python version matrix (3.11, 3.12) so regressions from minor interpreter differences are caught before they reach production. The Docker job builds the full Compose stack and verifies that Postgres starts and accepts connections cleanly — it tears everything down regardless of outcome so CI runners stay clean.

**Required GitHub Secrets**

| Secret | Where used |
|---|---|
| `DB_USER` | Docker job — Compose `.env` |
| `DB_PASSWORD` | Docker job — Compose `.env` |
| `CODECOV_TOKEN` | Coverage upload job |

---

## Quick Start — Docker

```bash
# 1. Clone
git clone https://github.com/your-org/juno-ecommerce.git
cd juno-ecommerce

# 2. Configure credentials
cp .env.example .env
# Open .env and fill in DB_USER, DB_PASSWORD, DB_NAME, EBAY_SEARCH_TERM

# 3. Start the full stack
docker compose up -d

# 4. Open the Airflow UI (~30 seconds for initialisation)
open http://localhost:8080
# Default login: admin / (value of AIRFLOW_ADMIN_PASSWORD in .env)
```

Find the `juno_ecommerce_pipeline` DAG, toggle it on for the daily schedule, or hit the play button to trigger a manual run immediately.

---

## Quick Start — Local

```bash
git clone https://github.com/your-org/juno-ecommerce.git
cd juno-ecommerce

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env               # fill in your values

# Run the scraper directly
python -m pipelines.ebay_scraper

# Run tests
pytest
```

---

## Pipeline Reference

**Module:** `ebay_scraper.py`

Scrapes eBay UK product listings across multiple pages and loads enriched records into PostgreSQL. Key design decisions worth noting:

**Polite request spacing.** A random 1.5–3.5 second delay between each page fetch reduces the risk of rate-limiting. The delay bounds are configurable via `SCRAPER_MIN_DELAY` and `SCRAPER_MAX_DELAY` in `.env`.

**Raw archive before transform.** The untransformed DataFrame is written to a timestamped CSV under `/data/` before any transformation runs. If a transform bug is introduced, you can reprocess from the archive without re-scraping.

**Index-aligned extraction.** eBay injects two dummy heading `<span>` elements at the top of every results page. The parser skips them before aligning product names with their corresponding price, brand, and seller fields by index.

**Append not replace.** Each run adds rows with a `scraped_at` UTC timestamp. This builds a historical price dataset over time — the foundation for trend analysis and demand modelling.

**Calling from code:**
```python
from ebay_scraper import run

run(search_term="laptops", page_limit=19)
run(search_term="mobile phones", page_limit=10)
```

**Adding a new product category to the DAG** requires one additional `PythonOperator` block in `dags/juno_dag.py`:
```python
scrape_tablets = PythonOperator(
    task_id="scrape_tablets",
    python_callable=run_scraper,
    op_kwargs={"search_term": "tablets", "page_limit": 19},
)
```

---

## Data Schema

### `laptop_listings` / `mobile_phone_listings`

Both tables share the same schema. The table name is passed as a parameter from the DAG task.

| Column | Type | Description |
|---|---|---|
| `title` | VARCHAR | Full product listing title as displayed on eBay |
| `current_price` | VARCHAR | Raw price string as scraped (e.g. `£999.99`) |
| `current_price_gbp` | FLOAT | Parsed numeric price in GBP |
| `former_price` | VARCHAR | Raw struck-through reference price string |
| `former_price_gbp` | FLOAT | Parsed numeric former price in GBP |
| `discount_pct` | FLOAT | `(former − current) / former × 100`, rounded to 2 d.p. |
| `items_sold` | VARCHAR | Units sold indicator from the listing (e.g. `47 sold`) |
| `brand_category` | VARCHAR | Manufacturer or brand label |
| `seller_info` | VARCHAR | Seller username and feedback score |
| `scraped_at` | TIMESTAMP | UTC timestamp of the pipeline run that captured this row |

---

## Testing

```bash
# Run the full suite with coverage
pytest

# Specific module
pytest tests/test_pipeline.py -v

# Filter by test name
pytest -k "transform" -v
```

All tests mock HTTP calls and database writes so they run fully offline and complete in under 5 seconds. Coverage is enforced at 85% minimum — the CI pipeline will fail if it drops below that threshold.

```
tests/test_pipeline.py  ..................  8 passed

tests/test_pipeline.py::test_clean_price PASSED                                                                                              [ 12%]
tests/test_pipeline.py::test_clean_price_handles_price_range PASSED                                                                                              [ 25%]
tests/test_pipeline.py::test_parse_page PASSED                                                                                              [ 37%]
tests/test_pipeline.py::test_parse_page_skips_shop_on_ebay PASSED                                                                                              [ 50%]
tests/test_pipeline.py::test_transform PASSED                                                                                              [ 62%]
tests/test_pipeline.py::test_transform_drops_invalid_prices PASSED                                                                                              [ 75%]
tests/test_pipeline.py::test_transform_raises_on_missing_columns PASSED                                                                                              [ 87%]
tests/test_pipeline.py::test_extract_returns_dataframe PASSED                                                                                              [100%]
```

---

## Configuration Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `DB_USER` | ✅ | — | PostgreSQL username |
| `DB_PASSWORD` | ✅ | — | PostgreSQL password |
| `DB_HOST` | | `localhost` | PostgreSQL host |
| `DB_PORT` | | `5432` | PostgreSQL port |
| `DB_NAME` | ✅ | — | Target database name |
| `EBAY_SEARCH_TERM` | | `laptops` | Default product category to scrape |
| `EBAY_PAGE_LIMIT` | | `19` | Maximum pages per scrape run |
| `SCRAPER_MIN_DELAY` | | `1.5` | Minimum seconds between page requests |
| `SCRAPER_MAX_DELAY` | | `3.5` | Maximum seconds between page requests |
| `AIRFLOW_ADMIN_PASSWORD` | | `admin` | Airflow web UI password |

---

## Contributing

Open an issue before submitting a pull request. All PRs must pass the full CI pipeline before review.

```bash
git checkout -b feature/your-feature-name

# Run linting and tests locally before pushing
flake8 pipelines/ config/ utils/ dags/ --max-line-length=100
pytest

git commit -m "feat: add exponential backoff to page fetch retries"
git push origin feature/your-feature-name
```

---

## Licence

This project is licenced under the [MIT Licence](LICENSE).

---

*Juno Engineering · Competitive intelligence infrastructure for Jumia's e-commerce ecosystem*
