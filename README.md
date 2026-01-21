# Medical Data Warehouse Pipeline

This project builds a robust data handling pipeline for Ethiopian medical business data scraped from Telegram. It leverages a modern stack including **Telethon** for scraping, **PostgreSQL** for storage, and **dbt** for transformation and dimensional modeling.

## Project Structure

```bash
medical-telegram-warehouse/
├── .github/                # CI/CD workflows
├── data/                   # Data Lake (Raw JSON & Images) - Gitignored
├── medical_warehouse/      # dbt project for transformations
│   ├── models/             # Staging and Marts
│   ├── tests/              # Custom data tests
│   └── dbt_project.yml     
├── logs/                   # Execution logs - Gitignored
├── src/                    # Source code for ETL
│   ├── scraper.py          # Telegram Extractor
│   └── loader.py           # Database Loader
├── tests/                  # Unit tests
├── docker-compose.yml      # PostgreSQL Service
└── requirements.txt        # Python dependencies
```

## Features Implemented (Week 8)

### ✅ Task 1: Data Scraping (Extract)
- **Scraper Script**: `src/scraper.py` uses Telethon to connect to Telegram.
- **Data Lake**:
  - Partitioned JSON storage: `data/raw/telegram_messages/YYYY-MM-DD/channel_name.json`.
  - Image storage: `data/raw/images/{channel_name}/{message_id}.jpg`.
- **Fields Collected**: `message_id`, `date`, `text`, `media`, `views`, `forwards`.
- **Logging**: Detailed execution logs in `logs/scraper.log`.

### ✅ Task 2: Data Warehouse & Transformation
- **Database**: PostgreSQL 15 via Docker.
- **Loading**: `src/loader.py` ingests raw JSON into `raw.telegram_messages`.
- **dbt Modeling**:
  - **Staging**: `stg_telegram_messages` (Cleaning, Casting, Null handling).
  - **Marts (Star Schema)**:
    - `dim_channels`: Channel metadata and aggregated stats.
    - `dim_dates`: Date dimension generated via SQL.
    - `fct_messages`: Fact table with metrics and keys.
- **Testing**:
  - Schema tests: `unique`, `not_null`, `relationships`.
  - Custom test: `assert_no_future_messages`.

## Setup Instructions

1. **Environment Setup**
   ```bash
   pip install -r requirements.txt
   cp .env.example .env
   # Populate .env with Telegram API and DB credentials
   ```

2. **Start Database**
   ```bash
   docker-compose up -d
   ```

3. **Run Pipeline**
   ```bash
   # 1. Scrape Data
   python src/scraper.py
   
   # 2. Load to DB
   python src/loader.py
   
   # 3. Transform with dbt
   cd medical_warehouse
   dbt run
   dbt test
   ```

4. **Run Verification**
   ```bash
   # Unit Tests
   pytest
   
   # dbt Docs
   dbt docs generate && dbt docs serve
   ```
