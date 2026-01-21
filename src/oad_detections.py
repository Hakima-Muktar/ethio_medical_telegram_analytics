import os
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_NAME = os.getenv('POSTGRES_DB')
DB_HOST = os.getenv('POSTGRES_HOST')
DB_PORT = os.getenv('POSTGRES_PORT')

# Configure logging
logging.basicConfig(
    filename='logs/loader_detections.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_db_engine():
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)

def create_detections_table(engine):
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.yolo_detections (
                id SERIAL PRIMARY KEY,
                image_path TEXT,
                channel_name VARCHAR,
                message_id BIGINT,
                label VARCHAR,
                confidence FLOAT,
                x_min FLOAT,
                y_min FLOAT,
                x_max FLOAT,
                y_max FLOAT,
                ingested_at TIMESTAMP DEFAULT NOW()
            );
        """))
        conn.commit()
    logging.info("Table raw.yolo_detections created or exists.")

def load_detections(engine, csv_path='data/processed/yolo_detections.csv'):
    if not os.path.exists(csv_path):
        logging.warning("No detection CSV found.")
        return

    try:
        df = pd.read_csv(csv_path)
        
        # Insert data using pandas to_sql for simplicity mixed with existing engine
        # However, to consistency with previous loader, let's use to_sql which is efficient enough for this size
        df.to_sql('yolo_detections', engine, schema='raw', if_exists='append', index=False)
        
        logging.info(f"Loaded {len(df)} detections from {csv_path}")
        print(f"Loaded {len(df)} detections from {csv_path}")
        
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        print(f"Error loading data: {e}")

if __name__ == '__main__':
    if not all([DB_USER, DB_PASSWORD, DB_NAME]):
        print("Database credentials missing in .env")
        exit(1)
        
    try:
        engine = get_db_engine()
        create_detections_table(engine)
        load_detections(engine)
        print("Data loading complete.")
    except Exception as e:
        print(f"Failed to connect to DB: {e}")