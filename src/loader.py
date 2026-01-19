import os
import json
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
    filename='logs/loader.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_db_engine():
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)

def create_raw_table(engine):
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                id SERIAL PRIMARY KEY,
                message_id BIGINT,
                channel_name VARCHAR,
                message_date TIMESTAMP,
                message_text TEXT,
                has_media BOOLEAN,
                image_path VARCHAR,
                views INTEGER,
                forwards INTEGER,
                ingested_at TIMESTAMP DEFAULT NOW()
            );
        """))
        conn.commit()
    logging.info("Table raw.telegram_messages created or exists.")

def load_data(engine):
    data_dir = os.path.join('data', 'raw', 'telegram_messages')
    if not os.path.exists(data_dir):
        logging.warning("No raw data directory found.")
        return

    conn = engine.connect()
    try:
        for root, dirs, files in os.walk(data_dir):
            for file in files:
                if file.endswith('.json'):
                    file_path = os.path.join(root, file)
                    logging.info(f"Processing {file_path}")
                    
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                    # Insert data
                    for msg in data:
                        # Basic UPSERT check via NOT EXISTS (simplification)
                        # or just insert everything for now, duplicate handling is better in dbt usually
                        # But let's check for duplicate message_id + channel_name to assume uniqueness?
                        # For raw layer, often appending is fine, but let's try to be clean.
                        
                        # Use raw SQL for insertion
                        query = text("""
                            INSERT INTO raw.telegram_messages 
                            (message_id, channel_name, message_date, message_text, has_media, image_path, views, forwards)
                            VALUES (:message_id, :channel_name, :message_date, :message_text, :has_media, :image_path, :views, :forwards)
                        """)
                        
                        conn.execute(query, {
                            'message_id': msg.get('message_id'),
                            'channel_name': msg.get('channel_name'),
                            'message_date': msg.get('message_date'),
                            'message_text': msg.get('message_text'),
                            'has_media': msg.get('has_media'),
                            'image_path': msg.get('image_path'),
                            'views': msg.get('views'),
                            'forwards': msg.get('forwards')
                        })
                    
                    logging.info(f"Loaded {len(data)} messages from {file}")
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        print(f"Error loading data: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    if not all([DB_USER, DB_PASSWORD, DB_NAME]):
        print("Database credentials missing in .env")
        exit(1)
        
    try:
        engine = get_db_engine()
        create_raw_table(engine)
        load_data(engine)
        print("Data loading complete.")
    except Exception as e:
        print(f"Failed to connect to DB: {e}")