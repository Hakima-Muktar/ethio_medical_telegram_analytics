import os
import json
import asyncio
import logging
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_ID = os.getenv('TG_API_ID')
API_HASH = os.getenv('TG_API_HASH')
PHONE = os.getenv('TG_PHONE')

# Configure logging
logging.basicConfig(
    filename='logs/scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# List of channels to scrape
CHANNELS = [
    'https://t.me/lobelia4cosmetics',
    'https://t.me/tikvahpharma',
    # 'CheMed Telegram Channel' # Placeholder, needs actual username
]

class TelegramScraper:
    def __init__(self, api_id, api_hash, phone):
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self.client = TelegramClient('scraper_session', api_id, api_hash)

    async def scrape_channel(self, channel_url):
        try:
            # Connect if not strictly connected, though 'with client' handles this usually
            entity = await self.client.get_entity(channel_url)
            channel_name = entity.username or str(entity.id)
            logging.info(f"Scraping channel: {channel_name}")
            print(f"Scraping channel: {channel_name}...")

            messages_data = []
            today = datetime.now().strftime('%Y-%m-%d')
            
            # Create daily directory for JSON
            json_dir = os.path.join('data', 'raw', 'telegram_messages', today)
            os.makedirs(json_dir, exist_ok=True)
            
            # Create channel directory for images
            image_dir = os.path.join('data', 'raw', 'images', channel_name)
            os.makedirs(image_dir, exist_ok=True)

            # Limit to 100 for dev/testing purposes as instructed implicitly or by common sense for now
            # Can be removed or increased for full scrape
            async for message in self.client.iter_messages(entity, limit=200):
                msg_data = {
                    'message_id': message.id,
                    'channel_name': channel_name,
                    'message_date': message.date.isoformat(),
                    'message_text': message.text,
                    'has_media': False,
                    'image_path': None,
                    'views': message.views,
                    'forwards': message.forwards,
                }

                if message.media and isinstance(message.media, MessageMediaPhoto):
                    msg_data['has_media'] = True
                    image_filename = f"{message.id}.jpg"
                    image_path = os.path.join(image_dir, image_filename)
                    # Download image
                    await self.client.download_media(message, file=image_path)
                    msg_data['image_path'] = image_path

                messages_data.append(msg_data)

            # Save to JSON
            json_path = os.path.join(json_dir, f"{channel_name}.json")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(messages_data, f, ensure_ascii=False, indent=4)
            
            logging.info(f"Saved {len(messages_data)} messages for {channel_name} in {json_path}")
            print(f"Saved {len(messages_data)} messages for {channel_name}")

        except Exception as e:
            logging.error(f"Error scraping {channel_url}: {e}")
            print(f"Error scraping {channel_url}: {e}")

    async def run(self):
        # Check if phone is provided
        if not self.phone:
            print("Error: TG_PHONE not found in environment variables.")
            return

        print(f"Starting Telegram Client with Phone: {self.phone[:4]}***")
        
        # Start the client with the phone number
        await self.client.start(phone=self.phone)
        
        async with self.client:
            for channel in CHANNELS:
                await self.scrape_channel(channel)

if __name__ == '__main__':
    if not API_ID or not API_HASH:
        print("Please set TG_API_ID and TG_API_HASH in .env file")
        exit(1)
        
    scraper = TelegramScraper(API_ID, API_HASH, PHONE)
    # python 3.7+
    asyncio.run(scraper.run())