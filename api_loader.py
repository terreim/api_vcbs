from flask import Flask, jsonify, request, render_template
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import asyncio
import logging
from cachetools import TTLCache
from datetime import datetime, timedelta

# Configuration constants
CACHE_TTL_SECONDS = 60
SCRAPE_INTERVAL_SECONDS = 15
RENDER_WAIT_SECONDS = 4

# Set up logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s - %(message)s]')
logger = logging.getLogger(__name__)

app = Flask(__name__)
PRICEBOARD_URL = 'https://priceboard.vcbs.com.vn/Priceboard'

class ScrapeDriver:
    def __init__(self):
        self.priceboard_elements = []
        self.stock_names = []
        self.row_element = []
        self.cache = TTLCache(maxsize=1, ttl=CACHE_TTL_SECONDS)
        self.lock = asyncio.Lock()
        self.data_ready = asyncio.Event()
        self.scraping_in_progress = False
        self.active_connections = 0
        self.last_scrape_time = None
        self.queue = asyncio.Queue()

    def get_element(self, element, suffix):
        item = element.find('td', id=lambda x: x and x.endswith(suffix))
        return item.text.strip() if item else None

    async def scrape_elements(self, response):
        try:
            self.stock_names = []
            self.row_element = []

            soup = BeautifulSoup(response, 'html.parser')
            self.priceboard_elements = soup.select('#priceboardContentTableBody > tr')

            for element in self.priceboard_elements:
                try:
                    row = BeautifulSoup(str(element), 'html.parser')
                    name = row.find('span', class_='symbol_link').text
                    if 's_8_s' in name:
                        continue
                    self.stock_names.append(name)
                    self.row_element.append(row)
                except AttributeError:
                    pass  # Ignore elements with missing data
        except Exception as e:
            logger.error(f'Error in scrape_elements: {e}')

    async def scrape_data(self):
        try:
            logger.info("Starting scrape_data")
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.goto(PRICEBOARD_URL)
                await page.wait_for_timeout(RENDER_WAIT_SECONDS * 1000)  # Wait for JS to render
                response = await page.content()
                await browser.close()

            await self.scrape_elements(response)

            data = []
            for name, row in zip(self.stock_names, self.row_element):
                try:
                    row_data = {
                        'name': name,
                        'ceiling': self.get_element(row, 'ceiling'),
                        'floor': self.get_element(row, 'floor'),
                        'prior_close_price': self.get_element(row, 'priorClosePrice'),
                        'change': self.get_element(row, 'change'),
                        'close_price': self.get_element(row, 'closePrice'),
                        'close_volume': self.get_element(row, 'closeVolume'),
                        'total_trading': self.get_element(row, 'totalTrading'),
                        'open': self.get_element(row, 'open'),
                        'high': self.get_element(row, 'high'),
                        'low': self.get_element(row, 'low'),
                        'foreign_buy': self.get_element(row, 'foreignBuy'),
                        'foreign_sell': self.get_element(row, 'foreignSell'),
                        'foreign_remain': self.get_element(row, 'foreignRemain')
                    }
                    data.append(row_data)
                except AttributeError:
                    logger.warning("Skipping row due to missing data")
                except Exception as e:
                    logger.error(f"Error parsing row data: {e}", exc_info=True)

            async with self.lock:
                self.cache['data'] = data
                self.data_ready.set()
            logger.info(f"Scrape completed with {len(data)} items")
            self.last_scrape_time = datetime.now()
        except Exception as e:
            logger.error(f"Error in scrape_data: {e}", exc_info=True)
        finally:
            self.scraping_in_progress = False
            await self.notify_queue()

    async def get_data(self):
        await self.data_ready.wait()
        async with self.lock:
            data = self.cache.get('data', [])
            logger.info(f"Retrieved {len(data)} items from cache")
            return data

    async def notify_queue(self):
        while not self.queue.empty():
            future = await self.queue.get()
            future.set_result(await self.get_data())

    async def handle_request(self):
        if self.scraping_in_progress:
            future = asyncio.get_event_loop().create_future()
            await self.queue.put(future)
            return await future

        current_time = datetime.now()
        if not self.last_scrape_time or (current_time - self.last_scrape_time) >= timedelta(seconds=SCRAPE_INTERVAL_SECONDS):
            self.scraping_in_progress = True
            await self.scrape_data()
            return await self.get_data()
        else:
            return await self.get_data()

scraper = ScrapeDriver()

@app.route('/')
def index():
    return render_template('home.html')

@app.route('/api/health')
def health():
    return "OK", 200

@app.route('/api/data/custom', methods=['GET'])
async def get_data_custom():
    scraper.active_connections += 1
    try:
        logger.info("Received request for /api/data/custom")
        data = await scraper.handle_request()
        selected_stocks = request.args.getlist('stock_name')
        selected_columns = request.args.getlist('columns')

        if not selected_stocks and not selected_columns:
            return jsonify(data)

        filtered_data = []
        for stock in data:
            if stock['name'] in selected_stocks:
                filtered_stock = {key: value for key, value in stock.items() if key in selected_columns or key == 'name'}
                filtered_data.append(filtered_stock)

        return jsonify(filtered_data)
    except Exception as e:
        logger.error(f"Error in get_data_custom: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        scraper.active_connections -= 1

@app.after_request
def after_request(response):
    return response

def run_server():
    logger.info("Starting the server")
    app.run(host="0.0.0.0", port=8000, debug=True, use_reloader=False)

if __name__ == "__main__":
    logger.info("Starting the application")
    run_server()
