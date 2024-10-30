import json
import asyncio
import datetime
import time
import os
import pandas as pd
import concurrent.futures
import requests
import ast
from betfairlightweight import APIClient
from betfairlightweight import filters
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import datetime, timezone

load_dotenv()

# Oddschecker data fetching function
def get_oddschecker_data(url, user_agent):
    chrome_options = Options()
    chrome_options.add_argument(f'user-agent={user_agent}')
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")

    service = Service('/usr/local/bin/chromedriver')  # Adjust the path if necessary
    #service = Service("/opt/homebrew/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        # Navigate to the specific oddschecker page
        driver.get(url)

        # Wait for the page to load
        time.sleep(6) 

        # Get the page source
        page_source = driver.page_source

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(page_source, 'html.parser')

        # Odds table has id "t1"
        odds_table = soup.find('tbody', id='t1')

        if not odds_table:
            print(f"No odds table found for URL: {url}")
            return None  # Skip this URL if the table isn't found

        # Extract each row and the data within
        odds_data = []
        bookmakers_set = set()

        for row in odds_table.find_all('tr'):
            bet_name = row.find('a', class_='popup').text.strip() 
            odds_dict = {'bet_name': bet_name}
            
            # Find all td elements with odds information
            for td in row.find_all('td', class_=lambda x: x and ('o' in x.split() or 'bs' in x.split())): 
                bookmaker = td.get('data-bk')  # Extract the bookmaker name
                decimal_odds = td.get('data-odig')  # Extract the decimal odds value
                if bookmaker and decimal_odds:  # Only add if both are present
                    odds_dict[bookmaker] = float(decimal_odds)  # Convert odds to float
                    bookmakers_set.add(bookmaker)
            
            odds_data.append(odds_dict)

        # Create a DataFrame with all bookmakers as columns
        df = pd.DataFrame(odds_data)

        # Ensure all bookmakers are columns, even if some are missing in certain rows
        df = df.reindex(columns=['bet_name'] + sorted(bookmakers_set))

        # Add additional columns
        df['timestamp'] = datetime.now(timezone.utc).isoformat()
        #df['url'] = url

        return df
    finally:
        # Close the browser
        driver.quit()

# Asynchronous function to fetch Oddschecker data periodically
async def fetch_oddschecker_data_periodically(interval, url, user_agent):
    loop = asyncio.get_event_loop()
    oddschecker_csv = 'oddschecker_data.csv'

    # Check if CSV file exists; if not, write headers on first data fetch
    header_written = os.path.isfile(oddschecker_csv)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            # Run get_oddschecker_data in a separate thread
            data = await loop.run_in_executor(executor, get_oddschecker_data, url, user_agent)
            print(f"Oddschecker data fetched at {datetime.now(timezone.utc).isoformat()}")

            # Append data to CSV
            if data is not None and not data.empty:
                data.to_csv(oddschecker_csv, mode='a', header=not header_written, index=False)
                header_written = True  # Set header_written to True after first write

            await asyncio.sleep(interval)

# Polymarket data fetching function
def get_polymarket_data():
    r = requests.get("https://gamma-api.polymarket.com/events?id=903193")
    response = r.json()

    market_data = []
    for market in response:
        overall_data = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'market_id': market.get('id', 'N/A'),
            'market_name': market.get('title', 'N/A'),
            #'End Date': market.get('endDate', 'N/A'),
            'overall_liquidity': float(market.get('liquidity', 0)),
            'overall_volume': float(market.get('volume', 0)),
            'overall_volume_24hr': float(market.get('volume24hr', 0)),
        }
        for bet in market.get('markets', []):
            bet_id = bet.get('id')
            if bet_id not in ['253591', '253597']:
                continue  # Skip bets not in the specified list

            outcome_prices_str = bet.get('outcomePrices', '["N/A", "N/A"]')
            try:
                outcome_prices = ast.literal_eval(outcome_prices_str)
                yes_price = float(outcome_prices[0]) if len(outcome_prices) > 0 else None
                no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else None
            except (ValueError, SyntaxError):
                yes_price = None
                no_price = None

            bet_data = {
                'bet_id': bet.get('id'),
                'bet_name': bet.get('question', 'N/A'),
                'bet_liquidity': float(bet.get('liquidity', 0)),
                'bet_volume': float(bet.get('volume', 0)),
                'bet_volume_24hr': float(bet.get('volume24hr', 0)),
                'yes_price': yes_price,
                'no_price': no_price,
            }
            combined_data = {**overall_data, **bet_data}
            market_data.append(combined_data)

    return pd.DataFrame(market_data)

# Asynchronous function to fetch Polymarket data periodically
async def fetch_polymarket_data_periodically(interval):
    loop = asyncio.get_event_loop()
    polymarket_csv = 'polymarket_data.csv'

    # Check if CSV file exists; if not, write headers
    if not os.path.isfile(polymarket_csv):
        with open(polymarket_csv, 'w') as f:
            headers = [
                'timestamp','market_id','market_name',
                'overall_liquidity','overall_volume','overall_volume_24hr',
                'bet_id','bet_name','bet_liquidity','bet_volume',
                'bet_volume_24hr','yes_price','no_price',
            ]
            f.write(','.join(headers) + '\n')

    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            # Run get_polymarket_data in a separate thread
            data = await loop.run_in_executor(executor, get_polymarket_data)
            print(f"Polymarket data fetched at {datetime.now(timezone.utc).isoformat()}")

            # Append data to CSV
            if not data.empty:
                data.to_csv(polymarket_csv, mode='a', header=False, index=False)

            await asyncio.sleep(interval)

# Betfair data fetching function
def get_betfair_data(client):
    market_id = '1.176878927'  # The specific market ID you want to fetch data for
    selection_ids = ['10874213', '12126964']

    # Fetch market catalogue for the specific market ID
    market_catalogues = client.betting.list_market_catalogue(
        filter=filters.market_filter(market_ids=[market_id]),
        max_results=1,
        market_projection=['RUNNER_DESCRIPTION']
    )

    betfair_data = []
    if market_catalogues:
        market = market_catalogues[0]

        # Fetch market book for the specific market ID
        market_books = client.betting.list_market_book(
            market_ids=[market.market_id],
            price_projection=filters.price_projection(price_data=['EX_BEST_OFFERS'])
        )

        if market_books:
            market_book = market_books[0]
            for runner in market_book.runners:
                if str(runner.selection_id) not in selection_ids:
                    continue 
                runner_data = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'market_id': market.market_id,
                    'market_name': market.market_name,
                    'bet_id': runner.selection_id,
                    'bet_name': next(
                        (r.runner_name for r in market.runners if r.selection_id == runner.selection_id),
                        'Unknown'
                    ),
                    'back_price': runner.ex.available_to_back[0].price if runner.ex.available_to_back else None,
                    'back_size': runner.ex.available_to_back[0].size if runner.ex.available_to_back else None,
                    'lay_price': runner.ex.available_to_lay[0].price if runner.ex.available_to_lay else None,
                    'lay_size': runner.ex.available_to_lay[0].size if runner.ex.available_to_lay else None,
                }
                betfair_data.append(runner_data)
    else:
        print(f"No market found for market ID {market_id}")

    return pd.DataFrame(betfair_data)

# Asynchronous function to fetch Betfair data periodically
async def fetch_betfair_data_periodically(client, interval):
    loop = asyncio.get_event_loop()
    betfair_csv = 'betfair_data.csv'

    # Check if CSV file exists; if not, write headers
    if not os.path.isfile(betfair_csv):
        with open(betfair_csv, 'w') as f:
            headers = [
                'timestamp', 'market_id', 'market_name', 'bet_id',
                'bet_name', 'back_price', 'back_size',
                'lay_price', 'lay_size'
            ]
            f.write(','.join(headers) + '\n')

    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            # Run get_betfair_data in a separate thread
            data = await loop.run_in_executor(executor, get_betfair_data, client)
            print(f"Betfair data fetched at {datetime.now(timezone.utc).isoformat()}")

            # Append data to CSV
            if not data.empty:
                data.to_csv(betfair_csv, mode='a', header=False, index=False)

            await asyncio.sleep(interval)

# PredictIt data fetching function
def get_predictit_data():
    url = "https://www.predictit.org/api/marketdata/all/"
    response = requests.get(url)
    data = response.json()

    bet_ids = [27485, 27487]
    market_id = 7456

    # Find the market with the specified ID
    market_data = None
    for market in data.get('markets', []):
        if market.get('id') == market_id:
            market_data = market
            break

    if not market_data:
        print(f"No market found for market ID {market_id}")
        return pd.DataFrame()  # Return an empty DataFrame

    # Extract data
    timestamp = datetime.now(timezone.utc).isoformat()
    market_name = market_data.get('name', 'N/A')
    contracts = market_data.get('contracts', [])

    predictit_data = []
    for contract in contracts:
        if contract.get('id') not in bet_ids:
            continue
        contract_data = {
            'timestamp': timestamp,
            'market_id': market_id,
            'market_name': market_name,
            'bet_id': contract.get('id', 'N/A'),
            'bet_name': contract.get('name', 'N/A'),
            'last_trade_price': contract.get('lastTradePrice', 'N/A'),
            'buy_yes_price': contract.get('bestBuyYesCost', 'N/A'),
            'buy_no_price': contract.get('bestBuyNoCost', 'N/A'),
            'sell_yes_price': contract.get('bestSellYesCost', 'N/A'),
            'sell_no_price': contract.get('bestSellNoCost', 'N/A'),
            'volume': contract.get('volume', 'N/A')
            #'Expiration Date': contract.get('dateEnd', 'N/A')
        }
        predictit_data.append(contract_data)

    return pd.DataFrame(predictit_data)

# Asynchronous function to fetch PredictIt data periodically
async def fetch_predictit_data_periodically(interval):
    loop = asyncio.get_event_loop()
    predictit_csv = 'predictit_data.csv'

    # Check if CSV file exists; if not, write headers
    if not os.path.isfile(predictit_csv):
        with open(predictit_csv, 'w') as f:
            headers = [
                'timestamp','market_id','market_name',
                'bet_id','bet_name','last_trade_price',
                'buy_yes_price','buy_no_price',
                'sell_yes_price','sell_no_price','volume'
            ]
            f.write(','.join(headers) + '\n')

    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            # Run get_predictit_data in a separate thread
            data = await loop.run_in_executor(executor, get_predictit_data)
            print(f"PredictIt data fetched at {datetime.now(timezone.utc).isoformat()}")

            # Append data to CSV
            if not data.empty:
                data.to_csv(predictit_csv, mode='a', header=False, index=False)

            await asyncio.sleep(interval)

# Main function to run all tasks
def main():
    # Oddschecker parameters
    oddschecker_url = "https://www.oddschecker.com/politics/us-politics/us-presidential-election/winner"
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)..."

    # Betfair credentials (replace with your actual credentials)
    app_key = os.getenv("BF_API_KEY")
    username = os.getenv("BF_LOGIN")
    password = os.getenv("BF_PASS")
    certs_path = 'certs'  # Update with your certificate path

    # Initialize Betfair client
    client = APIClient(username, password, app_key=app_key, certs=certs_path)
    client.login()

    # Define the intervals (in seconds) to fetch data
    betfair_interval = 60      # Fetch Betfair data every 60 seconds
    polymarket_interval = 60   # Fetch Polymarket data every 60 seconds
    predictit_interval = 60    # Fetch PredictIt data every 60 seconds
    oddschecker_interval = 60  # Fetch Oddschecker data every 60 seconds

    # Run all tasks in the asyncio event loop
    loop = asyncio.get_event_loop()
    tasks = [
        fetch_polymarket_data_periodically(polymarket_interval),
        fetch_betfair_data_periodically(client, betfair_interval),
        fetch_predictit_data_periodically(predictit_interval),
        fetch_oddschecker_data_periodically(oddschecker_interval, oddschecker_url, user_agent)
    ]
    try:
        loop.run_until_complete(asyncio.gather(*tasks))
    except KeyboardInterrupt:
        pass
    finally:
        client.logout()
        loop.close()

if __name__ == "__main__":
    main()