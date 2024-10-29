import json
import asyncio
import websockets
import datetime
import threading
import time
import os
from betfairlightweight import APIClient
from betfairlightweight.filters import streaming_market_filter
from betfairlightweight.streaming.listener import StreamListener
from dotenv import load_dotenv

load_dotenv()

# Polymarket data streaming function
async def stream_polymarket(assets_ids):
    url = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
    last_time_pong = datetime.datetime.now()

    # Create directory for Polymarket data if it doesn't exist
    os.makedirs('polymarket_data', exist_ok=True)

    async with websockets.connect(url) as websocket:
        await websocket.send(json.dumps({"assets_ids": assets_ids, "type": "market"}))

        while True:
            m = await websocket.recv()
            if m != "PONG":
                last_time_pong = datetime.datetime.now()
                d = json.loads(m)
                print(f"Polymarket data: {d}")

                # Store the data locally
                timestamp = datetime.datetime.utcnow().isoformat()
                filename = f"polymarket_data/{timestamp}.json"
                with open(filename, 'w') as f:
                    json.dump(d, f)
            else:
                if last_time_pong + datetime.timedelta(seconds=10) < datetime.datetime.now():
                    await websocket.send("PING")

# Betfair data streaming function
def stream_betfair(app_key, username, password, certs_path, market_ids):
    client = APIClient(username, password, app_key=app_key, certs=certs_path)
    client.login()

    listener = StreamListener(max_latency=None)
    stream = client.streaming.create_stream(listener=listener)

    market_filter = streaming_market_filter(market_ids=market_ids)
    stream.subscribe_to_markets(market_filter=market_filter)

    stream.start()

    # Create directory for Betfair data if it doesn't exist
    os.makedirs('betfair_data', exist_ok=True)

    try:
        while True:
            listener.update_cache()
            market_books = listener.market_cache
            for market_id, market_book in market_books.items():
                print(f"Betfair data for market {market_id}: {market_book}")

                # Store the data locally
                timestamp = datetime.datetime.utcnow().isoformat()
                directory = f"betfair_data/{market_id}"
                os.makedirs(directory, exist_ok=True)
                filename = f"{directory}/{timestamp}.json"
                with open(filename, 'w') as f:
                    json.dump(market_book, f, default=str)
            time.sleep(1)
    except KeyboardInterrupt:
        stream.stop()
        client.logout()

# Main function to run both streams
def main():
    # Polymarket asset IDs
    polymarket_assets_ids = [
        "69236923620077691027083946871148646972011131466059644796654161903044970987404",
        "21742633143463906290569050155826241533067272736897614950488156847949938836455"
    ]

    # Betfair credentials (replace with your actual credentials)
    app_key = os.getenv("BF_API_KEY")
    username = os.getenv("BF_LOGIN")
    password = os.getenv("BF_PASS")
    certs_path = 'certs'  # Update with your certificate path
    betfair_market_ids = ["1.176878927"]  # Replace with your Betfair market IDs

    # Start Betfair streaming in a separate thread
    betfair_thread = threading.Thread(
        target=stream_betfair,
        args=(app_key, username, password, certs_path, betfair_market_ids)
    )
    betfair_thread.start()

    # Run Polymarket streaming in the asyncio event loop
    #asyncio.run(stream_polymarket(polymarket_assets_ids))

if __name__ == "__main__":
    main()