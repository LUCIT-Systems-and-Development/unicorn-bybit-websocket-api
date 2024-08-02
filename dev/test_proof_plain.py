import json
import logging
import os
import requests
import time
import websocket
import threading

logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")


url = "wss://stream.bybit.com/v5/public/linear"


def on_message(ws, message):
    try:
        data = json.loads(message)
        if "topic" in data and "kline" in data["topic"]:
            print("Received OHLCV data:")
        else:
            print("Received some data:")
        print(data)
    except json.JSONDecodeError:
        print("Received non-JSON message")


def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed.")


def on_open(ws):
    symbols = []
    response = requests.get("https://api.bybit.com/v2/public/symbols")
    data = response.json()
    for symbol in data['result']:
        if symbol['quote_currency'] == 'USDT' and symbol['status'] == 'Trading':
            symbols.append(symbol['name'])
    params = {
        "op": "subscribe",
        "args": [f"kline.1.{symbol}" for symbol in symbols]
    }
    ws.send(json.dumps(params))
    print(f"Subscribed to {', '.join(symbols)} OHLCV data. ({len(str(params))} chars)")


def run():
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()


ws_thread = threading.Thread(target=run)
ws_thread.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Terminating the WebSocket connection...")
    # Die WebSocket-Verbindung kann hier nicht sauber geschlossen werden, da run_forever blockiert.
    # In einer produktiven Anwendung sollte eine Möglichkeit zum sauberen Schließen implementiert werden.
    ws_thread.join()
