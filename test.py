import logging
import os
import time

from unicorn_bybit_websocket_api import BybitWebSocketApiManager


logging.getLogger("unicorn_bybit_websocket_api")
logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")

bybit_ws = BybitWebSocketApiManager(exchange="bybit.com")

bybit_ws.create_stream()

while True:
    time.sleep(1)
