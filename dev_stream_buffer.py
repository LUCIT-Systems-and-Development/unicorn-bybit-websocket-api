import asyncio
import logging
import os
import threading
import time
from unicorn_bybit_websocket_api import BybitWebSocketApiManager


logging.getLogger("unicorn_bybit_websocket_api")
logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")


async def main():
    print("Waiting 30 seconds, then we start flushing the stream_buffer!")
    symbols = []
    rest_data = bybit_wsm.restclient.get_symbols()
    for symbol in rest_data['result']:
        if symbol['quote_currency'] == 'USDT' and symbol['status'] == 'Trading':
            symbols.append(symbol['name'])
    bybit_wsm.create_stream(endpoint="public/linear",
                            channels="kline.1",
                            markets=symbols,
                            stream_label="KLINE_1m")
    worker_thread = threading.Thread(target=print_stream_data_from_stream_buffer, args=(bybit_wsm,))
    worker_thread.start()
    while not bybit_wsm.is_manager_stopping():
        bybit_wsm.print_summary()
        await asyncio.sleep(1)


def print_stream_data_from_stream_buffer(wsm):
    time.sleep(30)
    while True:
        if wsm.is_manager_stopping():
            exit(0)
        oldest_stream_data_from_stream_buffer = wsm.pop_stream_data_from_stream_buffer()
        if oldest_stream_data_from_stream_buffer is None:
            time.sleep(0.1)
        else:
            try:
                # remove # to activate the print function:
                # print(oldest_stream_data_from_stream_buffer)
                pass
            except KeyError:
                # Any kind of error...
                # not able to process the data? write it back to the stream_buffer
                wsm.add_to_stream_buffer(oldest_stream_data_from_stream_buffer)


with BybitWebSocketApiManager(exchange='bybit.com') as bybit_wsm:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\r\nGracefully stopping ...")
