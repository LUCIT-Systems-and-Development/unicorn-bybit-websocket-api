import asyncio
import logging
import os
from unicorn_bybit_websocket_api import BybitWebSocketApiManager


logging.getLogger("unicorn_bybit_websocket_api")
logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")


async def main():
    async def process_asyncio_queue(stream_id=None):
        print(f"Start processing the data from stream '{bybit_wsm.get_stream_label(stream_id)}':")
        while bybit_wsm.is_stop_request(stream_id) is False:
            data = await bybit_wsm.get_stream_data_from_asyncio_queue(stream_id)
            print(data)
            bybit_wsm.asyncio_queue_task_done(stream_id)

    symbols = []
    rest_data = bybit_wsm.restclient.get_symbols()
    for symbol in rest_data['result']:
        if symbol['quote_currency'] == 'USDT' and symbol['status'] == 'Trading':
            symbols.append(symbol['name'])
    bybit_wsm.create_stream(endpoint="public/linear",
                            channels="kline.1",
                            markets=symbols,
                            stream_label="KLINE_1m",
                            process_asyncio_queue=process_asyncio_queue)
    while not bybit_wsm.is_manager_stopping():
        bybit_wsm.print_summary()
        await asyncio.sleep(1)


with BybitWebSocketApiManager(exchange='bybit.com') as bybit_wsm:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\r\nGracefully stopping ...")
