from unicorn_bybit_websocket_api import BybitWebSocketApiManager

bybit_wsm = BybitWebSocketApiManager(exchange="bybit.com")
bybit_wsm.create_stream(endpoint="public/linear", channels=['kline.1'], markets=['btcusdt', 'ethusdt'])

while True:
    oldest_data_from_stream_buffer = bybit_wsm.pop_stream_data_from_stream_buffer()
    if oldest_data_from_stream_buffer:
        print(oldest_data_from_stream_buffer)
