from unicorn_bybit_websocket_api import BybitWebSocketApiManager

ubbwa = BybitWebSocketApiManager(exchange="bybit.com")

ubbwa.create_stream()

while True:
    ubbwa.print_summary()

