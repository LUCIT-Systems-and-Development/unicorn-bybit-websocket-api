# unicorn-bybit-websocket-api Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to 
[Semantic Versioning](http://semver.org/).

[Discussions about unicorn-bybit-websocket-api releases!](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/discussions/categories/releases)

[How to upgrade to the latest version!](https://unicorn-bybit-websocket-api.docs.lucit.tech/readme.html#installation-and-upgrade)

## 0.1.0.dev (development stage/unreleased/unstable)

## 0.1.0
BETA VERSION

The core functions work. Websocket connections to public endpoints can be established and are stable. (No long-term tests!)

Example usage:
````
from unicorn_bybit_websocket_api import BybitWebSocketApiManager

bybit_wsm = BybitWebSocketApiManager(exchange="bybit.com")
bybit_wsm.create_stream(endpoint="public/linear", channels=['kline.1'], markets=['btcusdt', 'ethusdt'])

while True:
    oldest_data_from_stream_buffer = bybit_wsm.pop_stream_data_from_stream_buffer()
    if oldest_data_from_stream_buffer:
        print(oldest_data_from_stream_buffer)
````
