[![Get a UNICORN Trading Suite License](https://raw.githubusercontent.com/LUCIT-Systems-and-Development/unicorn-binance-suite/master/images/logo/LUCIT-UTS-License-Offer.png)](https://shop.lucit.services/software/unicorn-trading-suite)

[![GitHub Release](https://img.shields.io/github/release/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api.svg?label=github)](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/releases)
[![GitHub Downloads](https://img.shields.io/github/downloads/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/total?color=blue)](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/releases)
![Anaconda Release](https://img.shields.io/conda/v/lucit/unicorn-bybit-websocket-api?color=blue)
![Anaconda Downloads](https://img.shields.io/conda/dn/lucit/unicorn-bybit-websocket-api?color=blue)
[![PyPi Release](https://img.shields.io/pypi/v/unicorn-bybit-websocket-api?color=blue)](https://pypi.org/project/unicorn-bybit-websocket-api/)
[![PyPi Downloads](https://pepy.tech/badge/unicorn-bybit-websocket-api)](https://pepy.tech/project/unicorn-bybit-websocket-api)
[![License](https://img.shields.io/badge/license-LSOSL-blue)](https://unicorn-bybit-websocket-api.docs.lucit.tech/license.html)
[![Supported Python Version](https://img.shields.io/pypi/pyversions/unicorn_bybit_websocket_api.svg)](https://www.python.org/downloads/)
[![PyPI - Status](https://img.shields.io/pypi/status/unicorn_bybit_websocket_api.svg)](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/issues)
[![codecov](https://codecov.io/gh/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/branch/master/graph/badge.svg?token=5I03AZ3F5S)](https://codecov.io/gh/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api)
[![CodeQL](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/actions/workflows/codeql-analysis.yml)
[![Unit Tests](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/actions/workflows/unit-tests.yml)
[![Build and Publish GH+PyPi](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/actions/workflows/build_wheels.yml/badge.svg)](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/actions/workflows/build_wheels.yml)
[![Build and Publish Anaconda](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/actions/workflows/build_conda.yml/badge.svg)](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/actions/workflows/build_conda.yml)
[![Read the Docs](https://img.shields.io/badge/read-%20docs-yellow)](https://unicorn-bybit-websocket-api.docs.lucit.tech)
[![Read How To`s](https://img.shields.io/badge/read-%20howto-yellow)](https://medium.lucit.tech)
[![Github](https://img.shields.io/badge/source-github-cbc2c8)](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api)
[![Telegram](https://img.shields.io/badge/community-telegram-41ab8c)](https://t.me/unicorndevs)
[![Gitter](https://img.shields.io/badge/community-gitter-41ab8c)](https://gitter.im/unicorn-trading-suite/unicorn-bybit-websocket-api?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Get Free Professional Support](https://img.shields.io/badge/chat-lucit%20support-004166)](https://www.lucit.tech/get-support.html)

[![LUCIT-UBWA-Banner](https://raw.githubusercontent.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/master/images/logo/LUCIT-UBWA-Banner-Readme.png)](https://www.lucit.tech/unicorn-bybit-websocket-api.html)

# UNICORN Bybit WebSocket API

[Description](#description) | [Live Demo](#live-demo) | [Installation](#installation-and-upgrade) | [How To](#howto) |
[Documentation](#documentation) | [Examples](#examples) | [Change Log](#change-log) | [Wiki](#wiki) | [Social](#social) |
[Notifications](#receive-notifications) | [Bugs](#how-to-report-bugs-or-suggest-improvements) | 
[Contributing](#contributing) | [Disclaimer](#disclaimer) | [Commercial Support](#commercial-support)

A Python SDK by [LUCIT](https://www.lucit.tech) to use the Bybit Websocket API`s (+testnet) in a simple, fast, flexible, robust and 
fully-featured way. 

Part of '[UNICORN Trading Suite](https://www.lucit.tech/unicorn-trading-suite.html)'.

[Get help](https://www.lucit.tech/get-support.html) with the integration of the `UNICORN Trading Suite` modules!

## Get a UNICORN Trading Suite License

To run modules of the *UNICORN Trading Suite* you need a [valid license](https://medium.lucit.tech/how-to-obtain-and-use-a-unicorn-trading-suite-license-key-and-run-the-ubs-module-according-to-best-87b0088124a8#4ca4)!

## Receive Data from Bybit WebSockets

### [Create a multiplex websocket connection](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.create_stream) to Bybit with a [`stream_buffer`](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/%60stream_buffer%60) with just 3 lines of code

```
from unicorn_bybit_websocket_api import BybitWebSocketApiManager

ubwa = BybitWebSocketApiManager(exchange="bybit.com")
ubwa.create_stream(channels=['trade', 'kline_1m'], markets=['btcusdt', 'bnbbtc', 'ethbtc'])
```

#### And 4 more lines to print out the data

```
while True:
    oldest_data_from_stream_buffer = ubwa.pop_stream_data_from_stream_buffer()
    if oldest_data_from_stream_buffer:
        print(oldest_data_from_stream_buffer)
```

### Or with a [callback function](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html?highlight=process_stream_data#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.create_stream) just do

```
from unicorn_bybit_websocket_api import BybitWebSocketApiManager

def process_new_receives(stream_data):
    print(str(stream_data))

ubwa = BybitWebSocketApiManager(exchange="bybit.com")
ubwa.create_stream(channels=['trade', 'kline_1m'], 
                   markets=['btcusdt', 'bnbbtc', 'ethbtc'], 
                   process_stream_data=process_new_receives)
```

### Or with an [async callback function](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html?highlight=process_stream_data#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.create_stream) just do

```
from unicorn_bybit_websocket_api import BybitWebSocketApiManager
import asyncio

async def process_new_receives(stream_data):
    print(stream_data)
    await asyncio.sleep(1)

ubwa = BybitWebSocketApiManager()
ubwa.create_stream(channels=['trade', 'kline_1m'],
                   markets=['btcusdt', 'bnbbtc', 'ethbtc'],
                   process_stream_data_async=process_new_receives)
```

### Or await the stream data in an asyncio coroutine

All the methods of data collection presented have their own advantages and disadvantages. However, this is the 
generally recommended method for processing data from streams.

```
from unicorn_bybit_websocket_api import BybitWebSocketApiManager
import asyncio

async def main():
    async def process_asyncio_queue(stream_id=None):
        print(f"Start processing the data from stream '{ubwa.get_stream_label(stream_id)}':")
        while ubwa.is_stop_request(stream_id) is False:
            data = await ubwa.get_stream_data_from_asyncio_queue(stream_id)
            print(data)
            ubwa.asyncio_queue_task_done(stream_id)
    ubwa.create_stream(channels=['trade'],
                       markets=['ethbtc', 'btcusdt'],
                       stream_label="TRADES",
                       process_asyncio_queue=process_asyncio_queue)
    while not ubwa.is_manager_stopping():
            await asyncio.sleep(1)

with BybitWebSocketApiManager(exchange='bybit.com') as ubwa:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\r\nGracefully stopping ...")
    except Exception as e:
        print(f"\r\nERROR: {e}\r\nGracefully stopping ...")
```

Basically that's it, but there are more options.

## [Subscribe](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.subscribe_to_stream) / [unsubscribe](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.unsubscribe_from_stream) new markets and channels

```
markets = ['engbtc', 'zileth']
channels = ['kline_5m', 'kline_15m', 'kline_30m', 'kline_1h', 'kline_12h', 'depth5']

ubwa.subscribe_to_stream(stream_id=stream_id, channels=channels, markets=markets)

ubwa.unsubscribe_from_stream(stream_id=stream_id, markets=markets)

ubwa.unsubscribe_from_stream(stream_id=stream_id, channels=channels)
```

## Stop `ubwa` after usage to avoid memory leaks

When you instantiate UBWA with `with`, `ubwa.stop_manager()` is automatically executed upon exiting the `with`-block.

```
with BybitWebSocketApiManager() as ubwa:
    ubwa.create_stream(channels="trade", markets="btcusdt", stream_label="TRADES")
```

Without `with`, you must explicitly execute `ubwa.stop_manager()` yourself.

```
ubwa.stop_manager()
```

## `stream_signals` - know the state of your streams
Usually you want to know when a stream is working and when it is not. This can be useful to know that your own system is 
currently "blind" and you may want to close open positions to be on the safe side, know that indicators will now provide 
incorrect values or that you have to reload the missing data via REST as an alternative. 

For this purpose, the UNICORN Bybit WebSocket API provides so-called 
[`stream_signals`](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/%60stream_signals%60), 
which are used to tell your code in real time when a stream is connected, when it received its first data record, when 
it was disconnected and stopped, and when the stream cannot be restored.

```
from unicorn_bybit_websocket_api import BybitWebSocketApiManager
import time

def process_stream_signals(signal_type=None, stream_id=None, data_record=None, error_msg=None):
    print(f"Received stream_signal for stream '{ubwa.get_stream_label(stream_id=stream_id)}': "
          f"{signal_type} - {stream_id} - {data_record} - {error_msg}")

with BybitWebSocketApiManager(process_stream_signals=process_stream_signals) as ubwa:
    ubwa.create_stream(channels="trade", markets="btcusdt", stream_label="TRADES")
    time.sleep(2)
    print(f"Waiting 5 seconds and then stop the stream ...")
    time.sleep(5)
```

## More?

[Discover even more possibilities](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html), 
[use this script](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/blob/master/examples/_archive/example_stream_everything.py) 
to stream everything from "bybit.com" or try our [examples](#examples)!

This should be known by everyone using this lib: 

- [Best practice solutions for a maximum stable connection](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/Best-practice-solutions-for-a-maximum-stable-connection!)
- [Do you want consistent data from bybit?](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/discussions/254)

## Description
The Python package [UNICORN Bybit WebSocket API](https://www.lucit.tech/unicorn-bybit-websocket-api.html) 
provides an API to the Bybit Websocket API`s of 
[bybit](https://github.com/bybit-exchange/bybit-official-api-docs) 
([+Testnet](https://testnet.bybit.com/)), 
[Bybit Margin](https://bybit-docs.github.io/apidocs/spot/en/#user-data-streams) 
([+Testnet](https://testnet.bybit.com/)), 


### What are the benefits of the UNICORN Bybit WebSocket API?
- Fully managed websockets and 100% auto-reconnect! Also handles maintenance windows!

- No memory leaks from Python version 3.7 to 3.12!

- The full [UBS stack](https://www.lucit.tech/unicorn-trading-suite.html) is delivered as a compiled C extension for 
  maximum performance.

- [Supported exchanges](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/Bybit-websocket-endpoint-configuration-overview): 

| Exchange                                       | Exchange string     | WS                                                                                                                                   | WS API                                                                                                                               |
|------------------------------------------------|---------------------|--------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| [Bybit](https://www.bybit.com)                 | `bybit.com`         | ![yes](https://raw.githubusercontent.com/lucit-systems-and-development/unicorn-bybit-websocket-api/master/images/misc/ok-icon.png) | ![yes](https://raw.githubusercontent.com/lucit-systems-and-development/unicorn-bybit-websocket-api/master/images/misc/ok-icon.png) |
| [Bybit Testnet](https://testnet.bybit.com) | `bybit.com-testnet` | ![yes](https://raw.githubusercontent.com/lucit-systems-and-development/unicorn-bybit-websocket-api/master/images/misc/ok-icon.png) | ![yes](https://raw.githubusercontent.com/lucit-systems-and-development/unicorn-bybit-websocket-api/master/images/misc/ok-icon.png) |


- Streams are processing asynchronous/concurrent (Python asyncio) and each stream is started in a separate thread, so 
you don't need to deal with asyncio in your code! But you can consume with 
[`await`](https://unicorn-bybit-websocket-api.docs.lucit.tech/readme.html#or-await-the-webstream-data-in-an-asyncio-task)
, if you want!

- Supports 
[subscribe](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.subscribe_to_stream)/[unsubscribe](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.unsubscribe_from_stream)
on all exchanges! (Take a look to the max supported subscriptions per stream in the [endpoint configuration overview](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/Bybit-websocket-endpoint-configuration-overview)!)

- [UNICORN Bybit WebSocket API](https://www.lucit.tech/unicorn-bybit-websocket-api.html) respects Bybit's API guidelines and protects you from avoidable reconnects and bans.

- Support for multiple private `!userData` streams with different `api_key` and `api_secret`. 
  ([example_multiple_userdata_streams.py](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/blob/master/examples/_archive/example_multiple_userdata_streams.py))

- [Pick up the received data from the `stream_buffer`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html?highlight=get_stream_info#unicorn_bybit_websocket_api.unicorn_bybit_websocket_api_manager.BybitWebSocketApiManager.pop_stream_data_from_stream_buffer) ([FIFO or LIFO](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/%60stream_buffer%60)) - 
if you can not store your data in cause of a temporary technical issue, you can 
[kick back the data to the `stream_buffer`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html?highlight=get_stream_info#unicorn_bybit_websocket_api.unicorn_bybit_websocket_api_manager.BybitWebSocketApiManager.add_to_stream_buffer) 
which stores the receives in the RAM till you are able to process the data in the normal way again. 
[Learn more!](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/%60stream_buffer%60)

- Use separate `stream_buffers` for 
[specific streams](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/blob/master/examples/_archive/example_stream_buffer_extended.py) 
or 
[users](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/blob/master/examples/_archive/example_multiple_userdata_streams.py)!

- Watch the `stream_signals` to receive `CONNECT`, `FIRST_RECEIVED_DATA`, `DISCONNECT`, `STOP` and 
  `STREAM_UNREPAIRABLE` signals from your streams! [Learn more!](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/%60stream_signals%60)

- Get the received data unchanged as received, as Python dictionary or converted with 
[UnicornFy](https://github.com/LUCIT-Systems-and-Development/unicorn-fy) into well-formed Python dictionaries. Use the `output`
parameter of 
[`create_stream()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html?highlight=create_stream#unicorn_bybit_websocket_api.unicorn_bybit_websocket_api_manager.BybitWebSocketApiManager.create_stream) 
to control the output format.

- Helpful management features like 
[`clear_asyncio_queue()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.clear_asyncio_queue), 
[`clear_stream_buffer()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.clear_stream_buffer), 
[`get_bybit_api_status()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_bybit_api_status), 
[`get_current_receiving_speed()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_current_receiving_speed), 
[`get_errors_from_endpoints()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_errors_from_endpoints), 
[`get_limit_of_subscriptions_per_stream()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_limit_of_subscriptions_per_stream), 
[`get_request_id()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_request_id), 
[`get_result_by_request_id()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_result_by_request_id),
[`get_results_from_endpoints()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_results_from_endpoints), 
[`get_stream_buffer_length()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_stream_buffer_length), 
[`get_stream_info()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_stream_info), 
[`get_stream_list()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_stream_list), 
[`get_stream_id_by_label()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_stream_id_by_label), 
[`get_stream_statistic()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_stream_statistic), 
[`get_stream_subscriptions()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_stream_subscriptions), 
[`get_version()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_version), 
[`is_update_available()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.is_update_availabe), 
[`get_stream_data_from_asyncio_queue()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_stream_data_from_asyncio_queue), 
[`pop_stream_data_from_stream_buffer()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.pop_stream_data_from_stream_buffer), 
[`print_summary()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.print_summary), 
[`replace_stream()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.replace_stream), 
[`set_stream_label()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.set_stream_label), 
[`set_ringbuffer_error_max_size()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.set_ringbuffer_error_max_size), 
[`subscribe_to_stream()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.subscribe_to_stream), 
[`stop_stream()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.stop_stream),
[`unsubscribe_from_stream()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.unsubscribe_from_stream), 
[`wait_till_stream_has_started()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.wait_till_stream_has_started) 
and many more! Explore them [here](https://unicorn-bybit-websocket-api.docs.lucit.tech/modules.html).

- Monitor the status of the created `BybitWebSocketApiManager()` instance within your code with 
[`get_monitoring_status_plain()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html?highlight=plain#unicorn_bybit_websocket_api.unicorn_bybit_websocket_api_manager.BybitWebSocketApiManager.get_monitoring_status_plain)
and specific streams with 
[`get_stream_info()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_stream_info).

- Available as a package via `pip` and `conda` as precompiled C extension with stub files for improved Intellisense 
  functions and source code for easier debugging of the source code. [To the installation.](#installation-and-upgrade)

- Integration of [test cases](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/actions/workflows/unit-tests.yml) and [examples](#examples).

- Customizable base URL.

- *Socks5 Proxy* support:

  ```
  ubwa = BybitWebSocketApiManager(exchange="bybit.com", socks5_proxy_server="127.0.0.1:9050") 
  ```
  
  Read the [docs](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager)
  or this [how to](https://medium.com/@oliverzehentleitner/how-to-connect-to-bybit-com-websockets-using-python-via-a-socks5-proxy-3c5a3e063f12) 
  for more information or try 
  [example_socks5_proxy.py](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/blob/master/examples/_archive/example_socks5_proxy.py).

- Excessively tested on Linux, Mac and Windows on x86, arm32, arm64, ...

If you like the project, please [![star](https://raw.githubusercontent.com/lucit-systems-and-development/unicorn-bybit-websocket-api/master/images/misc/gh-star.png)](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/stargazers) it on 
[GitHub](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api)!

## Installation and Upgrade
The module requires Python 3.7 and runs smoothly up to and including Python 3.12.

Anaconda packages are available from Python version 3.8 and higher, but only in the latest version!

For the PyPy interpreter we offer packages via PyPi only from Python version 3.9 and higher.

The current dependencies are listed [here](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/blob/master/requirements.txt).

If you run into errors during the installation take a look [here](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-suite/wiki/Installation).

### Packages are created automatically with GitHub Actions
When a new release is to be created, we start two GitHubActions: 

- [Build and Publish Anaconda](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/actions/workflows/build_conda.yml)
- [Build and Publish GH+PyPi](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/actions/workflows/build_wheels.yml) 

Both start virtual Windows/Linux/Mac servers provided by GitHub in the cloud with preconfigured environments and 
create the respective compilations and stub files, pack them into wheels and conda packages and then publish them on 
GitHub, PYPI and Anaconda. This is a transparent method that makes it possible to trace the source code behind a 
compilation.

### A Cython binary, PyPy or source code based CPython wheel of the latest version with `pip` from [PyPI](https://pypi.org/project/unicorn-bybit-rest-api/)
Our [Cython](https://cython.org/) and [PyPy](https://www.pypy.org/) Wheels are available on [PyPI](https://pypi.org/), 
these wheels offer significant advantages for Python developers:

- ***Performance Boost with Cython Wheels:*** Cython is a programming language that supplements Python with static typing and C-level performance. By compiling 
  Python code into C, Cython Wheels can significantly enhance the execution speed of Python code, especially in 
  computationally intensive tasks. This means faster runtimes and more efficient processing for users of our package. 

- ***PyPy Wheels for Enhanced Efficiency:*** PyPy is an alternative Python interpreter known for its speed and efficiency. It uses Just-In-Time (JIT) compilation, 
  which can dramatically improve the performance of Python code. Our PyPy Wheels are tailored for compatibility with 
  PyPy, allowing users to leverage this speed advantage seamlessly.

Both Cython and PyPy Wheels on PyPI make the installation process simpler and more straightforward. They ensure that 
you get the optimized version of our package with minimal setup, allowing you to focus on development rather than 
configuration.

On Raspberry Pi and other architectures for which there are no pre-compiled versions, the package can still be 
installed with PIP. PIP then compiles the package locally on the target system during installation. Please be patient, 
this may take some time!

#### Installation
`pip install unicorn-bybit-websocket-api`

#### Update
`pip install unicorn-bybit-websocket-api --upgrade`

### A Conda Package of the latest version with `conda` from [Anaconda](https://anaconda.org/lucit)
The `unicorn-bybit-websocket-api` package is also available as a Cython version for the `linux-64`, `osx-64` 
and `win-64` architectures with [Conda](https://docs.conda.io/en/latest/) through the 
[`lucit` channel](https://anaconda.org/lucit). 

For optimal compatibility and performance, it is recommended to source the necessary dependencies from the 
[`conda-forge` channel](https://anaconda.org/conda-forge). 

#### Installation
```
conda config --add channels conda-forge
conda config --add channels lucit
conda install -c lucit unicorn-bybit-websocket-api
```

#### Update
`conda update -c lucit unicorn-bybit-websocket-api`

### From source of the latest release with PIP from [GitHub](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api)
#### Linux, macOS, ...
Run in bash:

`pip install https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/archive/$(curl -s https://api.github.com/repos/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/releases/latest | grep -oP '"tag_name": "\K(.*)(?=")').tar.gz --upgrade`

#### Windows
Use the below command with the version (such as 2.8.0) you determined 
[here](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/releases/latest):

`pip install https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/archive/2.8.0.tar.gz --upgrade`
### From the latest source (dev-stage) with PIP from [GitHub](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api)
This is not a release version and can not be considered to be stable!

`pip install https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/tarball/master --upgrade`

## Change Log
[https://unicorn-bybit-websocket-api.docs.lucit.tech/changelog.html](https://unicorn-bybit-websocket-api.docs.lucit.tech/changelog.html)

## Documentation
- [General](https://unicorn-bybit-websocket-api.docs.lucit.tech)
- [Modules](https://unicorn-bybit-websocket-api.docs.lucit.tech/modules.html)

## Examples
- [Look here!](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/tree/master/examples/)

## Howto
- [How to Obtain and Use a Unicorn Trading Suite License Key and Run the UBS Module According to Best Practice](https://medium.lucit.tech/how-to-obtain-and-use-a-unicorn-trading-suite-license-key-and-run-the-ubs-module-according-to-best-87b0088124a8)


## Project Homepage
[https://www.lucit.tech/unicorn-bybit-websocket-api.html](https://www.lucit.tech/unicorn-bybit-websocket-api.html)

## Wiki
[https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki)

## Social
- [Discussions](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/discussions)
- [Gitter](https://gitter.im/unicorn-trading-suite/unicorn-bybit-websocket-api)
- [https://t.me/unicorndevs](https://t.me/unicorndevs)

## Receive Notifications
To receive notifications on available updates you can 
[![watch](https://raw.githubusercontent.com/lucit-systems-and-development/unicorn-bybit-websocket-api/master/images/misc/gh-watch.png)](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/watchers) 
the repository on [GitHub](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api), write your 
[own script](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/blob/master/examples/_archive/example_version_of_this_package.py) 
with using 
[`is_update_available()`](https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.is_update_availabe) 
or you use the 
[monitoring API service](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/UNICORN-Monitoring-API-Service).

Follow us on [LinkedIn](https://www.linkedin.com/company/lucit-systems-and-development), 
[X](https://twitter.com/LUCIT_SysDev) or [Facebook](https://www.facebook.com/lucit.systems.and.development)!

To receive news (like inspection windows/maintenance) about the Bybit API`s subscribe to their telegram groups: 

- [https://t.me/BybitEnglish/2486566](https://t.me/BybitEnglish/2486566)

## How to report Bugs or suggest Improvements?
[List of planned features](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/issues?q=is%3Aissue+is%3Aopen+label%3Aenhancement) - click ![thumbs-up](https://raw.githubusercontent.com/lucit-systems-and-development/unicorn-bybit-websocket-api/master/images/misc/thumbup.png) if you need one of them or suggest a new feature!

Before you report a bug, [try the latest release](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api#installation-and-upgrade). If the issue still exists, provide the error trace, OS 
and Python version and explain how to reproduce the error. A demo script is appreciated.

If you don't find an issue related to your topic, please open a new [issue](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/issues)!

[Report a security bug!](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/security/policy)

## Contributing
[UNICORN Bybit WebSocket API](https://www.lucit.tech/unicorn-bybit-websocket-api.html) is an open 
source project which welcomes contributions which can be anything from simple documentation fixes and reporting dead links to new features. To 
contribute follow 
[this guide](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/blob/master/CONTRIBUTING.md).
 
### Contributors
[![Contributors](https://contributors-img.web.app/image?repo=oliver-zehentleitner/unicorn-bybit-websocket-api)](https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/graphs/contributors)

We ![love](https://raw.githubusercontent.com/lucit-systems-and-development/unicorn-bybit-websocket-api/master/images/misc/heart.png) open source!

## Disclaimer
This project is for informational purposes only. You should not construe this information or any other material as 
legal, tax, investment, financial or other advice. Nothing contained herein constitutes a solicitation, recommendation, 
endorsement or offer by us or any third party provider to buy or sell any securities or other financial instruments in 
this or any other jurisdiction in which such solicitation or offer would be unlawful under the securities laws of such 
jurisdiction.

### If you intend to use real money, use it at your own risk!

Under no circumstances will we be responsible or liable for any claims, damages, losses, expenses, costs or liabilities 
of any kind, including but not limited to direct or indirect damages for loss of profits.

## Commercial Support

[![Get professional and fast support](https://raw.githubusercontent.com/LUCIT-Systems-and-Development/unicorn-trading-suite/master/images/support/LUCIT-get-professional-and-fast-support.png)](https://www.lucit.tech/get-support.html)

***Do you need a developer, operator or consultant?*** [Contact us](https://www.lucit.tech/contact.html) for a non-binding initial consultation!
