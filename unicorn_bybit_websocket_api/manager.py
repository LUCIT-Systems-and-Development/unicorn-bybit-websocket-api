#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯
#
# File: unicorn_bybit_websocket_api/manager.py
#
# Part of ‘UNICORN Bybit WebSocket API’
# Project website: https://www.lucit.tech/unicorn-bybit-websocket-api.html
# Github: https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api
# Documentation: https://unicorn-bybit-websocket-api.docs.lucit.tech
# PyPI: https://pypi.org/project/unicorn-bybit-websocket-api
# LUCIT Online Shop: https://shop.lucit.services/software
#
# License: LSOSL - LUCIT Synergetic Open Source License
# https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/blob/master/LICENSE
#
# Author: LUCIT Systems and Development
#
# Copyright (c) 2024-2024, LUCIT Systems and Development (https://www.lucit.tech)
# All rights reserved.

from .licensing_manager import LucitLicensingManager, NoValidatedLucitLicense
from .connection_settings import CONNECTION_SETTINGS
from .exceptions import *
from .restclient import BybitWebSocketApiRestclient
from .sockets import BybitWebSocketApiSocket
from collections import deque
from datetime import datetime, timezone
from operator import itemgetter
from typing import Optional, Union, Callable, List, Set
try:
    # python <=3.7 support
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

import asyncio
import colorama
import copy
import cython
import logging
import hmac
import hashlib
import os
import platform
import psutil
import re
import requests
import ssl
import sys
import threading
import time
import traceback
import uuid
import ujson as json
import websockets


__app_name__: str = "unicorn-bybit-websocket-api"
__version__: str = "0.1.0.dev"
__logger__: logging.getLogger = logging.getLogger("unicorn_bybit_websocket_api")

logger = __logger__


class BybitWebSocketApiManager(threading.Thread):
    """
    A Python SDK by LUCIT to use the Bybit Websocket API`s in a simple, fast, flexible, robust and fully-featured way.

    Bybit.com API documentation:

        - https://bybit-exchange.github.io/docs/


    :param process_asyncio_queue: Insert your Asyncio function into the same AsyncIO loop in which the websocket data
                                  is received. This method guarantees the fastest possible asynchronous processing of
                                  the data in the correct receiving sequence.
                                  https://unicorn-bybit-websocket-api.docs.lucit.tech/readme.html#or-await-the-webstream-data-in-an-asyncio-coroutine
    :type process_asyncio_queue: Optional[Callable]
    :param process_stream_data: Provide a function/method to process the received webstream data (callback).
                                The function will be called instead of
                                `add_to_stream_buffer() <unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.add_to_stream_buffer>`__
                                like `process_stream_data(stream_data, stream_buffer_name)` where
                                `stream_data` contains the raw_stream_data. If not provided, the raw stream_data will
                                get stored in the stream_buffer or provided to a specific callback function of
                                `create_stream()`! `How to read from stream_buffer!
                                <https://unicorn-bybit-websocket-api.docs.lucit.tech/README.html#and-4-more-lines-to-print-the-receives>`__
    :type process_stream_data: Optional[Callable]
    :param process_stream_data_async: Provide an asyncio function/method to process the received webstream data
                                (callback). The function will be called instead of
                                `add_to_stream_buffer() <unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.add_to_stream_buffer>`__
                                like `process_stream_data(stream_data, stream_buffer_name)` where
                                `stream_data` contains the raw_stream_data. If not provided, the raw stream_data will
                                get stored in the stream_buffer or provided to a specific callback function of
                                `create_stream()`! `How to read from stream_buffer!
                                <https://unicorn-bybit-websocket-api.docs.lucit.tech/README.html#and-4-more-lines-to-print-the-receives>`__
    :type process_stream_data_async: Optional[Callable]
    :param exchange: Select bybit.com, bybit.com-testnet (default: bybit.com)
    :type exchange: str
    :param warn_on_update: set to `False` to disable the update warning of UBBWA and also in UBBRA used as submodule.
    :type warn_on_update: bool
    :param restart_timeout: A stream restart must be successful within this time, otherwise a new restart will be
                            initialized. Default is 6 seconds.
    :type restart_timeout: int
    :param show_secrets_in_logs: set to True to show secrets like listen_key, api_key or api_secret in log file
                                 (default=False)
    :type show_secrets_in_logs: bool
    :param output_default: set to "dict" to convert the received raw data to a python dict
                           - otherwise with the default setting "raw_data" the output remains unchanged and gets
                           delivered as received from the endpoints. Change this for a specific stream with the `output`
                           parameter of `create_stream()` and `replace_stream()`
    :type output_default: str
    :param enable_stream_signal_buffer: set to True to enable the
                                        `stream_signal_buffer <https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/%60stream_signal_buffer%60>`__
                                        and receive information about
                                        disconnects and reconnects to manage a restore of the lost data during the
                                        interruption or to recognize your bot got blind.
    :type enable_stream_signal_buffer: bool
    :param disable_colorama: set to True to disable the use of `colorama <https://pypi.org/project/colorama/>`__
    :type disable_colorama: bool
    :param stream_buffer_maxlen: Set a max len for the generic `stream_buffer`. This parameter can also be used within
                                 `create_stream()` for a specific `stream_buffer`.
    :type stream_buffer_maxlen: int or None
    :param process_stream_signals: Provide a function/method to process the received stream signals. The function is
                                   running inside an asyncio loop and will be called instead of
                                   `add_to_stream_signal_buffer() <unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.add_to_stream_signal_buffer>`__
                                   like `process_stream_data(signal_type=False, stream_id=False, data_record=False)`.
    :type process_stream_signals: function
    :param auto_data_cleanup_stopped_streams: The parameter "auto_data_cleanup_stopped_streams=True" can be used to
                                              inform the UBBWA instance that all remaining data of a stopped stream
                                              should be automatically and completely deleted.
    :type auto_data_cleanup_stopped_streams: bool
    :param close_timeout_default: The `close_timeout` parameter defines a maximum wait time in seconds for
                                completing the closing handshake and terminating the TCP connection.
                                This parameter is passed through to the `websockets.client.connect()
                                <https://websockets.readthedocs.io/en/stable/topics/design.html?highlight=close_timeout#closing-handshake>`__
    :type close_timeout_default: int
    :param ping_interval_default: Once the connection is open, a `Ping frame` is sent every
                                `ping_interval` seconds. This serves as a keepalive. It helps keeping
                                the connection open, especially in the presence of proxies with short
                                timeouts on inactive connections. Set `ping_interval` to `None` to
                                disable this behavior.
                                This parameter is passed through to the `websockets.client.connect()
                                <https://websockets.readthedocs.io/en/stable/topics/timeouts.html?highlight=ping_interval#keepalive-in-websock ets>`__
    :type ping_interval_default: int
    :param ping_timeout_default: If the corresponding `Pong frame` isn't received within
                               `ping_timeout` seconds, the connection is considered unusable and is closed with
                               code 1011. This ensures that the remote endpoint remains responsive. Set
                               `ping_timeout` to `None` to disable this behavior.
                               This parameter is passed through to the `websockets.client.connect()
                               <https://websockets.readthedocs.io/en/stable/topics/timeouts.html?highlight=ping_timeout#keepalive-in-websockets>`__
    :type ping_timeout_default: int
    :param high_performance: Set to True makes `create_stream()` a non-blocking function
    :type high_performance:  bool
    :param debug: If True the lib adds additional information to logging outputs
    :type debug:  bool
    :param restful_base_uri: Override `restful_base_uri`. Example: `https://127.0.0.1`
    :type restful_base_uri:  str
    :param websocket_base_uri: Override `websocket_base_uri`. Example: `ws://127.0.0.1:8765/`
    :type websocket_base_uri:  str
    :param max_subscriptions_per_stream_spot: Override the `max_subscriptions_per_stream_spot` value. Example: 1024
    :type max_subscriptions_per_stream_spot:  int
    :param socks5_proxy_server: Set this to activate the usage of a socks5 proxy. Example: '127.0.0.1:9050'
    :type socks5_proxy_server:  str
    :param socks5_proxy_user: Set this to activate the usage of a socks5 proxy user. Example: 'alice'
    :type socks5_proxy_user:  str
    :param socks5_proxy_pass: Set this to activate the usage of a socks5 proxy password.
    :type socks5_proxy_pass:  str
    :param socks5_proxy_ssl_verification: Set to `False` to disable SSL server verification. Default is `True`.
    :type socks5_proxy_ssl_verification:  bool
    :param lucit_api_secret: The `api_secret` of your UNICORN Bybit Suite license from
                             https://shop.lucit.services/software/unicorn-trading-suite
    :type lucit_api_secret:  str
    :param lucit_license_ini: Specify the path including filename to the config file (ex: `~/license_a.ini`). If not
                              provided lucitlicmgr tries to load a `lucit_license.ini` from `/home/oliver/.lucit/`.
    :type lucit_license_ini:  str
    :param lucit_license_profile: The license profile to use. Default is 'LUCIT'.
    :type lucit_license_profile:  str
    :param lucit_license_token: The `license_token` of your UNICORN Bybit Suite license from
                                https://shop.lucit.services/software/unicorn-trading-suite
    :type lucit_license_token:  str
    """

    def __init__(self,
                 process_stream_data: Optional[Callable] = None,
                 process_stream_data_async: Optional[Callable] = None,
                 process_asyncio_queue: Optional[Callable] = None,
                 exchange: str = "bybit.com",
                 warn_on_update: bool = True,
                 restart_timeout: int = 6,
                 show_secrets_in_logs: bool = False,
                 output_default: Optional[Literal['dict', 'raw_data']] = "raw_data",
                 enable_stream_signal_buffer: bool = False,
                 disable_colorama: bool = False,
                 stream_buffer_maxlen: Optional[int] = None,
                 process_stream_signals=None,
                 close_timeout_default: int = 1,
                 ping_interval_default: int = 5,
                 ping_timeout_default: int = 10,
                 high_performance: bool = False,
                 debug: bool = False,
                 restful_base_uri: str = None,
                 websocket_base_uri: str = None,
                 max_subscriptions_per_stream_spot: Optional[int] = None,
                 max_subscriptions_per_stream_linear: Optional[int] = None,
                 max_subscriptions_per_stream_inverse: Optional[int] = None,
                 max_subscriptions_per_stream_option: Optional[int] = None,
                 socks5_proxy_server: str = None,
                 socks5_proxy_user: str = None,
                 socks5_proxy_pass: str = None,
                 socks5_proxy_ssl_verification: bool = True,
                 auto_data_cleanup_stopped_streams: bool = False,
                 lucit_api_secret: str = None,
                 lucit_license_ini: str = None,
                 lucit_license_profile: str = None,
                 lucit_license_token: str = None):
        threading.Thread.__init__(self)
        self.name = __app_name__
        self.version = __version__
        self.stop_manager_request = False
        self.auto_data_cleanup_stopped_streams = auto_data_cleanup_stopped_streams
        logger.info(f"New instance of {self.get_user_agent()}-{'compiled' if cython.compiled else 'source'} on "
                    f"{str(platform.system())} {str(platform.release())} for exchange {exchange} started ...")
        self.debug = debug
        logger.info(f"Debug is {self.debug}")

        self.lucit_api_secret = lucit_api_secret
        self.lucit_license_ini = lucit_license_ini
        self.lucit_license_profile = lucit_license_profile
        self.lucit_license_token = lucit_license_token
        self.llm = LucitLicensingManager(api_secret=self.lucit_api_secret,
                                         license_ini=self.lucit_license_ini,
                                         license_profile=self.lucit_license_profile,
                                         license_token=self.lucit_license_token,
                                         parent_shutdown_function=self.stop_manager,
                                         program_used=self.name,
                                         needed_license_type="UNICORN-BINANCE-SUITE",
                                         start=True)
        licensing_exception = self.llm.get_license_exception()
        if licensing_exception is not None:
            raise NoValidatedLucitLicense(licensing_exception)

        self.disable_colorama = disable_colorama
        if self.disable_colorama is not True:
            logger.info(f"Initiating `colorama_{colorama.__version__}`")
            colorama.init()
        logger.info(f"Using `websockets_{websockets.__version__}`")
        self.specific_process_asyncio_queue = {}
        self.specific_process_stream_data = {}
        self.specific_process_stream_data_async = {}
        self.process_asyncio_queue: Optional[Callable] = None
        self.process_stream_data: Optional[Callable] = None
        self.process_stream_data_async: Optional[Callable] = None
        if process_asyncio_queue is not None:
            logger.info(f"Using `asyncio_queue` ...")
            self.process_asyncio_queue: Optional[Callable] = process_asyncio_queue
        elif process_stream_data is not None:
            logger.info(f"Using `process_stream_data` ...")
            self.process_stream_data: Optional[Callable] = process_stream_data
        elif process_stream_data_async is not None:
            logger.info(f"Using `process_stream_data_async` ...")
            self.process_stream_data_async: Optional[Callable] = process_stream_data_async
        else:
            logger.info(f"Using `stream_buffer` ...")
        if process_stream_signals is None:
            # no special method to process stream signals provided, so we use add_to_stream_signal_buffer:
            self.process_stream_signals = self.add_to_stream_signal_buffer
            logger.info(f"Using `stream_signal_buffer`")
        else:
            # use the provided method to process stream signals:
            self.process_stream_signals = process_stream_signals
            logger.info(f"Using `process_stream_signals` ...")
        self.enable_stream_signal_buffer = enable_stream_signal_buffer
        if self.enable_stream_signal_buffer is True:
            logger.info(f"Enabled `stream_signal_buffer` ...")

        self.exchange = exchange
        self.stream_list = {}
        self.stream_list_lock = threading.Lock()

        if exchange not in CONNECTION_SETTINGS:
            error_msg = f"Unknown exchange '{str(exchange)}'! List of supported exchanges:\r\n" \
                        f"https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/" \
                        f"Bybit-websocket-endpoint-configuration-overview"
            logger.critical(error_msg)
            self.stop_manager()
            raise UnknownExchange(error_msg=error_msg)

        self.websocket_base_uri = websocket_base_uri or CONNECTION_SETTINGS[self.exchange][0]
        self.api_version = CONNECTION_SETTINGS[self.exchange][1]
        self.args_limit = CONNECTION_SETTINGS[self.exchange][2]
        self.max_subscriptions_per_stream_spot = max_subscriptions_per_stream_spot or CONNECTION_SETTINGS[self.exchange][3]
        self.max_subscriptions_per_stream_linear = max_subscriptions_per_stream_linear or CONNECTION_SETTINGS[self.exchange][4]
        self.max_subscriptions_per_stream_inverse = max_subscriptions_per_stream_inverse or CONNECTION_SETTINGS[self.exchange][5]
        self.max_subscriptions_per_stream_option = max_subscriptions_per_stream_option or CONNECTION_SETTINGS[self.exchange][6]

        self.socks5_proxy_server = socks5_proxy_server
        if socks5_proxy_server is None:
            self.socks5_proxy_address = None
            self.socks5_proxy_user: Optional[str] = None
            self.socks5_proxy_pass: Optional[str] = None
            self.socks5_proxy_port = None
        else:
            # Prepare Socks Proxy usage
            self.socks5_proxy_ssl_verification = socks5_proxy_ssl_verification
            self.socks5_proxy_user = socks5_proxy_user
            self.socks5_proxy_pass = socks5_proxy_pass
            self.socks5_proxy_address, self.socks5_proxy_port = socks5_proxy_server.split(":")
            websocket_ssl_context = ssl.SSLContext()
            if self.socks5_proxy_ssl_verification is False:
                websocket_ssl_context.verify_mode = ssl.CERT_NONE
                websocket_ssl_context.check_hostname = False
            self.websocket_ssl_context = websocket_ssl_context

        self.asyncio_queue = {}
        self.all_subscriptions_number = 0
        self.bybit_api_status = {'weight': None,
                                 'timestamp': 0,
                                 'status_code': None}
        self.event_loops = {}
        self.frequent_checks_list = {}
        self.frequent_checks_list_lock = threading.Lock()
        self.receiving_speed_average = 0
        self.receiving_speed_peak = {'value': 0,
                                     'timestamp': time.time()}
        self.high_performance = high_performance
        self.keep_max_received_last_second_entries = 5
        self.keepalive_streams_list = {}
        self.last_entry_added_to_stream_buffer = 0
        self.last_monitoring_check = time.time()
        self.last_update_check_github = {'timestamp': time.time(), 'status': {'tag_name': None}}
        self.last_update_check_github_check_command = {'timestamp': time.time(), 'status': {'tag_name': None}}
        self.listen_key_refresh_interval = 15*60
        self.max_send_messages_per_second = 5
        self.max_send_messages_per_second_reserve = 2
        self.most_receives_per_second = 0
        self.monitoring_api_server = None
        self.monitoring_total_received_bytes = 0
        self.monitoring_total_receives = 0
        self.output_default: Optional[Literal['dict', 'raw_data']] = output_default
        self.process_response = {}
        self.process_response_lock = threading.Lock()
        self.reconnects = 0
        self.reconnects_lock = threading.Lock()
        self.request_id = 0
        self.request_id_lock = threading.Lock()
        self.restart_timeout = restart_timeout
        self.return_response = {}
        self.return_response_lock = threading.Lock()
        self.ringbuffer_error = []
        self.ringbuffer_error_max_size = 500
        self.ringbuffer_result = []
        self.ringbuffer_result_max_size = 500
        self.show_secrets_in_logs = show_secrets_in_logs
        self.start_time = time.time()
        self.stream_buffer_maxlen = stream_buffer_maxlen
        self.stream_buffer = deque(maxlen=self.stream_buffer_maxlen)
        self.stream_buffer_lock = threading.Lock()
        self.stream_buffer_locks = {}
        self.stream_buffers = {}
        self.stream_signal_buffer = deque()
        self.stream_signal_buffer_lock = threading.Lock()
        self.socket_is_ready = {}
        self.sockets = {}
        self.stream_threads = {}
        self.total_received_bytes = 0
        self.total_received_bytes_lock = threading.Lock()
        self.total_receives = 0
        self.total_receives_lock = threading.Lock()
        self.total_transmitted = 0
        self.total_transmitted_lock = threading.Lock()
        self.close_timeout_default = close_timeout_default
        self.ping_interval_default = ping_interval_default
        self.ping_timeout_default = ping_timeout_default
        self.replacement_text = "***SECRET_REMOVED***"
        self.warn_on_update = warn_on_update
        if warn_on_update and self.is_update_available():
            update_msg = f"Release {self.name}_" + self.get_latest_version() + " is available, " \
                         f"please consider updating! Changelog: " \
                         f"https://unicorn-bybit-websocket-api.docs.lucit.tech/changelog.html"
            print(update_msg)
            logger.warning(update_msg)
        self.restclient = BybitWebSocketApiRestclient(debug=self.debug,
                                                      disable_colorama=self.disable_colorama,
                                                      exchange=self.exchange,
                                                      lucit_api_secret=self.lucit_api_secret,
                                                      lucit_license_ini=self.lucit_license_ini,
                                                      lucit_license_profile=self.lucit_license_profile,
                                                      lucit_license_token=self.lucit_license_token,
                                                      socks5_proxy_server=self.socks5_proxy_server,
                                                      socks5_proxy_user=self.socks5_proxy_user,
                                                      socks5_proxy_pass=self.socks5_proxy_pass,
                                                      stream_list=self.stream_list,
                                                      warn_on_update=self.warn_on_update)
        self.start()

    def __enter__(self):
        logger.debug(f"Entering with-context of BybitWebSocketApiManager() ...")
        return self

    def __exit__(self, exc_type, exc_value, error_traceback):
        logger.debug(f"Leaving with-context of BybitWebSocketApiManager() ...")
        self.stop_manager()
        if exc_type:
            logger.critical(f"An exception occurred: {exc_type} - {exc_value} - {error_traceback}")

    async def _run_process_asyncio_queue(self, scope=None, stream_id=None) -> bool:
        """ Execute a provided coroutine within the loop and process the exception results asynchronously."""
        if stream_id is None:
            return False
        stream_label = self.get_stream_label(stream_id=stream_id)
        if stream_label is None:
            stream_label = ""
        else:
            stream_label = f" ({stream_label})"
        try:
            if scope == "global":
                await self.process_asyncio_queue(stream_id=stream_id)
            elif scope == "specific":
                await self.specific_process_asyncio_queue[stream_id](stream_id=stream_id)
            else:
                return False
            logger.debug(f"`process_asyncio_queue` of stream_id {stream_id}{stream_label} completed successfully.")
            return True
        except Exception as error_msg:
            error_msg_wrapper = (f"Exception within to UBBWA`s provided `process_asyncio_queue`-coroutine of stream "
                                 f"'{stream_id}'{stream_label}: "
                                 f"\033[1m\033[31m{type(error_msg).__name__} - {error_msg}\033[0m\r\n"
                                 f"{traceback.format_exc()}")
            print(f"\r\n{error_msg_wrapper}")
            error_msg_wrapper = (f"Exception within to UBBWA`s provided `process_asyncio_queue`-coroutine of stream "
                                 f"'{stream_id}'{stream_label}: "
                                 f"{type(error_msg).__name__} - {error_msg}\r\n"
                                 f"{traceback.format_exc()}")
            logger.critical(error_msg_wrapper)
            self._crash_stream(stream_id=stream_id, error_msg=error_msg_wrapper)
            return False

    @staticmethod
    async def _shutdown_asyncgens(stream_id=None, loop=None) -> bool:
        if loop is None:
            return False
        logger.debug(f"BybitWebSocketApiManager._shutdown_asyncgens(stream_id={stream_id}) started ...")
        await loop.shutdown_asyncgens()
        return True

    async def _run_socket(self, stream_id, channels, endpoint, markets) -> None:
        while self.is_stop_request(stream_id=stream_id) is False \
                and self.is_crash_request(stream_id=stream_id) is False:
            try:
                async with BybitWebSocketApiSocket(self, stream_id, channels, endpoint, markets) as socket:
                    if socket is not None:
                        await socket.start_socket()
                    if self.is_stop_request(stream_id=stream_id) is False:
                        self._stream_is_restarting(stream_id=stream_id)
            except asyncio.CancelledError as error_msg:
                logger.debug(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                             f"{channels}), markets={markets}) - asyncio.CancelledError: {error_msg}")
                self._stream_is_stopping(stream_id=stream_id)
                return None
            except StreamIsCrashing as error_msg:
                logger.critical(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                                f"{channels}), markets={markets}) - StreamIsCrashing: {error_msg}")
                self._stream_is_crashing(stream_id=stream_id, error_msg=str(error_msg))
                return None
            except StreamIsStopping as error_msg:
                logger.info(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                            f"{channels}), markets={markets}) - StreamIsStopping: {error_msg}")
                self._stream_is_stopping(stream_id=stream_id)
                return None
            except ConnectionResetError as error_msg:
                logger.debug(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                             f"{channels}), markets={markets}) - ConnectionResetError: {error_msg}")
                self._stream_is_restarting(stream_id=stream_id, error_msg=str(error_msg))
            except ssl.SSLError as error_msg:
                logger.error(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                             f"{channels}), markets={markets}) - ssl.SSLError: {error_msg}")
                self._stream_is_restarting(stream_id=stream_id, error_msg=str(error_msg))
            except OSError as error_msg:
                logger.error(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                             f"{channels}), markets={markets}) - OSError: {error_msg}")
                self._stream_is_restarting(stream_id=stream_id, error_msg=str(error_msg))
            except websockets.ConnectionClosed as error_msg:
                logger.debug(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                             f"{channels}), markets={markets}) - websockets.ConnectionClosed: {error_msg}")
                self._stream_is_restarting(stream_id=stream_id, error_msg=error_msg)
            except websockets.InvalidStatusCode as error_msg:
                logger.error(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                             f"{channels}), markets={markets}) - websockets.InvalidStatusCode: {error_msg}")
                self._stream_is_restarting(stream_id=stream_id, error_msg=str(error_msg))
                if "Status code not 101: 400" in str(error_msg):
                    logger.error(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                                 f"{channels}), markets={markets}) - websockets.InvalidStatusCode: {error_msg}")
                    self._stream_is_restarting(stream_id=stream_id, error_msg=str(error_msg))
                elif "Status code not 101: 429" in str(error_msg):
                    logger.critical(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                                    f"{channels}), markets={markets}) - websockets.InvalidStatusCode: {error_msg}")
                    self._stream_is_crashing(stream_id=stream_id, error_msg=str(error_msg))
                    return None
                elif "Status code not 101: 500" in str(error_msg):
                    logger.error(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                                 f"{channels}), markets={markets}) - websockets.InvalidStatusCode: {error_msg}")
                    self._stream_is_restarting(stream_id=stream_id, error_msg=str(error_msg))
                else:
                    logger.error(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                                 f"{channels}), markets={markets}) - websockets.InvalidStatusCode: {error_msg}")
                    self._stream_is_restarting(stream_id=stream_id, error_msg=str(error_msg))
            except websockets.InvalidMessage as error_msg:
                logger.error(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                             f"{channels}), markets={markets}) - websockets.InvalidMessage: {error_msg}")
                self._stream_is_restarting(stream_id=stream_id, error_msg=str(error_msg))
            except websockets.NegotiationError as error_msg:
                logger.error(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                             f"{channels}), markets={markets}) - websockets.NegotiationError: {error_msg}")
                self._stream_is_restarting(stream_id=stream_id, error_msg=str(error_msg))
            except StreamIsRestarting as error_msg:
                logger.error(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                             f"{channels}), markets={markets}) - StreamIsRestarting: {error_msg}")
                self._stream_is_restarting(stream_id=stream_id, error_msg=str(error_msg))
            except Socks5ProxyConnectionError as error_msg:
                logger.error(f"BybitWebSocketApiManager._run_socket(stream_id={stream_id}), channels="
                             f"{channels}), markets={markets}) - Socks5ProxyConnectionError: {error_msg}")
                self._stream_is_restarting(stream_id=stream_id, error_msg=str(error_msg))
            await asyncio.sleep(0.1)
        if self.is_stop_request(stream_id=stream_id) is True:
            self._stream_is_stopping(stream_id=stream_id)
        elif self.is_crash_request(stream_id=stream_id) is True:
            self._stream_is_crashing(stream_id=stream_id)
        return None

    async def get_stream_data_from_asyncio_queue(self, stream_id=None):
        """
        Retrieves the oldest entry from the FIFO stack.

        :param stream_id: provide a stream_id - only needed for userData Streams (acquiring a listenKey)
        :type stream_id: str
        :return: stream_data - str, dict or None
        """
        if stream_id is None:
            return None
        try:
            return await self.asyncio_queue[stream_id].get()
        except RuntimeError:
            return None
        except KeyError:
            return None

    def asyncio_queue_task_done(self, stream_id=None) -> bool:
        """
        If `get_stream_data_from_asyncio_queue()` was used, `asyncio_queue_task_done()` must be executed at the end of
        the loop.

        :param stream_id: provide a stream_id - only needed for userData Streams (acquiring a listenKey)
        :type stream_id: str

        :return: bool

        Example:

        . code-block:: python

            while True:
                data = await bybit_wsm.get_stream_data_from_asyncio_queue(stream_id)
                print(data)
                bybit_wsm.asyncio_queue_task_done(stream_id)
        """
        if stream_id is None:
            return False
        try:
            self.asyncio_queue[stream_id].task_done()
        except KeyError:
            return False
        return True

    def send_stream_signal(self, signal_type=None, stream_id=None, data_record=None, error_msg=None) -> bool:
        """
        Send a stream signal
        """
        if str(error_msg).startswith("Stream with stream_id="):
            match = re.search(r'Reason:\s*(.*)', error_msg)
            if match:
                error_msg = match.group(1)
        self.process_stream_signals(signal_type=signal_type,
                                    stream_id=stream_id,
                                    data_record=data_record,
                                    error_msg=error_msg)
        with self.stream_list_lock:
            logger.debug(f"BybitWebSocketApiManager.send_stream_signal() - `stream_list_lock` was entered!")
            self.stream_list[stream_id]['last_stream_signal'] = signal_type
            logger.debug(f"BybitWebSocketApiManager.send_stream_signal() - Leaving `stream_list_lock`!")
        return True

    def send_with_stream(self, stream_id: str = None, payload: Union[dict, str] = None, timeout: float = 5.0) -> bool:
        """
        Send a payload with a specific stream.

        :param stream_id: id of the stream to be used for sending.
        :type stream_id: str
        :param payload: The payload to add.
        :type payload: dict or str(JSON)
        :param timeout: Timeout to wait for a ready stream.
        :type timeout: float or int

        :return: bool
        """
        if type(payload) is dict:
            payload = json.dumps(payload,
                                 ensure_ascii=False)
        if type(timeout) is int:
            timeout = float(timeout)

        if self.get_event_loop_by_stream_id(stream_id=stream_id) is not None:
            start_time = time.time()
            timeout_time = start_time + timeout
            while self.is_socket_ready(stream_id=stream_id) is False:
                if self.is_stop_request(stream_id=stream_id) is True \
                        or self.is_crash_request(stream_id=stream_id) is True \
                        or self.stream_list[stream_id]['status'].startswith("crashed") is True:
                    logger.error(f"BybitWebSocketApiManager.send_with_stream({stream_id} - Socket is stopping!")
                    return False
                if time.time() > timeout_time:
                    logger.error(f"BybitWebSocketApiManager.send_with_stream({stream_id} - Timeout exceeded!")
                    return False
                time.sleep(0.05)
            try:
                asyncio.run_coroutine_threadsafe(self.sockets[stream_id].websocket.send(payload),
                                                 self.get_event_loop_by_stream_id(stream_id=stream_id))
                logger.debug(f"BybitWebSocketApiManager.send_with_stream({stream_id} - Sent payload: {payload}")
                return True
            except KeyError as error_msg:
                logger.error(f"BybitWebSocketApiManager.send_with_stream({stream_id} - KeyError: {error_msg}")
                return False
            except AttributeError as error_msg:
                logger.error(f"BybitWebSocketApiManager.send_with_stream({stream_id} - AttributeError: {error_msg}")
                return False
        else:
            logger.error(f"BybitWebSocketApiManager.send_with_stream({stream_id} - No valid asyncio loop!")
            return False

    def _add_stream_to_stream_list(self,
                                   stream_id=None,
                                   channels=None,
                                   endpoint=None,
                                   markets=None,
                                   stream_label=None,
                                   stream_buffer_name: Union[Literal[False], str] = False,
                                   api_key=None,
                                   api_secret=None,
                                   output: Optional[Literal['dict', 'raw_data']] = None,
                                   ping_interval=None,
                                   ping_timeout=None,
                                   close_timeout=None,
                                   stream_buffer_maxlen=None,
                                   process_stream_data: Optional[Callable] = None,
                                   process_stream_data_async: Optional[Callable] = None,
                                   process_asyncio_queue: Optional[Callable] = None):
        """
        Create a list entry for new streams

        :param stream_id: provide a stream_id - only needed for userData Streams (acquiring a listenKey)
        :type stream_id: str
        :param channels: provide the channels to create the URI
        :type channels: str, list, set
        :param endpoint: provide the endpoint path without the version ('public/linear', 'private', 'trade' ...)
        :type endpoint: str
        :param markets: provide the markets to create the URI
        :type markets: str, list, set
        :param stream_label: provide a stream_label for the stream
        :type stream_label: str
        :param stream_buffer_name: If `False` the data is going to get written to the default stream_buffer,
                                   set to `True` to read the data via `pop_stream_data_from_stream_buffer(stream_id)` or
                                   provide a string to create and use a shared stream_buffer and read it via
                                   `pop_stream_data_from_stream_buffer('string')`.
        :type stream_buffer_name: False or str
        :param api_key: provide a valid Bybit API key
        :type api_key: str
        :param api_secret: provide a valid Bybit API secret
        :type api_secret: str
        :param output: the default setting `raw_data` can be globally overwritten with the parameter
                       `output_default <https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html?highlight=output_default#module-unicorn_bybit_websocket_api.unicorn_bybit_websocket_api_manager>`__
                       of BybitWebSocketApiManager`. To overrule the `output_default` value for this specific stream,
                       set `output` to "dict" to convert the received raw data to a python dict -
                       otherwise with the default setting "raw_data" the output remains unchanged and gets delivered as
                       received from the endpoints
        :type output: str
        :param ping_interval: Once the connection is open, a `Ping frame` is sent every
                              `ping_interval` seconds. This serves as a keepalive. It helps keeping
                              the connection open, especially in the presence of proxies with short
                              timeouts on inactive connections. Set `ping_interval` to `None` to
                              disable this behavior. (default: 20)
                              This parameter is passed through to the `websockets.client.connect()
                              <https://websockets.readthedocs.io/en/stable/topics/timeouts.html?highlight=ping_interval#keepalive-in-websockets>`__
        :type ping_interval: int or None
        :param ping_timeout: If the corresponding `Pong frame` isn't received within
                             `ping_timeout` seconds, the connection is considered unusable and is closed with
                             code 1011. This ensures that the remote endpoint remains responsive. Set
                             `ping_timeout` to `None` to disable this behavior. (default: 20)
                             This parameter is passed through to the `websockets.client.connect()
                             <https://websockets.readthedocs.io/en/stable/topics/timeouts.html?highlight=ping_interval#keepalive-in-websockets>`__
        :type ping_timeout: int or None
        :param close_timeout: The `close_timeout` parameter defines a maximum wait time in seconds for
                              completing the closing handshake and terminating the TCP connection. (default: 10)
                              This parameter is passed through to the `websockets.client.connect()
                              <https://websockets.readthedocs.io/en/stable/topics/design.html?highlight=close_timeout#closing-handshake>`__
        :type close_timeout: int or None
        :param stream_buffer_maxlen: Set a max len for the `stream_buffer`. Only used in combination with a non-generic
                                     `stream_buffer`. The generic `stream_buffer` uses always the value of
                                     `BybitWebSocketApiManager()`.
        :type stream_buffer_maxlen: int or None
        :param process_stream_data: Provide a function/method to process the received webstream data. The function
                            will be called instead of
                            `add_to_stream_buffer() <unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.add_to_stream_buffer>`__
                            like `process_stream_data(stream_data, stream_buffer_name)` where
                            `stream_data` cointains the raw_stream_data. If not provided, the raw stream_data will
                            get stored in the stream_buffer! `How to read from stream_buffer!
                            <https://unicorn-bybit-websocket-api.docs.lucit.tech/README.html#and-4-more-lines-to-print-the-receives>`__
        :type process_stream_data: function
        :param process_stream_data_async: Provide an asynchronous function/method to process the received webstream data.
                            The function will be called instead of
                            `add_to_stream_buffer() <unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.add_to_stream_buffer>`__
                            like `process_stream_data(stream_data, stream_buffer_name)` where
                            `stream_data` cointains the raw_stream_data. If not provided, the raw stream_data will
                            get stored in the stream_buffer! `How to read from stream_buffer!
                            <https://unicorn-bybit-websocket-api.docs.lucit.tech/README.html#and-4-more-lines-to-print-the-receives>`__
        :type process_stream_data_async: function
        :param process_asyncio_queue: Insert your Asyncio function into the same AsyncIO loop in which the websocket
                                      data is received. This method guarantees the fastest possible asynchronous
                                      processing of the data in the correct receiving sequence.
                                      https://unicorn-bybit-websocket-api.docs.lucit.tech/readme.html#or-await-the-webstream-data-in-an-asyncio-coroutine
        :type process_asyncio_queue: Optional[Callable]
        """
        output = output or self.output_default
        close_timeout = close_timeout or self.close_timeout_default
        ping_interval = ping_interval or self.ping_interval_default
        ping_timeout = ping_timeout or self.ping_timeout_default
        self.specific_process_asyncio_queue[stream_id] = process_asyncio_queue
        self.specific_process_stream_data[stream_id] = process_stream_data
        self.specific_process_stream_data_async[stream_id] = process_stream_data_async
        with self.stream_list_lock:
            logger.debug(f"BybitWebSocketApiManager._add_stream_to_stream_list() - `stream_list_lock` was entered!")
            self.stream_list[stream_id] = {'exchange': self.exchange,
                                           'stream_id': copy.deepcopy(stream_id),
                                           'recent_socket_id': None,
                                           'channels': copy.deepcopy(channels),
                                           'endpoint': copy.deepcopy(endpoint),
                                           'markets': copy.deepcopy(markets),
                                           'stream_label': copy.deepcopy(stream_label),
                                           'stream_buffer_name': copy.deepcopy(stream_buffer_name),
                                           'stream_buffer_maxlen': copy.deepcopy(stream_buffer_maxlen),
                                           'output': copy.deepcopy(output),
                                           'subscriptions': 0,
                                           'payload': [],
                                           'api_key': copy.deepcopy(api_key),
                                           'api_secret': copy.deepcopy(api_secret),
                                           'ping_interval': copy.deepcopy(ping_interval),
                                           'ping_timeout': copy.deepcopy(ping_timeout),
                                           'close_timeout': copy.deepcopy(close_timeout),
                                           'status': 'starting',
                                           'start_time': time.time(),
                                           'processed_receives_total': 0,
                                           'receives_statistic_last_second': {'most_receives_per_second': 0,
                                                                              'entries': {}},
                                           'seconds_to_last_heartbeat': None,
                                           'last_heartbeat': None,
                                           'stop_request': False,
                                           'crash_request': False,
                                           'crash_request_reason': None,
                                           'loop_is_closing': False,
                                           'seconds_since_has_stopped': None,
                                           'has_stopped': None,
                                           'reconnects': 0,
                                           'last_stream_signal': None,
                                           'logged_reconnects': [],
                                           'processed_transmitted_total': 0,
                                           'last_static_ping_listen_key': 0,
                                           'listen_key': None,
                                           'listen_key_cache_time': self.listen_key_refresh_interval,
                                           'last_received_data_record': None,
                                           'processed_receives_statistic': {},
                                           'transfer_rate_per_second': {'bytes': {}, 'speed': 0},
                                           'websocket_uri': None,
                                           '3rd-party-future': None}
            logger.debug(f"BybitWebSocketApiManager._add_stream_to_stream_list() - Leaving `stream_list_lock`!")
        logger.info("BybitWebSocketApiManager._add_stream_to_stream_list(" +
                    str(stream_id) + ", " + str(channels) + ", " + str(markets) + ", " + str(stream_label) + ", "
                    + str(stream_buffer_name) + ", " + str(stream_buffer_maxlen) + ")")

    def _create_stream_thread(self,
                              stream_id,
                              channels,
                              endpoint,
                              markets,
                              stream_buffer_name: Union[Literal[False], str] = False,
                              stream_buffer_maxlen=None):
        """
        Co function of self.create_stream to create a thread for the socket and to manage the coroutine

        :param stream_id: provide a stream_id - only needed for userData Streams (acquiring a listenKey)
        :type stream_id: str
        :param channels: provide the channels to create the URI
        :type channels: str, list, set
        :param endpoint: provide the endpoint to create the URI
        :type endpoint: str
        :param markets: provide the markets to create the URI
        :type markets: str, list, set
        :param stream_buffer_name: If `False` the data is going to get written to the default stream_buffer,
                           set to `True` to read the data via `pop_stream_data_from_stream_buffer(stream_id)` or
                           provide a string to create and use a shared stream_buffer and read it via
                           `pop_stream_data_from_stream_buffer('string')`.
        :type stream_buffer_name: False or str
        :param stream_buffer_maxlen: Set a max len for the `stream_buffer`. Only used in combination with a non-generic
                                     `stream_buffer`. The generic `stream_buffer` uses always the value of
                                     `BybitWebSocketApiManager()`.
        :type stream_buffer_maxlen: int or None
        :return:
        """
        if stream_buffer_name is not False:
            self.stream_buffer_locks[stream_buffer_name] = threading.Lock()
            try:
                # Not resetting the stream_buffer during a restart:
                if self.stream_buffers[stream_buffer_name]:
                    pass
            except KeyError:
                # Resetting
                self.stream_buffers[stream_buffer_name] = deque(maxlen=stream_buffer_maxlen)
        loop = None
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            if self.debug is True:
                loop.set_debug(enabled=True)
            self.event_loops[stream_id] = loop
            self.asyncio_queue[stream_id] = asyncio.Queue()
            # Todo: Task für ping starten
            # loop.create_task(self._ping_listen_key(stream_id=stream_id))
            logger.debug(f"BybitWebSocketApiManager._create_stream_thread({stream_id} - "
                         f"Adding `_run_socket({stream_id})` to asyncio loop ...")
            loop.run_until_complete(self._run_socket(stream_id=stream_id,
                                                     channels=channels,
                                                     endpoint=endpoint,
                                                     markets=markets))
        except OSError as error_msg:
            logger.critical(f"BybitWebSocketApiManager._create_stream_thread({str(stream_id)} - OSError  - can not "
                            f"create stream - error_msg: {str(error_msg)}")
        except RuntimeError as error_msg:
            logger.debug(f"BybitWebSocketApiManager._create_stream_thread() stream_id={str(stream_id)} "
                         f" - RuntimeError `error: 12` - error_msg: {str(error_msg)}")
        except Exception as error_msg:
            stream_label = self.get_stream_label(stream_id=stream_id)
            if stream_label is None:
                stream_label = ""
            else:
                stream_label = f" ({stream_label})"
            error_msg_wrapper = (f"Exception within a coroutine of stream '{stream_id}'{stream_label}: "
                                 f"\033[1m\033[31m{type(error_msg).__name__} - {error_msg}\033[0m\r\n"
                                 f"{traceback.format_exc()}")
            print(f"\r\n{error_msg_wrapper}")
            error_msg_wrapper = (f"Exception within a coroutine of stream '{stream_id}'{stream_label}: "
                                 f"{type(error_msg).__name__} - {error_msg}\r\n"
                                 f"{traceback.format_exc()}")
            logger.critical(error_msg_wrapper)
            self._crash_stream(stream_id=stream_id, error_msg=error_msg_wrapper)
        finally:
            logger.debug(f"Finally closing the loop stream_id={str(stream_id)}")
            try:
                with self.stream_list_lock:
                    logger.debug(f"BybitWebSocketApiManager._create_stream_thread() - `stream_list_lock` was "
                                 f"entered!")
                    self.stream_list[stream_id]['loop_is_closing'] = True
                    logger.debug(f"BybitWebSocketApiManager._create_stream_thread() - Leaving `stream_list_lock`!")
            except KeyError:
                pass
            if loop is not None:
                if loop.is_running():
                    try:
                        tasks = asyncio.all_tasks(loop)
                        loop.run_until_complete(self._shutdown_asyncgens(loop))
                        for task in tasks:
                            task.cancel()
                            try:
                                loop.run_until_complete(task)
                            except asyncio.CancelledError:
                                pass
                    except RuntimeError as error_msg:
                        logger.debug(f"BybitWebSocketApiManager._create_stream_thread() stream_id={str(stream_id)} - "
                                     f"RuntimeError `error: 14` - {error_msg}")
                    except RuntimeWarning as error_msg:
                        logger.debug(f"BybitWebSocketApiManager._create_stream_thread() stream_id={str(stream_id)} - "
                                     f"RuntimeWarning `error: 21` - {error_msg}")
                    except Exception as error_msg:
                        logger.debug(f"BybitWebSocketApiManager._create_stream_thread() finally - {error_msg}")
                if not loop.is_closed():
                    self.wait_till_stream_has_stopped(stream_id=stream_id, timeout=10)
                    loop.close()
            try:
                with self.stream_list_lock:
                    logger.debug(f"BybitWebSocketApiManager._create_stream_thread() - `stream_list_lock` was "
                                 f"entered!")
                    self.stream_list[stream_id]['loop_is_closing'] = False
                    logger.debug(f"BybitWebSocketApiManager._create_stream_thread() - Leaving `stream_list_lock`!")
            except KeyError as error_msg:
                logger.debug(f"BybitWebSocketApiManager._create_stream_thread() stream_id={str(stream_id)} - "
                             f"KeyError `error: 15` - {error_msg}")
            self.set_socket_is_ready(stream_id)

    def generate_signature(self, api_secret=None, data=None):
        """
        Signe the request.

        :param api_secret:
        :param data:

        :return:
        """
        ordered_data = self.order_params(data)
        query_string = '&'.join(["{}={}".format(d[0], d[1]) for d in ordered_data])
        m = hmac.new(api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
        return m.hexdigest()

    @staticmethod
    def order_params(data):
        """
        Convert params to list with signature as last element

        :param data:
        :return:

        """
        has_signature = False
        params = []
        for key, value in data.items():
            if key == 'signature':
                has_signature = True
            else:
                params.append((key, value))
        # sort parameters by key
        params.sort(key=itemgetter(0))
        if has_signature:
            params.append(('signature', data['signature']))
        return params

    async def _auto_data_cleanup_stopped_streams(self, interval=None, age=None) -> bool:
        if interval is None or age is None:
            return False
        logger.info(f"BybitWebSocketApiManager._auto_data_cleanup_stopped_streams() - Starting with an interval "
                    f"of {interval} seconds!")
        timestamp_last_check = 0
        await asyncio.sleep(10)
        while self.is_manager_stopping() is False:
            if self.get_timestamp_unix() > timestamp_last_check + interval:
                timestamp_last_check = self.get_timestamp_unix()
                if self.auto_data_cleanup_stopped_streams is True:
                    stopped_streams = []
                    with self.stream_list_lock:
                        logger.debug(f"BybitWebSocketApiManager._auto_data_cleanup_stopped_streams() - "
                                     f"`stream_list_lock` was entered!")
                        for stream_id in self.stream_list:
                            stopped_streams.append(stream_id)
                        logger.debug(f"BybitWebSocketApiManager._auto_data_cleanup_stopped_streams() - Leaving"
                                     f"`stream_list_lock`!")
                    for stream_id in stopped_streams:
                        if (self.stream_list[stream_id]['status'] == "stopped"
                                or self.stream_list[stream_id]['status'].startswith("crashed")):
                            if self.get_stream_info(stream_id=stream_id)['seconds_since_has_stopped'] > age:
                                logger.info(f"BybitWebSocketApiManager._auto_data_cleanup_stopped_streams() - "
                                            f"Removing all remaining data of stream with stream_id={stream_id} from "
                                            f"this instance!")
                                self.remove_all_data_of_stream_id(stream_id=stream_id)
                                logger.info(f"BybitWebSocketApiManager._auto_data_cleanup_stopped_streams() - "
                                            f"Remaining data of stream with stream_id={stream_id} successfully removed "
                                            f"from this instance!")
            await asyncio.sleep(interval+5)

    async def _frequent_checks(self):
        """
        This method gets started in a loop and is doing the frequent checks
        """
        frequent_checks_id = self.get_timestamp_unix()
        cpu_usage_time = False
        with self.frequent_checks_list_lock:
            self.frequent_checks_list[frequent_checks_id] = {'last_heartbeat': 0,
                                                             'stop_request': False,
                                                             'has_stopped': None}
        logger.debug(f"BybitWebSocketApiManager._frequent_checks() new instance created with frequent_checks_id"
                     f"={frequent_checks_id}")

        # threaded loop for min 1 check per second
        while self.stop_manager_request is False \
                and self.frequent_checks_list[frequent_checks_id]['stop_request'] is False:
            with self.frequent_checks_list_lock:
                self.frequent_checks_list[frequent_checks_id]['last_heartbeat'] = time.time()
            await asyncio.sleep(0.5)
            current_timestamp = int(time.time())
            last_timestamp = current_timestamp - 1
            next_to_last_timestamp = current_timestamp - 2
            total_most_stream_receives_last_timestamp = 0
            total_most_stream_receives_next_to_last_timestamp = 0
            active_stream_list = self.get_active_stream_list()
            # check CPU stats
            cpu = self.get_process_usage_cpu()
            if cpu >= 95:
                time_of_waiting = 5
                if cpu_usage_time is False:
                    cpu_usage_time = time.time()
                elif (time.time() - cpu_usage_time) > time_of_waiting:
                    logger.warning(f"BybitWebSocketApiManager._frequent_checks() - High CPU usage since "
                                   f"{str(time_of_waiting)} seconds: {str(cpu)}")
                    cpu_usage_time = False
            else:
                cpu_usage_time = False
            # count most_receives_per_second total last second
            if active_stream_list:
                for stream_id in active_stream_list:
                    # set the streams `most_receives_per_second` value
                    try:
                        if self.stream_list[stream_id]['receives_statistic_last_second']['entries'][last_timestamp] > \
                                self.stream_list[stream_id]['receives_statistic_last_second']['most_receives_per_second']:
                            with self.stream_list_lock:
                                logger.debug(f"BybitWebSocketApiManager._frequent_checks() - `stream_list_lock` was "
                                             f"entered!")
                                self.stream_list[stream_id]['receives_statistic_last_second']['most_receives_per_second'] = \
                                    self.stream_list[stream_id]['receives_statistic_last_second']['entries'][last_timestamp]
                                logger.debug(f"BybitWebSocketApiManager._frequent_checks() - Leaving "
                                             f"`stream_list_lock`!")
                    except KeyError:
                        pass
                    try:
                        total_most_stream_receives_last_timestamp += self.stream_list[stream_id]['receives_statistic_last_second']['entries'][last_timestamp]
                    except KeyError:
                        pass
                    try:
                        total_most_stream_receives_next_to_last_timestamp += self.stream_list[stream_id]['receives_statistic_last_second']['entries'][next_to_last_timestamp]
                    except KeyError:
                        pass
                    # delete list entries older than `keep_max_received_last_second_entries`
                    # receives_statistic_last_second
                    delete_index = []
                    if len(self.stream_list[stream_id]['receives_statistic_last_second']['entries']) > \
                            self.keep_max_received_last_second_entries:
                        with self.stream_list_lock:
                            logger.debug(f"BybitWebSocketApiManager._frequent_checks() - `stream_list_lock` was "
                                         f"entered!")
                            temp_entries = copy.deepcopy(self.stream_list[stream_id]['receives_statistic_last_second']['entries'])
                            logger.debug(f"BybitWebSocketApiManager._frequent_checks() - Leaving "
                                         f"`stream_list_lock`!")
                        for timestamp_key in temp_entries:
                            try:
                                if timestamp_key < current_timestamp - self.keep_max_received_last_second_entries:
                                    delete_index.append(timestamp_key)
                            except ValueError as error_msg:
                                logger.error("BybitWebSocketApiManager._frequent_checks() timestamp_key=" +
                                             str(timestamp_key) + " current_timestamp=" + str(current_timestamp) +
                                             " keep_max_received_last_second_entries=" +
                                             str(self.keep_max_received_last_second_entries) + " error_msg=" +
                                             str(error_msg))
                    for timestamp_key in delete_index:
                        with self.stream_list_lock:
                            logger.debug(f"BybitWebSocketApiManager._frequent_checks() - `stream_list_lock` was "
                                         f"entered!")
                            self.stream_list[stream_id]['receives_statistic_last_second']['entries'].pop(timestamp_key,
                                                                                                         None)
                            logger.debug(f"BybitWebSocketApiManager._frequent_checks() - Leaving "
                                         f"`stream_list_lock`!")
                    # transfer_rate_per_second
                    delete_index = []
                    if len(self.stream_list[stream_id]['transfer_rate_per_second']['bytes']) > \
                            self.keep_max_received_last_second_entries:
                        try:
                            temp_bytes = self.stream_list[stream_id]['transfer_rate_per_second']['bytes']
                            for timestamp_key in temp_bytes:
                                try:
                                    if timestamp_key < current_timestamp - self.keep_max_received_last_second_entries:
                                        delete_index.append(timestamp_key)
                                except ValueError as error_msg:
                                    logger.error(
                                        "BybitWebSocketApiManager._frequent_checks() timestamp_key="
                                        + str(timestamp_key) +
                                        " current_timestamp=" + str(current_timestamp) +
                                        " keep_max_received_last_second_"
                                        "entries=" + str(self.keep_max_received_last_second_entries) + " error_msg=" +
                                        str(error_msg))
                        except RuntimeError as error_msg:
                            logger.info("BybitWebSocketApiManager._frequent_checks() - "
                                        "Caught RuntimeError: " + str(error_msg))
                    for timestamp_key in delete_index:
                        with self.stream_list_lock:
                            logger.debug(f"BybitWebSocketApiManager._frequent_checks() - `stream_list_lock` was "
                                         f"entered!")
                            self.stream_list[stream_id]['transfer_rate_per_second']['bytes'].pop(timestamp_key, None)
                            logger.debug(f"BybitWebSocketApiManager._frequent_checks() - Leaving "
                                         f"`stream_list_lock`!")
            # set most_receives_per_second
            try:
                if int(self.most_receives_per_second) < int(total_most_stream_receives_last_timestamp):
                    self.most_receives_per_second = int(total_most_stream_receives_last_timestamp)
            except ValueError as error_msg:
                logger.error("BybitWebSocketApiManager._frequent_checks() self.most_receives_per_second"
                             "=" + str(self.most_receives_per_second) + " total_most_stream_receives_last_timestamp"
                             "=" + str(total_most_stream_receives_last_timestamp) + " total_most_stream_receives_next_"
                             "to_last_timestamp=" + str(total_most_stream_receives_next_to_last_timestamp) + " error_"
                             "msg=" + str(error_msg))
            # check receiving_speed_peak
            last_second_receiving_speed = self.get_current_receiving_speed_global()
            try:
                if last_second_receiving_speed > self.receiving_speed_peak['value']:
                    self.receiving_speed_peak['value'] = last_second_receiving_speed
                    self.receiving_speed_peak['timestamp'] = time.time()
                    logger.info(f"BybitWebSocketApiManager._frequent_checks() - reached new "
                                f"`highest_receiving_speed` "
                                f"{str(self.get_human_bytesize(self.receiving_speed_peak['value'], '/s'))} at "
                                f"{self.get_date_of_timestamp(self.receiving_speed_peak['timestamp'])}")
            except TypeError:
                pass
        logger.debug(f"BybitWebSocketApiManager._frequent_checks() - Leaving thread ...")

    @staticmethod
    def _handle_task_result(task: asyncio.Task) -> None:
        """
        This method is a callback for `loop.create_task()` to retrive the task exception and avoid the `Task exception
        was never retrieved` traceback on stdout:
        https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/issues/261
        """
        try:
            task.result()
        except asyncio.CancelledError:
            logger.debug(f"BybitWebSocketApiManager._handle_task_result() - asyncio.CancelledError raised by task "
                         f"= {task}")
        except SystemExit as error_code:
            logger.debug(f"BybitWebSocketApiManager._handle_task_result() - SystemExit({error_code}) raised by task "
                         f"= {task}")
        except Exception as error_msg:
            logger.critical(f"BybitWebSocketApiManager._handle_task_result() - Exception({error_msg}) raised by task "
                            f"= {task}")

    def add_payload_to_stream(self, stream_id=None, payload: dict = None):
        """
        Add a payload to a stream by `stream_id`.

        :param stream_id: id of a stream
        :type stream_id: str
        :param payload: The payload in JSON to add.
        :type payload: dict
        :return: bool
        """
        if payload is None or stream_id is None:
            return False
        else:
            try:
                with self.stream_list_lock:
                    logger.debug(f"BybitWebSocketApiManager.add_payload_to_stream() - `stream_list_lock` was entered!")
                    self.stream_list[stream_id]['payload'].append(payload)
                    logger.debug(f"BybitWebSocketApiManager.add_payload_to_stream() - Leaving `stream_list_lock`!")
            except KeyError:
                return False
            return True

    def add_to_ringbuffer_error(self, error):
        """
        Add received error messages from websocket endpoints to the error ringbuffer

        :param error: The data to add.
        :type error: string
        :return: bool
        """
        while len(self.ringbuffer_error) >= self.get_ringbuffer_error_max_size():
            self.ringbuffer_error.pop(0)
        self.ringbuffer_error.append(str(error))
        return True

    def add_to_ringbuffer_result(self, result):
        """
        Add received result messages from websocket endpoints to the result ringbuffer

        :param result: The data to add.
        :type result: string
        :return: bool
        """
        while len(self.ringbuffer_result) >= self.get_ringbuffer_result_max_size():
            self.ringbuffer_result.pop(0)
        self.ringbuffer_result.append(str(result))
        return True

    def add_to_stream_buffer(self, stream_data, stream_buffer_name: Union[Literal[False], str] = False):
        """
        Kick back data to the
        `stream_buffer <https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/%60stream_buffer%60>`__


        If it is not possible to process received stream data (for example, the database is restarting, so it's not
        possible to save the data), you can return the data back into the stream_buffer. After a few seconds you stopped
        writing data back to the stream_buffer, the BybitWebSocketApiManager starts flushing back the data to normal
        processing.

        :param stream_data: the data you want to write back to the buffer
        :type stream_data: raw stream_data or unicorn_fied stream data
        :param stream_buffer_name: If `False` the data is going to get written to the default stream_buffer,
                                   set to `True` to read the data via `pop_stream_data_from_stream_buffer(stream_id)` or
                                   provide a string to create and use a shared stream_buffer and read it via
                                   `pop_stream_data_from_stream_buffer('string')`.
        :type stream_buffer_name: False or str
        :return: bool
        """
        if stream_buffer_name is False:
            with self.stream_buffer_lock:
                self.stream_buffer.append(stream_data)
        else:
            with self.stream_buffer_locks[stream_buffer_name]:
                self.stream_buffers[stream_buffer_name].append(stream_data)
        self.last_entry_added_to_stream_buffer = time.time()
        return True

    def add_to_stream_signal_buffer(self, signal_type=None, stream_id=None, data_record=None, error_msg=None):
        """
        Add signals about a stream to the
        `stream_signal_buffer <https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/%60stream_signal_buffer%60>`__

        :param signal_type: "CONNECT", "DISCONNECT", "FIRST_RECEIVED_DATA" or "STREAM_UNREPAIRABLE"
        :type signal_type: str
        :param stream_id: id of a stream
        :type stream_id: str
        :param data_record: The last or first received data record
        :type data_record: str or dict
        :param error_msg: The message of the error.
        :type error_msg: str or dict
        :return: bool
        """
        if self.enable_stream_signal_buffer:
            stream_signal = {'type': signal_type,
                             'stream_id': stream_id,
                             'timestamp': time.time()}
            if signal_type == "CONNECT":
                # nothing to add ...
                pass
            elif signal_type == "STOP":
                # nothing to add ...
                pass
            elif signal_type == "DISCONNECT":
                try:
                    stream_signal['last_received_data_record'] = self.stream_list[stream_id]['last_received_data_record']
                except KeyError as error_msg:
                    logger.critical(f"BybitWebSocketApiManager.add_to_stream_signal_buffer({signal_type}) - "
                                    f"Cant determine last_received_data_record! - error_msg: {error_msg}")
                    stream_signal['last_received_data_record'] = None
            elif signal_type == "FIRST_RECEIVED_DATA":
                stream_signal['first_received_data_record'] = data_record
            elif signal_type == "STREAM_UNREPAIRABLE":
                stream_signal['error'] = str(error_msg)
            else:
                logger.error(f"BybitWebSocketApiManager.add_to_stream_signal_buffer({signal_type}) - "
                             f"Received invalid `signal_type`!")
                return False
            with self.stream_signal_buffer_lock:
                self.stream_signal_buffer.append(stream_signal)
            logger.info(f"BybitWebSocketApiManager.add_to_stream_signal_buffer({stream_signal})")
            return True
        else:
            return False

    def add_total_received_bytes(self, size):
        """
        Add received bytes to the total received bytes statistic

        :param size: int value of added bytes
        :type size: int
        """
        with self.total_received_bytes_lock:
            self.total_received_bytes += int(size)

    def clear_asyncio_queue(self, stream_id: str = None) -> bool:
        """
        Clear the asyncio queue of a specific stream.

        :param stream_id: provide a stream_id
        :type stream_id: str

        :return: bool
        """
        if stream_id is None:
            logger.error(f"BybitWebSocketApiManager.clear_asyncio_queue() - Missing parameter `stream_id`!")
            return False
        logger.debug(f"BybitWebSocketApiManager.clear_asyncio_queue(stream_id={stream_id}) - Resetting asyncio_queue "
                     f"...")
        try:
            while True:
                self.asyncio_queue[stream_id].get_nowait()
        except asyncio.QueueEmpty:
            logger.debug(
                f"BybitWebSocketApiManager.clear_asyncio_queue(stream_id={stream_id}) - Finished resetting of "
                f"asyncio_queue!")
        return True

    def clear_stream_buffer(self, stream_buffer_name: Union[Literal[False], str] = False):
        """
        Clear the
        `stream_buffer <https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/%60stream_buffer%60>`__

        :param stream_buffer_name: `False` to read from generic stream_buffer, the stream_id if you used True in
                                   create_stream() or the string name of a shared stream_buffer.
        :type stream_buffer_name: False or str
        :return: bool
        """
        if stream_buffer_name is False:
            try:
                self.stream_buffer.clear()
                return True
            except IndexError:
                return False
        else:
            try:
                with self.stream_buffer_locks[stream_buffer_name]:
                    self.stream_buffers[stream_buffer_name].clear()
                return True
            except IndexError:
                return False
            except KeyError:
                return False

    def create_payload(self, stream_id, method, channels=None, markets=None):
        """
        Create the payload for subscriptions

        :param stream_id: provide a stream_id
        :type stream_id: str
        :param method: `SUBSCRIBE` or `UNSUBSCRIBE`
        :type method: str
        :param channels: provide the channels to create the URI
        :type channels: str, list, set
        :param markets: provide the markets to create the URI
        :type markets: str, list, set
        :return: payload (list) or False
        """
        logger.info("BybitWebSocketApiManager.create_payload(" + str(stream_id) + ", " + str(channels) + ", " +
                    str(markets) + ") started ...")
        if channels is None or markets is None:
            logger.info(f"BybitWebSocketApiManager.create_payload({str(stream_id)}) - `channels` and `markets` must "
                        f"be specified!")
            return None
        if channels is not None:
            if type(channels) is str:
                channels = [channels]
        if markets is not None:
            if type(markets) is str:
                markets = [markets]
        payload = []
        if method == "subscribe":
            for channel in channels:
                params = {
                    "op": "subscribe",
                    "args": [f"{channel}.{symbol.upper()}" for symbol in markets]
                }
                payload.append(params)
        elif method == "unsubscribe":
            raise NotImplemented(f"Feature 'unsubscribe' is currently not available!")
        else:
            logger.critical(f"BybitWebSocketApiManager.create_payload(" + str(stream_id) + ", "
                            + str(channels) + ", " + str(markets) + ") - Allowed values for `method`: `subscribe` "
                            "or `unsubscribe`!")
            return None
        logger.info("BybitWebSocketApiManager.create_payload(" + str(stream_id) + ", "
                    + str(channels) + ", " + str(markets) + ") - Payload: " + str(payload))
        logger.info("BybitWebSocketApiManager.create_payload(" + str(stream_id) + ", " + str(channels) + ", " +
                    str(markets) + ") finished ...")
        return payload

    def create_stream(self,
                      channels: Union[str, List[str], Set[str], None] = None,
                      endpoint: str = None,
                      markets: Union[str, List[str], Set[str], None] = None,
                      stream_label: str = None,
                      stream_buffer_name: Union[Literal[False], str] = False,
                      api_key: str = None,
                      api_secret: str = None,
                      output: Optional[Literal['dict', 'raw_data']] = None,
                      ping_interval: int = None,
                      ping_timeout: int = None,
                      close_timeout: int = None,
                      stream_buffer_maxlen: int = None,
                      process_stream_data: Optional[Callable] = None,
                      process_stream_data_async: Optional[Callable] = None,
                      process_asyncio_queue: Optional[Callable] = None):
        """
        Create a websocket stream

        If you provide 2 markets and 2 channels, then you are going to create 4 subscriptions (markets * channels).

            Example:

                channels = ['trade', 'kline_1']

                markets = ['bnbbtc', 'ethbtc']

                Finally:  bnbbtc@trade, ethbtc@trade, bnbbtc@kline.1, ethbtc@kline.1

        `There is a subscriptions limit per stream!
        <https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/Bybit-websocket-endpoint-configuration-overview>`__

        :param channels: provide the channels you wish to stream
        :type channels: str, list, set
        :param endpoint: provide the endpoint
        :type endpoint: str
        :param markets: provide the markets you wish to stream
        :type markets: str, list, set
        :param stream_label: provide a stream_label to identify the stream
        :type stream_label: str
        :param stream_buffer_name: If `False` the data is going to get written to the default stream_buffer,
                                   set to `True` to read the data via `pop_stream_data_from_stream_buffer(stream_id)` or
                                   provide a string to create and use a shared stream_buffer and read it via
                                   `pop_stream_data_from_stream_buffer('string')`.
        :type stream_buffer_name: bool or str
        :param api_key: provide a valid Bybit API key
        :type api_key: str
        :param api_secret: provide a valid Bybit API secret
        :type api_secret: str
        :param output: the default setting `raw_data` can be globally overwritten with the parameter
                       `output_default <https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html?highlight=output_default#module-unicorn_bybit_websocket_api.unicorn_bybit_websocket_api_manager>`__
                       of BybitWebSocketApiManager`. To overrule the `output_default` value for this specific stream,
                       set `output` to "dict" to convert the received raw data to a python dict -  otherwise with
                       the default setting "raw_data" the output remains unchanged and gets delivered as received from
                       the endpoints
        :type output: str
        :param ping_interval: Once the connection is open, a `Ping frame` is sent every
                              `ping_interval` seconds. This serves as a keepalive. It helps keeping
                              the connection open, especially in the presence of proxies with short
                              timeouts on inactive connections. Set `ping_interval` to `None` to
                              disable this behavior. (default: 20)
                              This parameter is passed through to the `websockets.client.connect()
                              <https://websockets.readthedocs.io/en/stable/topics/timeouts.html?highlight=ping_interval#keepalive-in-websockets>`__
        :type ping_interval: int or None
        :param ping_timeout: If the corresponding `Pong frame` isn't received within
                             `ping_timeout` seconds, the connection is considered unusable and is closed with
                             code 1011. This ensures that the remote endpoint remains responsive. Set
                             `ping_timeout` to `None` to disable this behavior. (default: 20)
                             This parameter is passed through to the `websockets.client.connect()
                             <https://websockets.readthedocs.io/en/stable/topics/timeouts.html?highlight=ping_interval#keepalive-in-websockets>`__
        :type ping_timeout: int or None
        :param close_timeout: The `close_timeout` parameter defines a maximum wait time in seconds for
                              completing the closing handshake and terminating the TCP connection. (default: 10)
                              This parameter is passed through to the `websockets.client.connect()
                              <https://websockets.readthedocs.io/en/stable/topics/design.html?highlight=close_timeout#closing-handshake>`__
        :type close_timeout: int or None
        :param stream_buffer_maxlen: Set a max len for the `stream_buffer`. Only used in combination with a non-generic
                                     `stream_buffer`. The generic `stream_buffer` uses always the value of
                                     `BybitWebSocketApiManager()`.
        :type stream_buffer_maxlen: int or None
        :param process_stream_data: Provide a function/method to process the received webstream data (callback). The
                            function will be called instead of
                            `add_to_stream_buffer() <unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.add_to_stream_buffer>`__
                            like `process_stream_data(stream_data)` where
                            `stream_data` contains the raw_stream_data. If not provided, the raw stream_data will
                            get stored in the stream_buffer or provided to the global callback function provided during
                            object instantiation! `How to read from stream_buffer!
                            <https://unicorn-bybit-websocket-api.docs.lucit.tech/README.html?highlight=pop_stream_data_from_stream_buffer#and-4-more-lines-to-print-the-receives>`__
        :type process_stream_data: function
        :param process_stream_data_async: Provide an asynchronous function/method to process the received webstream data (callback). The
                            function will be called instead of
                            `add_to_stream_buffer() <unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.add_to_stream_buffer>`__
                            like `process_stream_data(stream_data)` where
                            `stream_data` contains the raw_stream_data. If not provided, the raw stream_data will
                            get stored in the stream_buffer or provided to the global callback function provided during
                            object instantiation! `How to read from stream_buffer!
                            <https://unicorn-bybit-websocket-api.docs.lucit.tech/README.html?highlight=pop_stream_data_from_stream_buffer#and-4-more-lines-to-print-the-receives>`__
        :type process_stream_data_async: function
        :param process_asyncio_queue: Insert your Asyncio function into the same AsyncIO loop in which the websocket data
                                      is received. This method guarantees the fastest possible asynchronous processing of
                                      the data in the correct receiving sequence.
                                      https://unicorn-bybit-websocket-api.docs.lucit.tech/readme.html#or-await-the-webstream-data-in-an-asyncio-coroutine
        :type process_asyncio_queue: Optional[Callable]

        :return: stream_id or 'None'
        """
        if endpoint is None:
            raise ValueError("Parameter 'endpoint' must not be `None`!")
        if channels is None:
            channels = []
        if markets is None:
            markets = []
        if type(channels) is str:
            channels = [channels]
        if type(markets) is str:
            markets = [markets]
        output = output or self.output_default
        close_timeout = close_timeout or self.close_timeout_default
        ping_interval = ping_interval or self.ping_interval_default
        ping_timeout = ping_timeout or self.ping_timeout_default
        stream_id = self.get_new_uuid_id()

        logger.info(f"BybitWebSocketApiManager.create_stream({str(channels)}, {str(endpoint)}, {str(markets)}, "
                    f"{str(stream_label)}, {str(stream_buffer_name)}) with stream_id={stream_id}")

        self._add_stream_to_stream_list(stream_id=stream_id,
                                        channels=channels,
                                        endpoint=endpoint,
                                        markets=markets,
                                        stream_label=stream_label,
                                        stream_buffer_name=stream_buffer_name,
                                        api_key=api_key,
                                        api_secret=api_secret,
                                        output=output,
                                        ping_interval=ping_interval,
                                        ping_timeout=ping_timeout,
                                        close_timeout=close_timeout,
                                        stream_buffer_maxlen=stream_buffer_maxlen,
                                        process_stream_data=process_stream_data,
                                        process_stream_data_async=process_stream_data_async,
                                        process_asyncio_queue=process_asyncio_queue)
        self.set_socket_is_not_ready(stream_id)
        self.event_loops[stream_id] = None
        thread = threading.Thread(target=self._create_stream_thread,
                                  args=(stream_id,
                                        channels,
                                        endpoint,
                                        markets,
                                        stream_buffer_name,
                                        stream_buffer_maxlen),
                                  name=f"_create_stream_thread:  stream_id={stream_id}, time={time.time()}")
        thread.start()
        self.stream_threads[stream_id] = thread
        while self.is_socket_ready(stream_id=stream_id) is False:
            if self.is_stop_request(stream_id=stream_id) is True \
                    or self.is_crash_request(stream_id=stream_id) is True \
                    or self.stream_list[stream_id]['status'].startswith("crashed") is True:
                return stream_id
            if self.high_performance is True:
                break
            time.sleep(0.1)
        if self.event_loops[stream_id] is not None:
            if self.event_loops[stream_id].is_closed():
                return stream_id
        if self.specific_process_asyncio_queue[stream_id] is not None:
            logger.debug(f"BybitWebSocketApiManager.create_stream({stream_id} - Adding "
                         f"`specific_process_asyncio_queue[{stream_id}]()` to asyncio loop ...")
            if self.get_event_loop_by_stream_id(stream_id=stream_id) is not None:
                asyncio.run_coroutine_threadsafe(self._run_process_asyncio_queue(scope="specific",
                                                                                 stream_id=stream_id),
                                                 self.get_event_loop_by_stream_id(stream_id=stream_id))
            else:
                logger.error(f"BybitWebSocketApiManager.create_stream({stream_id} - No valid asyncio loop!")
        elif self.process_asyncio_queue is not None:
            # The global process_asyncio_queue can be overwritten by specific process stream (async) functions
            if self.specific_process_stream_data[stream_id] is None \
                    and self.specific_process_stream_data_async[stream_id] is None:
                logger.debug(f"BybitWebSocketApiManager.create_stream({stream_id} - "
                             f"Adding `process_asyncio_queue()` to asyncio loop ...")
                if self.get_event_loop_by_stream_id(stream_id=stream_id) is not None:
                    asyncio.run_coroutine_threadsafe(self._run_process_asyncio_queue(scope="global",
                                                                                     stream_id=stream_id),
                                                     self.get_event_loop_by_stream_id(stream_id=stream_id))
                else:
                    logger.error(f"BybitWebSocketApiManager.create_stream({stream_id} - No valid asyncio loop!")
        return stream_id

    def create_websocket_uri(self, channels=None, endpoint=None, markets=None, stream_id=None) -> str:
        """
        Create a websocket URI

        :param channels: provide the channels to create the URI
        :type channels: str, list, set
        :param endpoint: provide the endpoint to create the URI
        :type endpoint: str
        :param markets: provide the markets to create the URI
        :type markets: str, list, set
        :param stream_id: provide a stream_id - only needed for userData Streams (acquiring a listenKey)
        :type stream_id: str
        :return: str or None
        """
        if endpoint is None:
            raise ValueError(f"Parameter 'endpoint' must not be `None`!")
        uri = f"{self.websocket_base_uri}/{self.api_version}/{endpoint}"
        logger.info(f"BybitWebSocketApiManager.create_websocket_uri() - Created websocket URI for stream_id={stream_id}"
                    f" is '{uri}'")
        return uri

    def delete_listen_key_by_stream_id(self, stream_id) -> bool:
        """
        Delete a Bybit listen_key from a specific !userData stream

        :param stream_id: id of a !userData stream
        :type stream_id: str

        :return: bool
        """
        try:
            if self.stream_list[stream_id]['listen_key'] is not None:
                logger.info("BybitWebSocketApiManager.delete_listen_key_by_stream_id(" + str(stream_id) + ")")
                response, bybit_api_status = self.restclient.delete_listen_key(stream_id)
                if bybit_api_status is not None:
                    self.bybit_api_status = bybit_api_status
        except requests.exceptions.ReadTimeout as error_msg:
            logger.debug(f"BybitWebSocketApiManager.delete_listen_key_by_stream_id() - Not able to delete "
                         f"listen_key - requests.exceptions.ReadTimeout: {error_msg}")
            return False
        except KeyError:
            return False

    def delete_stream_from_stream_list(self, stream_id, timeout: float = 10.0) -> bool:
        """
        Delete a stream from the stream_list

        Even if a stream crashes or get stopped, its data remains in the BybitWebSocketApiManager till you stop the
        BybitWebSocketApiManager itself. If you want to tidy up the stream_list you can use this method.

        :param stream_id: id of a stream
        :type stream_id: str
        :param timeout: The timeout for how long to wait for the stream to stop. The function aborts if the waiting
                        time is exceeded and returns False.
        :type timeout: float

        :return: bool
        """
        logger.info("BybitWebSocketApiManager.delete_stream_from_stream_list(" + str(stream_id) + ")")
        logger.warning("`BybitWebSocketApiManager.delete_stream_from_stream_list()` is deprecated, use "
                       "`BybitWebSocketApiManager.remove_all_data_of_stream_id()` instead!")
        if self.wait_till_stream_has_stopped(stream_id=stream_id, timeout=timeout) is True:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.delete_stream_from_stream_list() - `stream_list_lock` "
                             f"was entered!")
                self.stream_list.pop(stream_id, False)
                logger.debug(f"BybitWebSocketApiManager.delete_stream_from_stream_list() - Leaving "
                             f"`stream_list_lock`!")
            return True
        else:
            return False

    def remove_all_data_of_stream_id(self, stream_id, timeout: float = 10.0) -> bool:
        """
        Delete all remaining data within the UBBWA instance of a stopped stream.

        Even if a stream crashes or get stopped, its data remains in the BybitWebSocketApiManager till you stop the
        BybitWebSocketApiManager itself. If you want to tidy up the entire UBBWA instance you can use this method.

        UnicornBybitWebSocketApiManager accepts the parameter `auto_data_cleanup_stopped_streams`. If this is set
        to `True` (`auto_data_cleanup_stopped_streams=True`), the UBBWA instance performs the cleanup with this function
        `remove_all_data_of_stream_id()` automatically and regularly.

        :param stream_id: id of a stream
        :type stream_id: str
        :param timeout: The timeout for how long to wait for the stream to stop. The function aborts if the waiting
                        time is exceeded and returns False.
        :type timeout: float

        :return: bool
        """
        logger.debug(f"BybitWebSocketApiManager.remove_all_data_of_stream_id({stream_id}) started ...")
        if self.wait_till_stream_has_stopped(stream_id=stream_id, timeout=timeout) is True:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.remove_all_data_of_stream_id() - `stream_list_lock` "
                             f"was entered!")
                self.stream_list.pop(stream_id, False)
                logger.debug(f"BybitWebSocketApiManager.remove_all_data_of_stream_id() - Leaving `stream_list_lock`!")
            try:
                del self.event_loops[stream_id]
            except KeyError:
                pass
            try:
                del self.specific_process_asyncio_queue[stream_id]
            except KeyError:
                pass
            try:
                del self.specific_process_stream_data[stream_id]
            except KeyError:
                pass
            try:
                del self.specific_process_stream_data_async[stream_id]
            except KeyError:
                pass
            try:
                del self.socket_is_ready[stream_id]
            except KeyError:
                pass
            try:
                del self.stream_threads[stream_id]
            except KeyError:
                pass
            logger.debug(f"BybitWebSocketApiManager.remove_all_data_of_stream_id({stream_id}) successfully finished!")
            return True
        else:
            logger.error(f"BybitWebSocketApiManager.remove_all_data_of_stream_id({stream_id}) timeout! Stream not "
                         f"stopped!")
            return False

    @staticmethod
    def fill_up_space_left(demand_of_chars, string, filling=" "):
        """
        Add whitespaces to `string` to a length of `demand_of_chars` on the left side

        :param demand_of_chars: how much chars does the string have to have?
        :type demand_of_chars: int
        :param string: the string that has to get filled up with spaces
        :type string: str
        :param filling: filling char (default: blank space)
        :type filling: str
        :return: the filled up string
        """
        blanks_pre = ""
        blanks_post = ""
        demand_of_blanks = demand_of_chars - len(str(string)) - 1
        while len(blanks_pre) < demand_of_blanks:
            blanks_pre += filling
            blanks_post = filling
        return blanks_pre + str(string) + blanks_post

    @staticmethod
    def fill_up_space_centered(demand_of_chars, string, filling=" "):
        """
        Add whitespaces to `string` to a length of `demand_of_chars`

        :param demand_of_chars: how much chars does the string have to have?
        :type demand_of_chars: int
        :param string: the string that has to get filled up with spaces
        :type string: str
        :param filling: filling char (default: blank space)
        :type filling: str
        :return: the filled up string
        """
        blanks_pre = ""
        blanks_post = ""
        demand_of_blanks = demand_of_chars - len(str(string)) - 1
        while (len(blanks_pre)+len(blanks_post)) < demand_of_blanks:
            blanks_pre += filling
            if (len(blanks_pre) + len(blanks_post)) < demand_of_blanks:
                blanks_post += filling
        return blanks_pre + str(string) + blanks_post

    @staticmethod
    def fill_up_space_right(demand_of_chars, string, filling=" "):
        """
        Add whitespaces to `string` to a length of `demand_of_chars` on the right side

        :param demand_of_chars: how much chars does the string have to have?
        :type demand_of_chars: int
        :param string: the string that has to get filled up with spaces
        :type string: str
        :param filling: filling char (default: blank space)
        :type filling: str
        :return: the filled up string
        """
        blanks_pre = " "
        blanks_post = ""
        demand_of_blanks = demand_of_chars - len(str(string))
        while len(blanks_post) < demand_of_blanks-1:
            blanks_pre = filling
            blanks_post += filling
        string = blanks_pre + str(string) + blanks_post
        return string[0:demand_of_chars]

    def get_active_stream_list(self):
        """
        Get a list of all active streams

        :return: set or False
        """
        # get the stream_list without stopped and crashed streams
        stream_list_with_active_streams = {}
        with self.stream_list_lock:
            logger.debug(f"BybitWebSocketApiManager.get_active_stream_list() - `stream_list_lock` "
                         f"was entered!")
            for stream_id in self.stream_list:
                if self.stream_list[stream_id]['status'] == "running":
                    stream_list_with_active_streams[stream_id] = self.stream_list[stream_id]
            logger.debug(f"BybitWebSocketApiManager.get_active_stream_list() - Leaving `stream_list_lock`!")
        try:
            if len(stream_list_with_active_streams) > 0:
                return stream_list_with_active_streams
        except KeyError:
            return False
        except UnboundLocalError:
            return False

    def get_all_receives_last_second(self):
        """
        Get the number of all receives of the last second

        :return: int
        """
        all_receives_last_second = 0
        last_second_timestamp = int(time.time()) - 1
        with self.stream_list_lock:
            logger.debug(f"BybitWebSocketApiManager.get_all_receives_last_second() - `stream_list_lock` "
                         f"was entered!")
            for stream_id in self.stream_list:
                try:
                    all_receives_last_second += self.stream_list[stream_id]['receives_statistic_last_second']['entries'][
                        last_second_timestamp]
                except KeyError:
                    pass
            logger.debug(f"BybitWebSocketApiManager.get_all_receives_last_second() - Leaving `stream_list_lock`!")
        return all_receives_last_second

    def get_bybit_api_status(self):
        """
        `get_bybit_api_status()` is obsolete and will be removed in future releases, please use `get_used_weight()`
        instead!

        :return: dict
        """
        logger.warning("`get_bybit_api_status()` is obsolete and will be removed in future releases, please use"
                       "`get_used_weight()` instead!")
        return self.bybit_api_status

    def get_debug_log(self):
        """
        Get the debug log string.

        :return: str
        """
        if self.debug:
            debug_msg = f" - called by {str(traceback.format_stack()[-2]).strip()}"
        else:
            debug_msg = ""
        return debug_msg

    @staticmethod
    def get_timestamp() -> int:
        """
        Get a Bybit conform Timestamp.

        :return: int
        """
        return int(time.time() * 1000)

    @staticmethod
    def get_timestamp_unix() -> float:
        """
        Get a Bybit conform Timestamp.

        :return: float
        """
        return float(time.time())

    def get_used_weight(self):
        """
        Get used_weight, last status_code and the timestamp of the last status update

        :return: dict
        """
        return self.bybit_api_status

    def get_current_receiving_speed(self, stream_id):
        """
        Get the receiving speed of the last second in Bytes

        :return: int
        """
        current_timestamp = int(time.time())
        last_timestamp = current_timestamp - 1
        try:
            if self.stream_list[stream_id]['transfer_rate_per_second']['bytes'][last_timestamp] > 0:
                with self.stream_list_lock:
                    logger.debug(f"BybitWebSocketApiManager.get_current_receiving_speed() - `stream_list_lock` "
                                 f"was entered!")
                    self.stream_list[stream_id]['transfer_rate_per_second']['speed'] = \
                        self.stream_list[stream_id]['transfer_rate_per_second']['bytes'][last_timestamp]
                    logger.debug(f"BybitWebSocketApiManager.get_current_receiving_speed() - Leaving "
                                 f"`stream_list_lock`!")
        except TypeError:
            return 0
        except KeyError:
            return 0
        try:
            current_receiving_speed = self.stream_list[stream_id]['transfer_rate_per_second']['speed']
        except KeyError:
            current_receiving_speed = 0
        return current_receiving_speed

    def get_current_receiving_speed_global(self):
        """
        Get the receiving speed of the last second in Bytes from all streams!

        :return: int
        """
        current_receiving_speed = 0
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.get_current_receiving_speed_global() - `stream_list_lock` "
                             f"was entered!")
                temp_stream_list = copy.deepcopy(self.stream_list)
                logger.debug(f"BybitWebSocketApiManager.get_current_receiving_speed_global() - Leaving "
                             f"`stream_list_lock`!")
        except RuntimeError as error_msg:
            logger.debug(f"BybitWebSocketApiManager.get_current_receiving_speed_global() - RuntimeError: "
                         f"{str(error_msg)}")
            return 0
        except TypeError as error_msg:
            logger.debug(f"BybitWebSocketApiManager.get_current_receiving_speed_global() - RuntimeError: "
                         f"{str(error_msg)}")
            return 0
        for stream_id in temp_stream_list:
            current_receiving_speed += self.get_current_receiving_speed(stream_id)
        return current_receiving_speed

    @staticmethod
    def get_date_of_timestamp(timestamp):
        """
        Convert a timestamp into a readable date/time format for humans

        :param timestamp: provide the timestamp you want to convert into a date
        :type timestamp: timestamp
        :return: str
        """
        date = str(datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d, %H:%M:%S UTC'))
        return date

    def get_errors_from_endpoints(self):
        """
        Get all the stored error messages from the ringbuffer sent by the endpoints.

        :return: list
        """
        return self.ringbuffer_error

    def get_event_loop_by_stream_id(self, stream_id: Optional[Union[str, bool]] = False) -> Optional[asyncio.AbstractEventLoop]:
        """
        Get the asyncio event loop used by a specific stream.

        :return: asyncio.AbstractEventLoop or None
        """
        if stream_id is False:
            return None
        else:
            try:
                return self.event_loops[stream_id]
            except KeyError as error_msg:
                logger.debug(f"BybitWebSocketApiManager.get_event_loop_by_stream_id() - KeyError - {str(error_msg)}")
                return None

    def get_exchange(self):
        """
        Get the name of the used exchange like "bybit.com" or "bybit.com-testnet"

        :return: str
        """
        return self.exchange

    @staticmethod
    def get_human_bytesize(amount_bytes, suffix=""):
        """
        Convert the bytes to something readable

        :param amount_bytes: amount of bytes
        :type amount_bytes: int
        :param suffix: add a string after
        :type suffix: str
        :return:
        """
        if amount_bytes > 1024 * 1024 * 1024 * 1024:
            amount_bytes = str(round(amount_bytes / (1024 * 1024 * 1024 * 1024), 3)) + " tB" + suffix
        elif amount_bytes > 1024 * 1024 * 1024:
            amount_bytes = str(round(amount_bytes / (1024 * 1024 * 1024), 2)) + " gB" + suffix
        elif amount_bytes > 1024 * 1024:
            amount_bytes = str(round(amount_bytes / (1024 * 1024), 2)) + " mB" + suffix
        elif amount_bytes > 1024:
            amount_bytes = str(round(amount_bytes / 1024, 2)) + " kB" + suffix
        else:
            amount_bytes = str(amount_bytes) + " B" + suffix
        return amount_bytes

    @staticmethod
    def get_human_uptime(uptime):
        """
        Convert a timespan of seconds into hours, days, ...

        :param uptime: Uptime in seconds
        :type uptime: int
        :return:
        """
        if uptime > (60 * 60 * 24):
            uptime_days = int(uptime / (60 * 60 * 24))
            uptime_hours = int(((uptime - (uptime_days * (60 * 60 * 24))) / (60 * 60)))
            uptime_minutes = int((uptime - ((uptime_days * (60 * 60 * 24)) + (uptime_hours * 60 * 60))) / 60)
            uptime_seconds = int(
                uptime - ((uptime_days * (60 * 60 * 24)) + ((uptime_hours * (60 * 60)) + (uptime_minutes * 60))))
            uptime = str(uptime_days) + "d:" + str(uptime_hours) + "h:" + str(int(uptime_minutes)) + "m:" + str(
                int(uptime_seconds)) + "s"
        elif uptime > (60 * 60):
            uptime_hours = int(uptime / (60 * 60))
            uptime_minutes = int((uptime - (uptime_hours * (60 * 60))) / 60)
            uptime_seconds = int(uptime - ((uptime_hours * (60 * 60)) + (uptime_minutes * 60)))
            uptime = str(uptime_hours) + "h:" + str(int(uptime_minutes)) + "m:" + str(int(uptime_seconds)) + "s"
        elif uptime > 60:
            uptime_minutes = int(uptime / 60)
            uptime_seconds = uptime - uptime_minutes * 60
            uptime = str(uptime_minutes) + "m:" + str(int(uptime_seconds)) + "s"
        else:
            uptime = str(int(uptime)) + " seconds"
        return uptime

    @staticmethod
    def get_latest_release_info():
        """
        Get infos about the latest available release

        :return: dict or None
        """
        try:
            respond = requests.get(f'https://api.github.com/repos/LUCIT-Systems-and-Development/'
                                   f'unicorn-bybit-websocket-api/releases/latest')
            latest_release_info = respond.json()
            return latest_release_info
        except Exception as error_msg:
            logger.debug(f"BybitWebSocketApiManager.get_latest_release_info() - Exception: {error_msg}")
            return None

    @staticmethod
    def get_latest_release_info_check_command():
        """
        Get infos about the latest available `check_lucit_collector` release
        
        :return: dict or None
        """
        try:
            respond = requests.get('https://api.github.com/repos/LUCIT-Development/check_lucit_collector.py/'
                                   'releases/latest')
            return respond.json()
        except Exception as error_msg:
            logger.debug(f"BybitWebSocketApiManager.get_latest_release_info_check_command() - Exception: {error_msg}")
            return None

    def get_latest_version(self) -> Optional[str]:
        """
        Get the version of the latest available release (cache time 1 hour)

        :return: str or None
        """
        logger.debug(f"BybitWebSocketApiManager.get_latest_version() - Started ...")
        # Do a fresh request if status is None or last timestamp is older 1 hour
        if self.last_update_check_github['status'].get('tag_name') is None or \
                (self.last_update_check_github['timestamp'] + (60 * 60) < time.time()):
            self.last_update_check_github['status'] = self.get_latest_release_info()
        if self.last_update_check_github['status'].get('tag_name') is not None:
            try:
                return self.last_update_check_github['status']['tag_name']
            except KeyError as error_msg:
                logger.debug(f"BybitLocalDepthCacheManager.get_latest_version() - KeyError: {error_msg}")
                return None
        else:
            return None

    def get_latest_version_check_command(self) -> Optional[str]:
        """
        Get the version of the latest available `check_lucit_collector.py` release (cache time 1 hour)
        
        :return: str or None
        """
        # Do a fresh request if status is None or last timestamp is older 1 hour
        if self.last_update_check_github_check_command['status'].get('tag_name') is None or \
                (self.last_update_check_github_check_command['timestamp'] + (60 * 60) < time.time()):
            self.last_update_check_github_check_command['status'] = self.get_latest_release_info_check_command()
        if self.last_update_check_github_check_command['status'].get('tag_name') is not None:
            try:
                return self.last_update_check_github_check_command['status']['tag_name']
            except KeyError as error_msg:
                logger.debug(f"BybitWebSocketApiManager.get_latest_version_check_command() - KeyError: {error_msg}")
                return None
        else:
            return None

    def get_limit_of_subscriptions_per_stream(self):
        """
        Get the number of allowed active subscriptions per stream (limit of Bybit API)

        :return: int
        """
        return self.max_subscriptions_per_stream

    def get_number_of_all_subscriptions(self):
        """
        Get the amount of all stream subscriptions

        :return: int
        """
        subscriptions = 0
        try:
            active_stream_list = copy.deepcopy(self.get_active_stream_list())
            if active_stream_list:
                for stream_id in active_stream_list:
                    subscriptions += active_stream_list[stream_id]['subscriptions']
                self.all_subscriptions_number = subscriptions
        except TypeError as error_msg:
            logger.debug(f"BybitWebSocketApiManager.get_number_of_all_subscriptions() - TypeError: {error_msg}")
            return self.all_subscriptions_number
        except RuntimeError as error_msg:
            logger.debug(f"BybitWebSocketApiManager.get_number_of_all_subscriptions() - RuntimeError: {error_msg}")
            return self.all_subscriptions_number
        return subscriptions

    def get_most_receives_per_second(self):
        """
        Get the highest total receives per second value

        :return: int
        """
        return self.most_receives_per_second

    def get_number_of_streams_in_stream_list(self):
        """
        Get the number of streams that are stored in the stream_list

        :return: int
        """
        return len(self.stream_list)

    def get_number_of_subscriptions(self, stream_id):
        """
        Get the number of subscriptions of a specific stream

        :return: int
        """
        count_subscriptions = 0
        with self.stream_list_lock:
            logger.debug(f"BybitWebSocketApiManager.get_number_of_subscriptions() - `stream_list_lock` "
                         f"was entered!")
            for channel in self.stream_list[stream_id]['channels']:
                if "!" in channel \
                        or channel == "orders" \
                        or channel == "accounts" \
                        or channel == "transfers" \
                        or channel == "allTickers" \
                        or channel == "allMiniTickers" \
                        or channel == "blockheight":
                    count_subscriptions += 1
                    continue
                else:
                    for market in self.stream_list[stream_id]['markets']:
                        if "!" in market \
                                or market == "orders" \
                                or market == "accounts" \
                                or market == "transfers" \
                                or market == "allTickers" \
                                or market == "allMiniTickers" \
                                or market == "blockheight":
                            count_subscriptions += 1
                        else:
                            count_subscriptions += 1
            logger.debug(f"BybitWebSocketApiManager.get_number_of_subscriptions() - Leaving `stream_list_lock`!")
        return count_subscriptions

    def get_keep_max_received_last_second_entries(self):
        """
        Get the number of how much received_last_second entries are stored till they get deleted

        :return: int
        """
        return self.keep_max_received_last_second_entries

    @staticmethod
    def get_new_uuid_id() -> str:
        """
        Get a new unique uuid in string format. This is used as 'stream_id' or 'socket_id'.

        :return: uuid (str)
        """
        stream_id = uuid.uuid4()
        new_id_hash = hashlib.sha256(str(stream_id).encode()).hexdigest()
        new_id = f"{new_id_hash[0:12]}-{new_id_hash[12:16]}-{new_id_hash[16:20]}-{new_id_hash[20:24]}-" \
                 f"{new_id_hash[24:32]}"
        return str(new_id)

    def get_process_usage_memory(self):
        """
        Get the used memory of this process

        :return: str
        """
        process = psutil.Process(os.getpid())
        memory = self.get_human_bytesize(process.memory_info()[0])
        return memory

    @staticmethod
    def get_process_usage_cpu():
        """
        Get the used cpu power of this process

        :return: int
        """
        try:
            cpu = psutil.cpu_percent(interval=None)
        except OSError as error_msg:
            logger.error(f"BybitWebSocketApiManager.get_process_usage_cpu() - OSError - error_msg: {str(error_msg)}")
            return None
        return cpu

    @staticmethod
    def get_process_usage_threads():
        """
        Get the amount of threads that this process is using

        :return: int
        """
        threads = threading.active_count()
        return threads

    def get_reconnects(self):
        """
        Get the number of total reconnects

        :return: int
        """
        return self.reconnects

    def get_request_id(self):
        """
        Get a unique `request_id`

        :return: int
        """
        with self.request_id_lock:
            logger.debug(f"BybitWebSocketApiManager.get_monitoring_status_plain() - `stream_list_lock` "
                         f"was entered!")
            self.request_id += 1
            return self.request_id

    def get_result_by_request_id(self, request_id=None, timeout=10):
        """
        Get the result related to the provided `request_id`

        :param request_id: if you run `get_stream_subscriptions()
                           <https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_stream_subscriptions>`__
                           it returns a unique `request_id` - provide it to this method to receive the result.
        :type request_id: stream_id (uuid)
        :param timeout: seconds to wait to receive the result. If not there it returns 'False'
        :type timeout: int
        :return: `result` or None
        """
        if request_id is None:
            return None
        wait_till_timestamp = time.time() + timeout
        while wait_till_timestamp >= time.time():
            for result in self.ringbuffer_result:
                result_dict = json.loads(result)
                if result_dict['id'] == request_id:
                    return result
        return None

    def get_results_from_endpoints(self):
        """
        Get all the stored result messages from the ringbuffer sent by the endpoints.

        :return: list
        """
        return self.ringbuffer_result

    def get_ringbuffer_error_max_size(self):
        """
        How many entries should be stored in the ringbuffer?

        :return: int
        """
        return self.ringbuffer_error_max_size

    def get_ringbuffer_result_max_size(self):
        """
        How many entries should be stored in the ringbuffer?

        :return: int
        """
        return self.ringbuffer_result_max_size

    def get_start_time(self):
        """
        Get the start_time of the  BybitWebSocketApiManager instance

        :return: timestamp
        """
        return self.start_time

    def get_stream_buffer_byte_size(self):
        """
        Get the current byte size estimation of the stream_buffer

        :return: int
        """
        total_received_bytes = self.get_total_received_bytes()
        total_receives = self.get_total_receives()
        stream_buffer_length = self.get_stream_buffer_length()
        if total_received_bytes == 0 or total_receives == 0:
            return 0
        else:
            return round(total_received_bytes / total_receives * stream_buffer_length)

    def get_stream_buffer_length(self, stream_buffer_name: Union[Literal[False], str] = False):
        """
        Get the current number of items in all stream_buffer or of a specific stream_buffer

        :param stream_buffer_name: Name of the stream_buffer
        :type stream_buffer_name: str or stream_id
        :return: int
        """
        number = 0
        if stream_buffer_name:
            try:
                return len(self.stream_buffers[stream_buffer_name])
            except KeyError as error_msg:
                logger.debug(f"BybitWebSocketApiManager.get_stream_buffer_length() - KeyError - "
                             f"error_msg: {error_msg}")
                return 0
        else:
            number += len(self.stream_buffer)
            for stream_buffer_name in self.stream_buffers:
                number += len(self.stream_buffers[stream_buffer_name])
            return number

    def get_stream_id_by_label(self, stream_label: str = None) -> Optional[str]:
        """
        Get the stream_id of a specific stream by stream label

        :param stream_label: stream_label of the stream you search
        :type stream_label: str
        :return: stream_id or None
        """
        if stream_label is not None:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.get_stream_id_by_label() - `stream_list_lock` "
                             f"was entered!")
                for stream_id in self.stream_list:
                    if self.stream_list[stream_id]['stream_label'] == stream_label:
                        logger.debug(f"BybitWebSocketApiManager.get_stream_id_by_label() - Found `stream_id` via "
                                     f"`stream_label` `{stream_label}`")
                        logger.debug(f"BybitWebSocketApiManager.get_stream_id_by_label() - Leaving "
                                     f"`stream_list_lock`!")
                        return stream_id
                logger.debug(f"BybitWebSocketApiManager.get_stream_id_by_label() - Leaving `stream_list_lock`!")
            logger.error(f"BybitWebSocketApiManager.get_stream_id_by_label() - No `stream_id` found via "
                         f"`stream_label` {stream_label}`")
            return None
        else:
            return None

    def get_stream_info(self, stream_id):
        """
        Get all infos about a specific stream

        :param stream_id: id of a stream
        :type stream_id: str
        :return: set
        """
        current_timestamp = time.time()
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.get_stream_info() - `stream_list_lock` was entered!")
                temp_stream_list = copy.deepcopy(self.stream_list[stream_id])
                logger.debug(f"BybitWebSocketApiManager.get_stream_info() - Leaving `stream_list_lock`!")
        except RuntimeError:
            logger.error("BybitWebSocketApiManager.get_stream_info(" + str(stream_id) + ") Info: RuntimeError")
            return self.get_stream_info(stream_id)
        except KeyError:
            logger.error("BybitWebSocketApiManager.get_stream_info(" + str(stream_id) + ") Info: KeyError")
            return False
        if temp_stream_list['last_heartbeat'] is not None:
            temp_stream_list['seconds_to_last_heartbeat'] = \
                current_timestamp - self.stream_list[stream_id]['last_heartbeat']
        if temp_stream_list['has_stopped'] is not None:
            temp_stream_list['seconds_since_has_stopped'] = \
                int(current_timestamp) - int(self.stream_list[stream_id]['has_stopped'])
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.get_stream_info() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['processed_receives_statistic'] = self.get_stream_statistic(stream_id)
                logger.debug(f"BybitWebSocketApiManager.get_stream_info() - Leaving `stream_list_lock`!")
        except ZeroDivisionError:
            pass
        current_receiving_speed = self.get_current_receiving_speed(stream_id)
        with self.stream_list_lock:
            logger.debug(f"BybitWebSocketApiManager.get_stream_info() - `stream_list_lock` was entered!")
            self.stream_list[stream_id]['transfer_rate_per_second']['speed'] = current_receiving_speed
            logger.debug(f"BybitWebSocketApiManager.get_stream_info() - Leaving `stream_list_lock`!")
        return temp_stream_list

    def get_stream_label(self, stream_id=None):
        """
        Get the stream_label of a specific stream

        :param stream_id: id of a stream
        :type stream_id: str

        :return: str or None
        """
        if stream_id is not None:
            try:
                return self.stream_list[stream_id]['stream_label']
            except KeyError:
                return None
        else:
            return None

    def get_stream_subscriptions(self, stream_id, request_id=None):
        """
        Get a list of subscriptions of a specific stream from Bybit endpoints - the result can be received via
        the `stream_buffer` and is also added to the results ringbuffer - `get_results_from_endpoints()
        <https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_results_from_endpoints>`__
        to get all results or use `get_result_by_request_id(request_id)
        <https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_result_by_request_id>`__
        to get a specific one!

        This function is supported by CEX endpoints only!

        Info: ...

        :param stream_id: id of a stream
        :type stream_id: str
        :param request_id: id to use for the request - use `get_request_id()` to create a unique id. If not provided or
                           `False`, then this method is using `get_request_id()
                           <https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.get_request_id>`__
                           automatically.
        :type request_id: int
        :return: request_id (int)
        """
        if request_id is None:
            request_id = self.get_request_id()
        # Todo:
        if True:
            return request_id
        else:
            return None

    def get_stream_list(self):
        """
        Get a list of all streams

        :return: set
        """
        temp_stream_list = {}
        for stream_id in self.stream_list:
            temp_stream_list[stream_id] = self.get_stream_info(stream_id)
        return temp_stream_list

    def get_stream_buffer_maxlen(self, stream_buffer_name: Union[Literal[False], str] = False):
        """
        Get the maxlen value of the
        `stream_buffer <https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/%60stream_buffer%60>`__

        If maxlen is not specified or is None, `stream_buffer` may grow to an arbitrary length. Otherwise, the
        `stream_buffer` is bounded to the specified maximum length. Once a bounded length `stream_buffer` is full, when
        new items are added, a corresponding number of items are discarded from the opposite end.

        :param stream_buffer_name: `False` to read from generic stream_buffer, the stream_id if you used True in
                                   create_stream() or the string name of a shared stream_buffer.
        :type stream_buffer_name: False or str
        :return: int or False
        """
        if stream_buffer_name is False:
            try:
                return self.stream_buffer.maxlen
            except IndexError:
                return False
        else:
            try:
                return self.stream_buffers[stream_buffer_name].maxlen
            except IndexError:
                return False
            except KeyError:
                return False

    def get_stream_receives_last_second(self, stream_id):
        """
        Get the number of receives of specific stream from the last seconds

        :param stream_id: id of a stream
        :type stream_id: str
        :return: int
        """
        last_second_timestamp = int(time.time()) - 1
        try:
            return self.stream_list[stream_id]['receives_statistic_last_second']['entries'][last_second_timestamp]
        except KeyError:
            return 0

    def get_stream_statistic(self, stream_id):
        """
        Get the statistic of a specific stream

        :param stream_id: id of a stream
        :type stream_id: str
        :return: set
        """
        stream_statistic = {'stream_receives_per_second': 0,
                            'stream_receives_per_minute': 0,
                            'stream_receives_per_hour': 0,
                            'stream_receives_per_day': 0,
                            'stream_receives_per_month': 0,
                            'stream_receives_per_year': 0}
        try:
            if self.stream_list[stream_id]['status'] == "running":
                stream_statistic['uptime'] = time.time() - self.stream_list[stream_id]['start_time']
            elif self.stream_list[stream_id]['status'] == "stopped":
                stream_statistic['uptime'] = self.stream_list[stream_id]['has_stopped'] - self.stream_list[stream_id]['start_time']
            elif "crashed" in self.stream_list[stream_id]['status']:
                stream_statistic['uptime'] = self.stream_list[stream_id]['has_stopped'] - self.stream_list[stream_id]['start_time']
            elif self.stream_list[stream_id]['status'] == "restarting":
                stream_statistic['uptime'] = time.time() - self.stream_list[stream_id]['start_time']
            else:
                stream_statistic['uptime'] = time.time() - self.stream_list[stream_id]['start_time']
            try:
                stream_receives_per_second = self.stream_list[stream_id]['processed_receives_total'] / stream_statistic['uptime']
            except ZeroDivisionError:
                stream_receives_per_second = 0
            stream_statistic['stream_receives_per_second'] = stream_receives_per_second
            if stream_statistic['uptime'] > 60:
                stream_statistic['stream_receives_per_minute'] = stream_receives_per_second * 60
            if stream_statistic['uptime'] > 60 * 60:
                stream_statistic['stream_receives_per_hour'] = stream_receives_per_second * 60 * 60
            if stream_statistic['uptime'] > 60 * 60 * 24:
                stream_statistic['stream_receives_per_day'] = stream_receives_per_second * 60 * 60 * 24
            if stream_statistic['uptime'] > 60 * 60 * 24 * 30:
                stream_statistic['stream_receives_per_month'] = stream_receives_per_second * 60 * 60 * 24 * 30
            if stream_statistic['uptime'] > 60 * 60 * 24 * 30 * 12:
                stream_statistic['stream_receives_per_year'] = stream_receives_per_second * 60 * 60 * 24 * 30 * 12
            return stream_statistic
        except KeyError:
            return None

    def get_the_one_active_websocket_api(self) -> Optional[str]:
        """
        This function is needed to simplify the access to the websocket API, if only one API stream exists it is clear
        that only this stream can be used for the requests and therefore will be used.

        :return: stream_id or None (str)
        """
        found_entries = 0
        found_stream_id = None
        with self.stream_list_lock:
            logger.debug(f"BybitWebSocketApiManager.get_the_one_active_websocket_api() - `stream_list_lock` "
                         f"was entered!")
            for stream_id in self.stream_list:
                if self.stream_list[stream_id]['api'] is True:
                    found_entries += 1
                    found_stream_id = stream_id
            logger.debug(f"BybitWebSocketApiManager.get_the_one_active_websocket_api() - Leaving `stream_list_lock`!")
        if found_entries == 1:
            # Its clear, there is only one valid connection to use, so we can take it!
            logger.debug(f"BybitWebSocketApiManager.get_the_one_active_websocket_api() - Found `stream_id` "
                         f"`{found_stream_id}`")
            return found_stream_id
        else:
            logger.error(f"BybitWebSocketApiManager.get_the_one_active_websocket_api() - No valid `stream_id` found! "
                         f"- `found_entries` = {found_entries}")
            return None

    def get_total_received_bytes(self):
        """
        Get number of total received bytes

        :return: int
        """
        # how many bytes did we receive till now?
        return self.total_received_bytes

    def get_total_receives(self):
        """
        Get the number of total receives

        :return: int
        """
        return self.total_receives

    def get_user_agent(self):
        """
        Get the user_agent string "lib name + lib version + python version"

        :return:
        """
        user_agent = f"{self.name}_{str(self.get_version())}-python_{str(platform.python_version())}"
        return user_agent

    def get_version(self):
        """
        Get the package/module version

        :return: str
        """
        return self.version

    @staticmethod
    def help():
        """
        Help in iPython
        """
        print("Ctrl+D to close")

    def increase_received_bytes_per_second(self, stream_id, size):
        """
        Add the amount of received bytes per second

        :param stream_id: id of a stream
        :type stream_id: str
        :param size: amount of bytes to add
        :type size: int
        """
        current_timestamp = int(time.time())
        try:
            if self.stream_list[stream_id]['transfer_rate_per_second']['bytes'][current_timestamp]:
                pass
        except KeyError:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.increase_received_bytes_per_second() - `stream_list_lock` "
                             f"was entered!")
                self.stream_list[stream_id]['transfer_rate_per_second']['bytes'][current_timestamp] = 0
                logger.debug(f"BybitWebSocketApiManager.increase_received_bytes_per_second() - Leaving `stream_list_lock`!")
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.increase_received_bytes_per_second() - `stream_list_lock` "
                             f"was entered!")
                self.stream_list[stream_id]['transfer_rate_per_second']['bytes'][current_timestamp] += size
                logger.debug(f"BybitWebSocketApiManager.increase_received_bytes_per_second() - Leaving `stream_list_lock`!")
        except KeyError:
            pass

    def increase_processed_receives_statistic(self, stream_id):
        """
        Add the number of processed receives

        :param stream_id: id of a stream
        :type stream_id: str
        """
        current_timestamp = int(time.time())
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.increase_processed_receives_statistic() - `stream_list_lock` "
                             f"was entered!")
                self.stream_list[stream_id]['processed_receives_total'] += 1
                logger.debug(f"BybitWebSocketApiManager.increase_processed_receives_statistic() - Leaving "
                             f"`stream_list_lock`!")
        except KeyError:
            return False
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.increase_processed_receives_statistic() - `stream_list_lock` "
                             f"was entered!")
                self.stream_list[stream_id]['receives_statistic_last_second']['entries'][current_timestamp] += 1
                logger.debug(f"BybitWebSocketApiManager.increase_processed_receives_statistic() - Leaving "
                             f"`stream_list_lock`!")
        except KeyError:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.increase_processed_receives_statistic() - `stream_list_lock` "
                             f"was entered!")
                self.stream_list[stream_id]['receives_statistic_last_second']['entries'][current_timestamp] = 1
                logger.debug(f"BybitWebSocketApiManager.increase_processed_receives_statistic() - Leaving "
                             f"`stream_list_lock`!")
        with self.total_receives_lock:
            self.total_receives += 1

    def increase_reconnect_counter(self, stream_id=None):
        """
        Increase reconnect counter

        :param stream_id: id of a stream
        :type stream_id: str
        """
        with self.stream_list_lock:
            logger.debug(f"BybitWebSocketApiManager.increase_reconnect_counter() - `stream_list_lock` was entered!")
            self.stream_list[stream_id]['logged_reconnects'].append(time.time())
            self.stream_list[stream_id]['reconnects'] += 1
            logger.debug(f"BybitWebSocketApiManager.increase_reconnect_counter() - Leaving `stream_list_lock`!")
        with self.reconnects_lock:
            self.reconnects += 1

    def increase_transmitted_counter(self, stream_id):
        """
        Increase the counter of transmitted payloads
        :param stream_id: id of a stream
        :type stream_id: str
        """
        with self.stream_list_lock:
            logger.debug(f"BybitWebSocketApiManager.increase_transmitted_counter() - `stream_list_lock` was entered!")
            self.stream_list[stream_id]['processed_transmitted_total'] += 1
            logger.debug(f"BybitWebSocketApiManager.increase_transmitted_counter() - Leaving `stream_list_lock`!")
        with self.total_transmitted_lock:
            logger.debug(f"BybitWebSocketApiManager.increase_transmitted_counter() - `stream_list_lock` was entered!")
            self.total_transmitted += 1
            logger.debug(f"BybitWebSocketApiManager.increase_transmitted_counter() - Leaving `stream_list_lock`!")

    def is_manager_stopping(self):
        """
        Returns `True` if the manager has a stop request, 'False' if not.

        :return: bool
        """
        if self.stop_manager_request is False:
            return False
        else:
            return True

    def is_crash_request(self, stream_id) -> bool:
        """
        Has a specific stream a crash_request?

        :param stream_id: id of a stream
        :type stream_id: str
        :return: bool
        """
        logger.debug(f"BybitWebSocketApiManager.is_stop_request({stream_id}){self.get_debug_log()}")
        try:
            if self.stream_list[stream_id]['crash_request'] is True:
                return True
            else:
                return False
        except KeyError:
            return False

    def is_stop_request(self, stream_id) -> bool:
        """
        Has a specific stream a stop_request?

        :param stream_id: id of a stream
        :type stream_id: str
        :return: bool
        """
        logger.debug(f"BybitWebSocketApiManager.is_stop_request({stream_id}){self.get_debug_log()}")
        try:
            if self.stream_list[stream_id]['stop_request'] is True:
                return True
            elif self.is_manager_stopping():
                return True
            else:
                return False
        except KeyError:
            return False

    def is_stream_signal_buffer_enabled(self):
        """
        Is the stream_signal_buffer enabled?

        :return: bool
        """
        return self.enable_stream_signal_buffer

    def is_update_available(self):
        """
        Is a new release of this package available?

        :return: bool
        """
        installed_version = self.get_version()
        if ".dev" in installed_version:
            installed_version = installed_version[:-4]
        if self.get_latest_version() == installed_version:
            return False
        elif self.get_latest_version() is None:
            return False
        else:
            return True

    def is_update_available_check_command(self, check_command_version=None):
        """
        Is a new release of `check_lucit_collector.py` available?

        :return: bool
        """
        installed_version = check_command_version
        latest_version = self.get_latest_version_check_command()
        if ".dev" in str(installed_version):
            installed_version = installed_version[:-4]
        if latest_version == installed_version:
            return False
        elif latest_version is None:
            return False
        else:
            return True

    def pop_stream_data_from_stream_buffer(self, stream_buffer_name: Union[Literal[False], str] = None, mode="FIFO"):
        """
        Get oldest or latest entry from
        `stream_buffer <https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/%60stream_buffer%60>`__
        and remove from FIFO/LIFO stack.

        :param stream_buffer_name: `False` to read from generic stream_buffer, the stream_id if you used True in
                                   create_stream() or the string name of a shared stream_buffer.
        :type stream_buffer_name: False or str
        :param mode: How to read from the `stream_buffer` - "FIFO" (default) or "LIFO".
        :type mode: str
        :return: stream_data - str, dict or None
        """
        if stream_buffer_name is None:
            try:
                with self.stream_buffer_lock:
                    if mode.upper() == "FIFO":
                        stream_data = self.stream_buffer.popleft()
                    elif mode.upper() == "LIFO":
                        stream_data = self.stream_buffer.pop()
                    else:
                        return None
                return stream_data
            except IndexError:
                return None
        else:
            try:
                with self.stream_buffer_locks[stream_buffer_name]:
                    if mode.upper() == "FIFO":
                        stream_data = self.stream_buffers[stream_buffer_name].popleft()
                    elif mode.upper() == "LIFO":
                        stream_data = self.stream_buffers[stream_buffer_name].pop()
                    else:
                        return None
                return stream_data
            except IndexError:
                return None
            except KeyError:
                return None

    def pop_stream_signal_from_stream_signal_buffer(self):
        """
        Get the oldest entry from
        `stream_signal_buffer <https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/%60stream_signal_buffer%60>`__
        and remove from stack/pipe (FIFO stack)

        :return: stream_signal - dict or False
        """
        try:
            with self.stream_signal_buffer_lock:
                stream_signal = self.stream_signal_buffer.popleft()
            return stream_signal
        except IndexError:
            return False

    def print_stream_info(self, stream_id: str = None, add_string: str = None, footer: str = None, title: str = None):
        """
        Print all infos about a specific stream, helps debugging :)

        :param stream_id: id of a stream
        :type stream_id: str
        :param add_string: text to add to the output
        :type add_string: str
        :param footer: set a footer (last row) for print_summary output
        :type footer: str
        :param title: set to `True` to use curses instead of print()
        :type title: str
        """
        bybit_api_status_row = ""
        stream_label_row = ""
        status_row = ""
        payload_row = ""
        symbol_row = ""
        last_static_ping_listen_key = ""
        stream_info = self.get_stream_info(stream_id)
        if add_string is None:
            add_string = ""
        else:
            add_string = f" {add_string}\r\n"
        if self.socks5_proxy_address is not None and self.socks5_proxy_port is not None:
            proxy = f"\r\n proxy: {self.socks5_proxy_address}:{self.socks5_proxy_port} (ssl:" \
                    f"{self.socks5_proxy_ssl_verification})"
        else:
            proxy = ""
        try:
            if len(self.stream_list[stream_id]['logged_reconnects']) > 0:
                logged_reconnects_row = "\r\n logged_reconnects: "
                row_prefix = ""
                with self.stream_list_lock:
                    logger.debug(f"BybitWebSocketApiManager.print_stream_info() - `stream_list_lock` was entered!")
                    for timestamp in self.stream_list[stream_id]['logged_reconnects']:
                        logged_reconnects_row += row_prefix + \
                                                 self.get_date_of_timestamp(timestamp)
                        row_prefix = ", "
                    logger.debug(f"BybitWebSocketApiManager.print_stream_info() - Leaving `stream_list_lock`!")
            else:
                logged_reconnects_row = ""
        except KeyError:
            return False
        if "running" in stream_info['status']:
            stream_row_color_prefix = "\033[1m\033[32m"
            stream_row_color_suffix = "\033[0m\r\n"
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.print_stream_info() - `stream_list_lock` was entered!")
                for reconnect_timestamp in self.stream_list[stream_id]['logged_reconnects']:
                    if (time.time() - reconnect_timestamp) < 2:
                        stream_row_color_prefix = "\033[1m\033[33m"
                        stream_row_color_suffix = "\033[0m\r\n"
                logger.debug(f"BybitWebSocketApiManager.print_stream_info() - Leaving `stream_list_lock`!")
            status_row = stream_row_color_prefix + " status: " + str(stream_info['status']) + stream_row_color_suffix
        elif "crashed" in stream_info['status']:
            stream_row_color_prefix = "\033[1m\033[31m"
            stream_row_color_suffix = "\033[0m\r\n"
            status_row = stream_row_color_prefix + " status: " + str(stream_info['status']) + stream_row_color_suffix
        elif "restarting" in stream_info['status']:
            stream_row_color_prefix = "\033[1m\033[33m"
            stream_row_color_suffix = "\033[0m\r\n"
            status_row = stream_row_color_prefix + " status: " + str(stream_info['status']) + stream_row_color_suffix
        elif "stopped" in stream_info['status']:
            stream_row_color_prefix = "\033[1m\033[33m"
            stream_row_color_suffix = "\033[0m\r\n"
            status_row = stream_row_color_prefix + " status: " + str(stream_info['status']) + stream_row_color_suffix
        if "!userData" in self.stream_list[stream_id]['markets'] or "!userData" in self.stream_list[stream_id]['channels']:
            last_static_ping_listen_key = " last_static_ping_listen_key: " + \
                                          str(self.stream_list[stream_id]['last_static_ping_listen_key']) + "\r\n"
            if self.bybit_api_status['status_code'] == 200:
                bybit_api_status_code = "\033[1m\033[32m" + str(self.bybit_api_status['status_code']) + \
                                          "\033[0m"
            else:
                bybit_api_status_code = "\033[1m\033[31m" + str(self.bybit_api_status['status_code']) + \
                                          "\033[0m"
            bybit_api_status_row = " bybit_api_status: weight=" + str(self.bybit_api_status['weight']) + \
                                     ", status_code=" + str(bybit_api_status_code) + f" (last update " + \
                                     str(self.get_date_of_timestamp(self.bybit_api_status['timestamp'])) + \
                                     ")\r\n"
        current_receiving_speed = str(self.get_human_bytesize(self.get_current_receiving_speed(stream_id), "/s"))
        if self.stream_list[stream_id]['symbols'] is not None:
            symbol_row = " symbols:" + str(stream_info['symbols']) + "\r\n"
        if self.stream_list[stream_id]["payload"]:
            payload_row = " payload: " + str(self.stream_list[stream_id]["payload"]) + "\r\n"
        if self.stream_list[stream_id]["stream_label"] is not None:
            stream_label_row = " stream_label: " + self.stream_list[stream_id]["stream_label"] + "\r\n"
        if isinstance(stream_info['ping_interval'], int):
            ping_interval = f"{stream_info['ping_interval']} seconds"
        else:
            ping_interval = stream_info['ping_interval']
        if isinstance(stream_info['ping_timeout'], int):
            ping_timeout = f"{stream_info['ping_timeout']} seconds"
        else:
            ping_timeout = stream_info['ping_timeout']
        if isinstance(stream_info['close_timeout'], int):
            close_timeout = f"{stream_info['close_timeout']} seconds"
        else: 
            close_timeout = stream_info['close_timeout']
        if title is not None:
            first_row = str(self.fill_up_space_centered(96, f" {title} ", "=")) + "\r\n"
            last_row = str(self.fill_up_space_centered(96, f" Powered by {self.get_user_agent()} ", "=")) + "\r\n"
        else:
            first_row = str(self.fill_up_space_centered(96, f"{self.get_user_agent()} ", "=")) + "\r\n"
            last_row = "========================================================================================" \
                       "=======\r\n"
        if footer is not None:
            last_row = str(self.fill_up_space_centered(96, f" {footer} ", "=")) + "\r\n"
        try:
            uptime = self.get_human_uptime(stream_info['processed_receives_statistic']['uptime'])
            print(first_row +
                  " exchange: " + str(self.stream_list[stream_id]['exchange']) + f"{proxy}\r\n" +
                  str(add_string) +
                  " stream_id:", str(stream_id), "\r\n" +
                  str(stream_label_row) +
                  " stream_buffer_maxlen:", str(stream_info['stream_buffer_maxlen']), "\r\n" +
                  f" api: {self.stream_list[stream_id]['api']}\r\n" +
                  " channels (" + str(len(stream_info['channels'])) + "):", str(stream_info['channels']), "\r\n" +
                  " markets (" + str(len(stream_info['markets'])) + "):", str(stream_info['markets']), "\r\n" +
                  f" websocket_uri: {self.stream_list[stream_id]['websocket_uri']}\r\n" +
                  str(symbol_row) +
                  " subscriptions: " + str(self.stream_list[stream_id]['subscriptions']) + "\r\n" +
                  str(payload_row) +
                  str(status_row) +
                  f" ping_interval: {ping_interval}\r\n"
                  f" ping_timeout: {ping_timeout}\r\n"
                  f" close_timeout: {close_timeout}\r\n"
                  " start_time:", str(stream_info['start_time']), "\r\n"
                  " uptime:", str(uptime),
                  f"since {datetime.fromtimestamp(stream_info['start_time'], timezone.utc).strftime('%Y-%m-%d, %H:%M:%S UTC')}\r\n" +
                  " reconnects:", str(stream_info['reconnects']), logged_reconnects_row, "\r\n" +
                  str(bybit_api_status_row) +
                  str(last_static_ping_listen_key) +
                  " last_heartbeat:", str(stream_info['last_heartbeat']), "\r\n"
                  " seconds_to_last_heartbeat:", str(stream_info['seconds_to_last_heartbeat']), "\r\n"
                  " stop_request:", str(stream_info['stop_request']), "\r\n"                                                                      
                  " has_stopped:", str(stream_info['has_stopped']), "\r\n"
                  " seconds_since_has_stopped:",
                  str(stream_info['seconds_since_has_stopped']), "\r\n"
                  " current_receiving_speed:", str(current_receiving_speed), "\r\n" +
                  " processed_receives:", str(stream_info['processed_receives_total']), "\r\n" +
                  " transmitted_payloads:", str(self.stream_list[stream_id]['processed_transmitted_total']), "\r\n" +
                  " stream_most_receives_per_second:",
                  str(stream_info['receives_statistic_last_second']['most_receives_per_second']), "\r\n"
                  " stream_receives_per_second:",
                  str(stream_info['processed_receives_statistic']['stream_receives_per_second'].__round__(3)), "\r\n"
                  " stream_receives_per_minute:",
                  str(stream_info['processed_receives_statistic']['stream_receives_per_minute'].__round__(3)), "\r\n"
                  " stream_receives_per_hour:",
                  str(stream_info['processed_receives_statistic']['stream_receives_per_hour'].__round__(3)), "\r\n"
                  " stream_receives_per_day:",
                  str(stream_info['processed_receives_statistic']['stream_receives_per_day'].__round__(3)), "\r\n" +
                  last_row)
        except KeyError:
            self.print_stream_info(stream_id)

    def print_summary(self, add_string: str = None, disable_print: bool = False, footer: str = None, title: str = None):
        """
        Print an overview of all streams
        
        :param add_string: text to add to the output
        :type add_string: str
        :param disable_print: set to `True` to use curses instead of print()
        :type disable_print: bool
        :param footer: set a footer (last row) for print_summary output
        :type footer: str
        :param title: set a title (first row) for print_summary output
        :type title: str
        """
        streams = len(self.stream_list)
        active_streams = 0
        crashed_streams = 0
        restarting_streams = 0
        stopped_streams = 0
        active_streams_row = ""
        restarting_streams_row = ""
        stopped_streams_row = ""
        all_receives_per_second = 0.0
        current_receiving_speed = 0
        streams_with_stop_request = 0
        stream_rows = ""
        crashed_streams_row = ""
        bybit_api_status_row = ""
        received_bytes_per_x_row = ""
        streams_with_stop_request_row = ""
        stream_buffer_row = ""
        highest_receiving_speed_row = f"{str(self.get_human_bytesize(self.receiving_speed_peak['value'], '/s'))} " \
                                      f"(reached at " \
                                      f"{self.get_date_of_timestamp(self.receiving_speed_peak['timestamp'])})"

        if self.socks5_proxy_address is not None and self.socks5_proxy_port is not None:
            proxy = f"\r\n proxy: {self.socks5_proxy_address}:{self.socks5_proxy_port} (ssl_verification: " \
                    f"{self.socks5_proxy_ssl_verification})"
        else:
            proxy = ""

        if add_string is None:
            add_string = ""
        else:
            add_string = f" {add_string}\r\n"
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.print_summary() - `stream_list_lock` was entered!")
                temp_stream_list = copy.deepcopy(self.stream_list)
                logger.debug(f"BybitWebSocketApiManager.print_summary() - Leaving `stream_list_lock`!")
        except RuntimeError:
            return ""
        except TypeError:
            return ""
        for stream_id in temp_stream_list:
            stream_row_color_prefix = ""
            stream_row_color_suffix = ""
            current_receiving_speed += self.get_current_receiving_speed(stream_id)
            stream_statistic = self.get_stream_statistic(stream_id)
            if self.stream_list[stream_id]['status'] == "running":
                active_streams += 1
                all_receives_per_second += stream_statistic['stream_receives_per_second']
                try:
                    with self.stream_list_lock:
                        logger.debug(f"BybitWebSocketApiManager.print_summary() - `stream_list_lock` was entered!")
                        for reconnect_timestamp in self.stream_list[stream_id]['logged_reconnects']:
                            if (time.time() - reconnect_timestamp) < 1:
                                stream_row_color_prefix = "\033[1m\033[31m"
                                stream_row_color_suffix = "\033[0m"
                            elif (time.time() - reconnect_timestamp) < 2:
                                stream_row_color_prefix = "\033[1m\033[33m"
                                stream_row_color_suffix = "\033[0m"
                            elif (time.time() - reconnect_timestamp) < 4:
                                stream_row_color_prefix = "\033[1m\033[32m"
                                stream_row_color_suffix = "\033[0m"
                        logger.debug(f"BybitWebSocketApiManager.print_summary() - Leaving `stream_list_lock`!")
                except KeyError:
                    pass
            elif self.stream_list[stream_id]['status'] == "stopped":
                stopped_streams += 1
                stream_row_color_prefix = "\033[1m\033[33m"
                stream_row_color_suffix = "\033[0m"
            elif self.stream_list[stream_id]['status'] == "restarting":
                restarting_streams += 1
                stream_row_color_prefix = "\033[1m\033[33m"
                stream_row_color_suffix = "\033[0m"
            elif "crashed" in self.stream_list[stream_id]['status']:
                crashed_streams += 1
                stream_row_color_prefix = "\033[1m\033[31m"
                stream_row_color_suffix = "\033[0m"
            if self.stream_list[stream_id]['stream_label'] is not None:
                if len(self.stream_list[stream_id]['stream_label']) > 18:
                    stream_label = str(self.stream_list[stream_id]['stream_label'])[:13] + "..."
                else:
                    stream_label = str(self.stream_list[stream_id]['stream_label'])
            else:
                stream_label = str(self.stream_list[stream_id]['stream_label'])
            stream_rows += stream_row_color_prefix + str(stream_id) + stream_row_color_suffix + " |" + \
                self.fill_up_space_right(17, stream_label) + "|" + \
                self.fill_up_space_left(8, self.get_stream_receives_last_second(stream_id)) + "|" + \
                self.fill_up_space_left(11, str(stream_statistic['stream_receives_per_second'].__round__(2))) + "|" + \
                self.fill_up_space_left(8, self.stream_list[stream_id]['receives_statistic_last_second']['most_receives_per_second']) \
                + "|" + stream_row_color_prefix + \
                self.fill_up_space_left(8, str(len(self.stream_list[stream_id]['logged_reconnects']))) + \
                stream_row_color_suffix + "\r\n "
            if self.is_stop_request(stream_id) is True and \
                    self.stream_list[stream_id]['status'] == "running":
                streams_with_stop_request += 1
        if streams_with_stop_request >= 1:
            stream_row_color_prefix = "\033[1m\033[33m"
            stream_row_color_suffix = "\033[0m"
            streams_with_stop_request_row = (stream_row_color_prefix + " streams_with_stop_request: " +
                                             str(streams_with_stop_request) + stream_row_color_suffix + "\r\n")
        if crashed_streams >= 1:
            stream_row_color_prefix = "\033[1m\033[31m"
            stream_row_color_suffix = "\033[0m"
            crashed_streams_row = (stream_row_color_prefix + " crashed_streams: " + str(crashed_streams) +
                                   stream_row_color_suffix + "\r\n")
        total_received_bytes = str(self.get_total_received_bytes()) + " (" + str(
            self.get_human_bytesize(self.get_total_received_bytes())) + ")"
        try:
            received_bytes_per_second = self.get_total_received_bytes() / (time.time() - self.start_time)
            received_bytes_per_x_row += (str(self.get_human_bytesize(int(received_bytes_per_second), '/s')) +
                                         " (per day " + str(((received_bytes_per_second / 1024 / 1024 / 1024) * 60 *
                                                             60 * 24).__round__(2)) + " gB)")
            if self.get_stream_buffer_length() > 50:
                stream_row_color_prefix = "\033[1m\033[34m"
                stream_row_color_suffix = "\033[0m"
                stream_buffer_row += (stream_row_color_prefix + " stream_buffer_stored_items: " +
                                      str(self.get_stream_buffer_length()) + "\r\n")
                stream_buffer_row += " stream_buffer_byte_size: " + str(self.get_stream_buffer_byte_size()) + \
                                     " (" + str(self.get_human_bytesize(self.get_stream_buffer_byte_size())) + ")" + \
                                     stream_row_color_suffix + "\r\n"
            if active_streams > 0:
                active_streams_row = " \033[1m\033[32mactive_streams: " + str(active_streams) + "\033[0m\r\n"
            if restarting_streams > 0:
                restarting_streams_row = " \033[1m\033[33mrestarting_streams: " + str(restarting_streams) + "\033[0m\r\n"
            if stopped_streams > 0:
                stopped_streams_row = " \033[1m\033[33mstopped_streams: " + str(stopped_streams) + "\033[0m\r\n"
            if self.bybit_api_status['weight'] is not None:
                if self.bybit_api_status['status_code'] == 200:
                    bybit_api_status_code = "\033[1m\033[32m" + str(self.bybit_api_status['status_code']) + \
                                              "\033[0m"
                else:
                    bybit_api_status_code = "\033[1m\033[31m" + str(self.bybit_api_status['status_code']) + \
                                              "\033[0m"
                bybit_api_status_row = " bybit_api_status: weight=" + \
                                         str(self.bybit_api_status['weight']) + \
                                         ", status_code=" + str(bybit_api_status_code) + " (last update " + \
                                         str(self.get_date_of_timestamp(self.bybit_api_status['timestamp'])) + ")\r\n"

            if title is not None:
                first_row = str(self.fill_up_space_centered(96, f" {title} ", "=")) + "\r\n"
                last_row = str(self.fill_up_space_centered(96, f" Powered by {self.get_user_agent()} ", "=")) + "\r\n"
            else:
                first_row = str(self.fill_up_space_centered(96, f" {self.get_user_agent()} ", "=")) + "\r\n"
                last_row = "========================================================================================" \
                           "=======\r\n"
            if footer is not None:
                last_row = str(self.fill_up_space_centered(96, f" {footer} ", "=")) + "\r\n"

            try:
                print_text = (
                    first_row +
                    " exchange: " + str(self.exchange) + f"{proxy}\r\n" +
                    " uptime: " + str(self.get_human_uptime(int(time.time() - self.start_time))) + " since " +
                    str(self.get_date_of_timestamp(self.start_time)) + "\r\n" +
                    " streams: " + str(streams) + "\r\n" +
                    str(active_streams_row) +
                    str(crashed_streams_row) +
                    str(restarting_streams_row) +
                    str(stopped_streams_row) +
                    str(streams_with_stop_request_row) +
                    " subscriptions: " + str(self.get_number_of_all_subscriptions()) + "\r\n" +
                    str(stream_buffer_row) +
                    " current_receiving_speed: " + str(self.get_human_bytesize(current_receiving_speed, "/s")) + "\r\n" +
                    " average_receiving_speed: " + str(received_bytes_per_x_row) + "\r\n" +
                    " highest_receiving_speed: " + str(highest_receiving_speed_row) + "\r\n" +
                    " total_receives: " + str(self.total_receives) + "\r\n"
                    " total_received_bytes: " + str(total_received_bytes) + "\r\n"
                    " total_transmitted_payloads: " + str(self.total_transmitted) + "\r\n" +
                    " stream_buffer_maxlen: " + str(self.stream_buffer_maxlen) + "\r\n" +
                    str(bybit_api_status_row) +
                    " process_ressource_usage: cpu=" + str(self.get_process_usage_cpu()) + "%, memory=" +
                    str(self.get_process_usage_memory()) + ", threads=" + str(self.get_process_usage_threads()) +
                    "\r\n" + str(add_string) +
                    " ---------------------------------------------------------------------------------------------\r\n"
                    "               stream_id              |   stream_label  |  last  |  average  |  peak  | recon\r\n"
                    " ---------------------------------------------------------------------------------------------\r\n"
                    " " + str(stream_rows) +
                    "---------------------------------------------------------------------------------------------\r\n"
                    " all_streams                                            |" +
                    self.fill_up_space_left(8, str(self.get_all_receives_last_second())) + "|" +
                    self.fill_up_space_left(11, str(all_receives_per_second.__round__(2))) + "|" +
                    self.fill_up_space_left(8, str(self.most_receives_per_second)) + "|" +
                    self.fill_up_space_left(8, str(self.reconnects)) + "\r\n" +
                    last_row
                )
                if disable_print:
                    if sys.platform.startswith('Windows'):
                        print_text = self.remove_ansi_escape_codes(print_text)
                    return print_text
                else:
                    print(print_text)
            except UnboundLocalError:
                pass
        except ZeroDivisionError:
            pass

    def print_summary_to_png(self,
                             print_summary_export_path,
                             height_per_row=12.5,
                             add_string: str = None,
                             footer: str = None,
                             title: str = None):
        """
        Create a PNG image file with the console output of `print_summary()`

        *LINUX ONLY* It should not be hard to make it OS independent:
        https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/issues/61

        :param print_summary_export_path: If you want to export the output of print_summary() to an image,
                                          please provide a path like "/var/www/html/". `View the Wiki!
                                          <https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki/How-to-export-print_summary()-stdout-to-PNG%3F>`__
        :type print_summary_export_path: str
        :param height_per_row: set the height per row for the image height calculation
        :type height_per_row: float
        :param add_string: text to add to the output
        :type add_string: str
        :param footer: set a footer (last row) for print_summary output
        :type footer: str
        :param title: set a title (first row) for print_summary output
        :type title: str
        :return: bool
        """
        print_text = self.print_summary(disable_print=True, add_string=add_string, footer=footer, title=title)
        # Todo:
        # 1. Handle paths right
        # 2. Use PythonMagick instead of Linux ImageMagick
        with open(print_summary_export_path + "print_summary.txt", 'w') as text_file:
            print(self.remove_ansi_escape_codes(print_text), file=text_file)
            try:
                image_height = print_text.count("\n") * height_per_row + 15
            except AttributeError:
                return False
        os.system('convert -size 720x' + str(image_height) + ' xc:black -font "FreeMono" -pointsize 12 -fill white '
                  '-annotate +30+30 "@' + print_summary_export_path + 'print_summary.txt' + '" ' +
                  print_summary_export_path + 'print_summary_plain.png')
        os.system('convert ' + print_summary_export_path + 'print_summary_plain.png -font "FreeMono" '
                  '-pointsize 12 -fill red -undercolor \'#00000080\' -gravity North -annotate +0+5 '
                  '"$(date)" ' + print_summary_export_path + 'print_summary.png')
        return True

    @staticmethod
    def remove_ansi_escape_codes(text):
        """
        Remove ansi escape codes from the text string!

        :param text: The text :)
        :type text: str

        :return: str
        """
        text = str(text)
        text = text.replace("\033[1m\033[31m", "")
        text = text.replace("\033[1m\033[32m", "")
        text = text.replace("\033[1m\033[33m", "")
        text = text.replace("\033[1m\033[34m", "")
        text = text.replace("\033[0m", "")
        return text

    def replace_stream(self,
                       stream_id,
                       new_channels,
                       new_markets,
                       new_stream_label=None,
                       new_stream_buffer_name: Union[Literal[False], str] = False,
                       new_api_key=None,
                       new_api_secret=None,
                       new_symbols=None,
                       new_output: Optional[Literal['dict', 'raw_data']] = None,
                       new_ping_interval=20,
                       new_ping_timeout=20,
                       new_close_timeout=10,
                       new_stream_buffer_maxlen=None):
        """
        Replace a stream

        If you want to start a stream with a new config, its recommended, to first start a new stream with the new
        settings and close the old stream not before the new stream received its first data. So your data will stay
        consistent.

        :param stream_id: id of the old stream
        :type stream_id: str
        :param new_channels: the new channel list for the stream
        :type new_channels: str, tuple, list, set
        :param new_markets: the new markets list for the stream
        :type new_markets: str, tuple, list, set
        :param new_stream_label: provide a stream_label to identify the stream
        :type new_stream_label: str
        :param new_stream_buffer_name: If `False` the data is going to get written to the default stream_buffer,
                                   set to `True` to read the data via `pop_stream_data_from_stream_buffer(stream_id)` or
                                   provide a string to create and use a shared stream_buffer and read it via
                                   `pop_stream_data_from_stream_buffer('string')`.
        :type new_stream_buffer_name: False or str
        :param new_api_key: provide a valid Bybit API key
        :type new_api_key: str
        :param new_api_secret: provide a valid Bybit API secret
        :type new_api_secret: str
        :param new_symbols: provide the symbols for isolated_margin user_data streams
        :type new_symbols: str
        :return: new stream_id
        :param new_output: set to "dict" to convert the received raw data to a python dict - otherwise
                           the output remains unchanged and gets delivered as received from the endpoints
        :type new_output: str
        :param new_ping_interval: Once the connection is open, a `Ping frame` is sent every
                                  `ping_interval` seconds. This serves as a keepalive. It helps keeping
                                  the connection open, especially in the presence of proxies with short
                                  timeouts on inactive connections. Set `ping_interval` to `None` to
                                  disable this behavior. (default: 20)
                                  This parameter is passed through to the `websockets.client.connect()
                                  <https://websockets.readthedocs.io/en/stable/api.html?highlight=ping_interval#websockets.client.connect>`__
        :type new_ping_interval: int or None
        :param new_ping_timeout: If the corresponding `Pong frame` isn't received within
                                 `ping_timeout` seconds, the connection is considered unusable and is closed with
                                 code 1011. This ensures that the remote endpoint remains responsive. Set
                                 `ping_timeout` to `None` to disable this behavior. (default: 20)
                                 This parameter is passed through to the `websockets.client.connect()
                                 <https://websockets.readthedocs.io/en/stable/api.html?highlight=ping_interval#websockets.client.connect>`__
        :type new_ping_timeout: int or None
        :param new_close_timeout: The `close_timeout` parameter defines a maximum wait time in seconds for
                                  completing the closing handshake and terminating the TCP connection. (default: 10)
                                  This parameter is passed through to the `websockets.client.connect()
                                  <https://websockets.readthedocs.io/en/stable/api.html?highlight=ping_interval#websockets.client.connect>`__
        :type new_close_timeout: int or None
        :param new_stream_buffer_maxlen: Set a max len for the `stream_buffer`. Only used in combination with a non-generic
                                     `stream_buffer`. The generic `stream_buffer` uses always the value of
                                     `BybitWebSocketApiManager()`.
        :type new_stream_buffer_maxlen: int or None
        :return: stream_id or 'None'
        """
        # starting a new socket and stop the old stream not before the new stream received its first record
        new_stream_id = self.create_stream(new_channels,
                                           new_markets,
                                           new_stream_label,
                                           new_stream_buffer_name,
                                           new_api_key,
                                           new_api_secret,
                                           new_symbols,
                                           new_output,
                                           new_ping_interval,
                                           new_ping_timeout,
                                           new_close_timeout,
                                           new_stream_buffer_maxlen)
        if self.wait_till_stream_has_started(new_stream_id):
            self.stop_stream(stream_id=stream_id, delete_listen_key=False)
        return new_stream_id

    def run(self):
        """
        This method overloads `threading.run()` and starts management functions
        """
        time.sleep(1)
        loop = None
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            if self.debug is True:
                loop.set_debug(enabled=True)
            if self.auto_data_cleanup_stopped_streams is True:
                loop.create_task(self._auto_data_cleanup_stopped_streams(60, 900))  # Interval, Age
            loop.run_until_complete(self._frequent_checks())
        except OSError as error_msg:
            logger.critical(f"BybitWebSocketApiManager.run() - OSError - error_msg: {str(error_msg)}")
        except RuntimeError as error_msg:
            logger.debug(f"BybitWebSocketApiManager.run() - RuntimeError - error_msg: {str(error_msg)}")
        finally:
            logger.debug(f"BybitWebSocketApiManager.run() - Finally closing the loop!")
            if loop is not None:
                if loop.is_running():
                    try:
                        tasks = asyncio.all_tasks(loop)
                        loop.run_until_complete(self._shutdown_asyncgens(loop))
                        for task in tasks:
                            task.cancel()
                            try:
                                loop.run_until_complete(task)
                            except asyncio.CancelledError:
                                pass
                    except RuntimeError as error_msg:
                        logger.debug(f"BybitWebSocketApiManager.run() - RuntimeError - error_msg: {error_msg}")
                    except RuntimeWarning as error_msg:
                        logger.debug(f"BybitWebSocketApiManager.run() - RuntimeWarning - error_msg: {error_msg}")
                    except Exception as error_msg:
                        logger.debug(f"BybitWebSocketApiManager.run() finally - error_msg: {error_msg}")
                if not loop.is_closed():
                    loop.close()

    def set_heartbeat(self, stream_id) -> None:
        """
        Set heartbeat for a specific thread (should only be done by the stream itself)

        :return: None
        """
        logger.debug("BybitWebSocketApiManager.set_heartbeat(" + str(stream_id) + ")")
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.set_heartbeat() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['last_heartbeat'] = time.time()
                logger.debug(f"BybitWebSocketApiManager.set_heartbeat() - Leaving `stream_list_lock`!")
        except KeyError:
            pass
        return None

    def set_stop_request(self, stream_id=None):
        """
        Set a stop request for a specific stream.

        :return: None
        """
        if stream_id is None:
            return False
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.set_stop_request() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['stop_request'] = True
                logger.debug(f"BybitWebSocketApiManager.set_stop_request() - Leaving `stream_list_lock`!")
            return True
        except KeyError:
            return False

    def set_ringbuffer_error_max_size(self, max_size):
        """
        How many error messages should be kept in the ringbuffer?

        :param max_size: Max entries of error messages in the ringbuffer.
        :type max_size: int
        :return: bool
        """
        self.ringbuffer_error_max_size = int(max_size)

    def set_ringbuffer_result_max_size(self, max_size):
        """
        How many result messages should be kept in the ringbuffer?

        :param max_size: Max entries of result messages in the ringbuffer.
        :type max_size: int
        :return: bool
        """
        self.ringbuffer_result_max_size = int(max_size)

    def is_socket_ready(self, stream_id: str = None) -> bool:
        """
        Set `socket_is_ready` for a specific stream to False.

        :param stream_id: id of the stream
        :type stream_id: str
        """
        logger.debug(f"BybitWebSocketApiManager.is_socket_ready({stream_id}){self.get_debug_log()}")
        if self.socket_is_ready[stream_id] is True:
            return True
        else:
            return False

    def set_socket_is_not_ready(self, stream_id: str) -> bool:
        """
        Set `socket_is_ready` for a specific stream to False.

        :param stream_id: id of the stream
        :type stream_id: str
        :return: bool
        """
        logger.debug(f"BybitWebSocketApiManager.set_socket_is_not_ready({stream_id}){self.get_debug_log()}")
        self.socket_is_ready[stream_id] = False
        return True

    def set_socket_is_ready(self, stream_id: str) -> bool:
        """
        Set `socket_is_ready` for a specific stream to True.

        :param stream_id: id of the stream
        :type stream_id: str
        :return: bool
        """
        logger.debug(f"BybitWebSocketApiManager.set_socket_is_ready({stream_id}){self.get_debug_log()}")
        self.socket_is_ready[stream_id] = True
        return True

    def set_stream_label(self, stream_id, stream_label=None) -> bool:
        """
        Set a stream_label by stream_id

        :param stream_id: id of the stream
        :type stream_id: str
        :param stream_label: stream_label to set
        :type stream_label: str
        :return: bool
        """
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.set_stream_label() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['stream_label'] = stream_label
                logger.debug(f"BybitWebSocketApiManager.set_stream_label() - Leaving `stream_list_lock`!")
            return True
        except KeyError:
            return False

    def set_keep_max_received_last_second_entries(self, number_of_max_entries):
        """
        Set how much received_last_second entries are stored till they get deleted!

        :param number_of_max_entries: number of entries to keep in list
        :type number_of_max_entries: int
        """
        self.keep_max_received_last_second_entries = number_of_max_entries

    def split_payload(self, params, method, max_items_per_request=350):
        """
        Sending more than 8000 chars via websocket.send() leads to a connection loss, 350 list elements is a good limit
        to keep the payload length under 8000 chars and avoid reconnects

        :param params: params of subscribe payload
        :type params: list
        :param method: SUBSCRIBE or UNSUBSCRIBE
        :type method: str
        :param max_items_per_request: max size for params, if more it gets split
        :return: list or False
        """
        count_items = 0
        add_params = []
        payload = []
        for param in params:
            add_params.append(param)
            count_items += 1
            if count_items > max_items_per_request:
                add_payload = {"method": method,
                               "params": add_params,
                               "id": self.get_request_id()}
                payload.append(add_payload)
                count_items = 0
                add_params = []
        if len(add_params) > 0:
            add_payload = {"method": method,
                           "params": add_params,
                           "id": self.get_request_id()}
            payload.append(add_payload)
            return payload
        else:
            logger.error(f"BybitWebSocketApiManager.split_payload() result is None!")
            return None

    def stop_manager(self, close_api_session: bool = True):
        """
        Stop the BybitWebSocketApiManager with all streams, monitoring and management threads
        """
        logger.info("BybitWebSocketApiManager.stop_manager() - Stopping "
                    "unicorn_bybit_websocket_api_manager " + self.version + " ...")
        if self.stop_manager_request is False:
            # send signal to all threads
            self.stop_manager_request = True
            try:
                with self.stream_list_lock:
                    logger.debug(f"BybitWebSocketApiManager.stop_manager() - `stream_list_lock` was entered!")
                    stream_list = copy.deepcopy(self.stream_list)
                    logger.debug(f"BybitWebSocketApiManager.stop_manager() - Leaving `stream_list_lock`!")
                try:
                    for stream_id in stream_list:
                        self.stop_stream(stream_id)
                except AttributeError:
                    pass
            except AttributeError as error_msg:
                logger.debug(f"BybitWebSocketApiManager.stop_manager() - AttributeError: {error_msg}")
            # stop monitoring API services
            self.stop_monitoring_api()
            # stop restclient
            try:
                if self.exchange in CONNECTION_SETTINGS and self.restclient is not None:
                    self.restclient.stop()
            except AttributeError as error_msg:
                logger.debug(f"stop_manager() - AttributeError: {error_msg}")
            # close lucit license manger and the api session
            if close_api_session is True:
                self.llm.close()
            return True

    def stop_manager_with_all_streams(self, close_api_session: bool = True):
        """
        Stop the BybitWebSocketApiManager with all streams, monitoring and management threads

        Alias of 'stop_manager()'
        """
        logger.info("BybitWebSocketApiManager.stop_manager_with_all_streams() - Stopping "
                    "unicorn_bybit_websocket_api_manager " + self.version + " ...")

        self.stop_manager(close_api_session=close_api_session)

    def stop_monitoring_api(self) -> bool:
        """
        Stop the monitoring API service

        :return: bool
        """
        try:
            if self.monitoring_api_server is not None:
                self.monitoring_api_server.stop()
                time.sleep(1)
                return True
        except AttributeError:
            return True

    def stop_stream(self, stream_id, delete_listen_key: bool = True):
        """
        Stop a specific stream

        :param stream_id: id of a stream
        :type stream_id: str
        :param delete_listen_key: If set to `True` (default), the `listen_key` gets deleted. Set to `False` if you run
                                  more than one userData stream with this `listen_key`!
        :type delete_listen_key: str
        :return: bool
        """
        # stop a specific stream by stream_id
        logger.info(f"BybitWebSocketApiManager.stop_stream({stream_id}){self.get_debug_log()}")
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.stop_stream() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['stop_request'] = True
                logger.debug(f"BybitWebSocketApiManager.stop_stream() - Leaving `stream_list_lock`!")
        except KeyError:
            return False
        if delete_listen_key:
            try:
                self.delete_listen_key_by_stream_id(stream_id)
            except requests.exceptions.ReadTimeout as error_msg:
                logger.debug(f"BybitWebSocketApiManager.stop_stream() - Not able to delete listen_key - "
                             f"requests.exceptions.ReadTimeout: {error_msg}")
            except requests.exceptions.ConnectionError as error_msg:
                logger.debug(f"BybitWebSocketApiManager.stop_stream() - Not able to delete listen_key - "
                             f"requests.exceptions.ConnectionError: {error_msg}")
        return True

    def _crash_stream(self, stream_id, error_msg=None):
        """
        Loop inside: Stop a specific stream with 'crashed' status

        :param stream_id: id of a stream
        :type stream_id: str
        :param error_msg: Reason
        :type error_msg: str
        :return: bool
        """
        # stop a specific stream by stream_id
        logger.critical(f"BybitWebSocketApiManager._crash_stream({stream_id}){self.get_debug_log()}")
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager._crash_stream() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['crash_request'] = True
                self.stream_list[stream_id]['crash_request_reason'] = error_msg
                logger.debug(f"BybitWebSocketApiManager._crash_stream() - Leaving `stream_list_lock`!")
        except KeyError:
            return False
        return True

    def _stream_is_crashing(self, stream_id: str = None, error_msg: str = None) -> bool:
        """
        If a stream can not heal itself in cause of wrong parameter (wrong market, channel type) it calls this method

        :param stream_id: id of a stream
        :type stream_id: str
        :param error_msg: Error msg to add to the stream status!
        :type error_msg: str
        """
        logger.critical(f"BybitWebSocketApiManager._stream_is_crashing({stream_id}){self.get_debug_log()}")
        self.set_stop_request(stream_id=stream_id)
        with self.stream_list_lock:
            logger.debug(f"BybitWebSocketApiManager._stream_is_crashing() - `stream_list_lock` was entered!")
            self.stream_list[stream_id]['has_stopped'] = time.time()
            self.stream_list[stream_id]['status'] = "crashed"
            logger.debug(f"BybitWebSocketApiManager._stream_is_crashing() - Leaving `stream_list_lock`!")
        self.set_socket_is_ready(stream_id)
        if error_msg is not None:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager._stream_is_crashing() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['status'] += " - " + str(error_msg)
                logger.debug(f"BybitWebSocketApiManager._stream_is_crashing() - Leaving `stream_list_lock`!")
        else:
            if self.stream_list[stream_id]['crash_request_reason'] is not None:
                error_msg = self.stream_list[stream_id]['crash_request_reason']
        self.send_stream_signal(stream_id=stream_id, signal_type="STREAM_UNREPAIRABLE", error_msg=error_msg)
        return True

    def _stream_is_restarting(self, stream_id, error_msg=None):
        """
        Streams report with this call their restarts

        :param stream_id: id of a stream
        :type stream_id: str
        :return: bool
        """
        logger.info(f"BybitWebSocketApiManager._stream_is_restarting({stream_id}) - error_msg: {error_msg} - "
                    f"{self.get_debug_log()}")
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager._stream_is_restarting() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['status'] = "restarting"
                logger.debug(f"BybitWebSocketApiManager._stream_is_restarting() - Leaving `stream_list_lock`!")
            return True
        except KeyError:
            return False

    def _stream_is_stopping(self, stream_id: str = None) -> bool:
        """
        Streams report with this call their shutdowns

        :param stream_id: id of a stream
        :type stream_id: str
        :return: bool
        """
        logger.info(f"BybitWebSocketApiManager._stream_is_stopping({stream_id}){self.get_debug_log()}")
        try:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager._stream_is_stopping() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['has_stopped'] = time.time()
                self.stream_list[stream_id]['status'] = "stopped"
                logger.debug(f"BybitWebSocketApiManager._stream_is_stopping() - Leaving `stream_list_lock`!")
        except KeyError:
            pass
        self.send_stream_signal(stream_id=stream_id, signal_type="STOP")
        return True

    def subscribe_to_stream(self, stream_id: str = None, channels=None, markets=None) -> bool:
        """
        Subscribe channels and/or markets to an existing stream

        If you provide one channel and one market, then every subscribed market is going to get added to the new channel
        and all subscribed channels are going to get added to the new market!

        `How are the parameter `channels` and `markets` used with
        `subscriptions <https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.create_stream>`__

        :param stream_id: id of a stream
        :type stream_id: str
        :param channels: provide the channels you wish to subscribe
        :type channels: str, list, set
        :param markets: provide the markets you wish to subscribe
        :type markets: str, list, set
        :return: bool
        """
        logger.info(f"BybitWebSocketApiManager.subscribe_to_stream(" + str(stream_id) + ", " + str(channels) +
                    f", " + str(markets) + f"){self.get_debug_log()} - started ... -")
        if stream_id is None:
            logger.critical(f"BybitWebSocketApiManager.subscribe_to_stream() - error_msg: `stream_id` is missing!")
            return False
        if channels is None:
            channels = []
        else:
            if type(channels) is str:
                channels = [channels]
            if type(channels) is set:
                channels = list(channels)
        if markets is None:
            markets = []
        else:
            if type(markets) is str:
                markets = [markets]
            if type(markets) is set:
                markets = list(markets)
        if type(self.stream_list[stream_id]['channels']) is str:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['channels'] = [self.stream_list[stream_id]['channels']]
                logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - Leaving `stream_list_lock`!")
        if type(self.stream_list[stream_id]['channels']) is set:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['channels'] = list(self.stream_list[stream_id]['channels'])
                logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - Leaving `stream_list_lock`!")
        if type(self.stream_list[stream_id]['markets']) is str:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['markets'] = [self.stream_list[stream_id]['markets']]
                logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - Leaving `stream_list_lock`!")
        if type(self.stream_list[stream_id]['markets']) is set:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['markets'] = list(self.stream_list[stream_id]['markets'])
                logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - Leaving `stream_list_lock`!")
        with self.stream_list_lock:
            logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - `stream_list_lock` was entered!")
            self.stream_list[stream_id]['channels'] = list(set(self.stream_list[stream_id]['channels'] + channels))
            logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - Leaving `stream_list_lock`!")
        markets_new = []
        for market in markets:
            markets_new.append(str(market).upper())
        with self.stream_list_lock:
            logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - `stream_list_lock` was entered!")
            self.stream_list[stream_id]['markets'] = list(set(self.stream_list[stream_id]['markets'] + markets_new))
            logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - Leaving `stream_list_lock`!")
        payload = self.create_payload(stream_id, "subscribe",
                                      channels=self.stream_list[stream_id]['channels'],
                                      markets=self.stream_list[stream_id]['markets'])
        subscriptions = self.get_number_of_subscriptions(stream_id)
        with self.stream_list_lock:
            logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - `stream_list_lock` was entered!")
            self.stream_list[stream_id]['subscriptions'] = subscriptions
            logger.debug(f"BybitWebSocketApiManager.subscribe_to_stream() - Leaving `stream_list_lock`!")
        # Todo: control subscription limit!
        if payload is None:
            logger.error(f"BybitWebSocketApiManager.subscribe_to_stream({str(stream_id)}) - error_msg: Payload is "
                         f"None!")
            return False
        try:
            for item in payload:
                if self.send_with_stream(stream_id=stream_id, payload=item) is False:
                    self.add_payload_to_stream(stream_id=stream_id, payload=item)
            logger.info(f"BybitWebSocketApiManager.subscribe_to_stream({str(stream_id)}, {str(channels)}, "
                        f"{str(markets)}) finished ...")
            return True
        except TypeError as error_msg:
            logger.error(f"BybitWebSocketApiManager.subscribe_to_stream({str(stream_id)}) - TypeError - "
                         f"{str(error_msg)}")
            return False

    def unsubscribe_from_stream(self, stream_id: str = None, channels=None, markets=None) -> bool:
        """
        Unsubscribe channels and/or markets to an existing stream

        If you provide one channel and one market, then all subscribed markets from the specific channel and all
        subscribed channels from the specific markets are going to be removed!

        `How are the parameter `channels` and `markets` used with
        `subscriptions <https://unicorn-bybit-websocket-api.docs.lucit.tech/unicorn_bybit_websocket_api.html#unicorn_bybit_websocket_api.manager.BybitWebSocketApiManager.create_stream>`__

        :param stream_id: id of a stream
        :type stream_id: str
        :param channels: provide the channels you wish to unsubscribe
        :type channels: str, list, set
        :param markets: provide the markets you wish to unsubscribe
        :type markets: str, list, set
        :return: bool
        """
        logger.info(f"BybitWebSocketApiManager.unsubscribe_from_stream(" + str(stream_id) + ", " + str(channels) +
                    f", " + str(markets) + f"){self.get_debug_log()} - started ... -")
        if stream_id is None:
            logger.critical(f"BybitWebSocketApiManager.unsubscribe_from_stream() - error_msg: `stream_id` is missing!")
            return False
        if markets is None:
            markets = []
        if channels is None:
            channels = []
        if type(channels) is str:
            channels = [channels]
        if type(markets) is str:
            markets = [markets]
        if type(self.stream_list[stream_id]['channels']) is str:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.unsubscribe_from_stream() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['channels'] = [self.stream_list[stream_id]['channels']]
                logger.debug(f"BybitWebSocketApiManager.unsubscribe_from_stream() - Leaving `stream_list_lock`!")
        if type(self.stream_list[stream_id]['markets']) is str:
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.unsubscribe_from_stream() - `stream_list_lock` was entered!")
                self.stream_list[stream_id]['markets'] = [self.stream_list[stream_id]['markets']]
                logger.debug(f"BybitWebSocketApiManager.unsubscribe_from_stream() - Leaving `stream_list_lock`!")
        for channel in channels:
            try:
                with self.stream_list_lock:
                    logger.debug(f"BybitWebSocketApiManager.unsubscribe_from_stream() - `stream_list_lock` was "
                                 f"entered!")
                    self.stream_list[stream_id]['channels'].remove(channel)
                    logger.debug(f"BybitWebSocketApiManager.unsubscribe_from_stream() - Leaving `stream_list_lock`!")
            except ValueError:
                pass
        for i in range(len(markets)):
            markets[i] = markets[i].lower()
        for market in markets:
            if re.match(r'[a-zA-Z0-9]{41,43}', market) is None:
                try:
                    with self.stream_list_lock:
                        logger.debug(f"BybitWebSocketApiManager.unsubscribe_from_stream() - `stream_list_lock` was "
                                     f"entered!")
                        self.stream_list[stream_id]['markets'].remove(market)
                        logger.debug(f"BybitWebSocketApiManager.unsubscribe_from_stream() - Leaving "
                                     f"`stream_list_lock`!")
                except ValueError:
                    pass
        payload = self.create_payload(stream_id, "unsubscribe", channels=channels, markets=markets)
        if payload is None:
            logger.error(f"BybitWebSocketApiManager.unsubscribe_from_stream({str(stream_id)}) - error_msg: Payload "
                         f"is None!")
            return False
        try:
            for item in payload:
                if self.send_with_stream(stream_id=stream_id, payload=item) is False:
                    self.add_payload_to_stream(stream_id=stream_id, payload=item)
            subscriptions = self.get_number_of_subscriptions(stream_id)
            with self.stream_list_lock:
                logger.debug(f"BybitWebSocketApiManager.unsubscribe_from_stream() - `stream_list_lock` was "
                             f"entered!")
                self.stream_list[stream_id]['subscriptions'] = subscriptions
                logger.debug(f"BybitWebSocketApiManager.unsubscribe_from_stream() - Leaving `stream_list_lock`!")
            logger.info(f"BybitWebSocketApiManager.unsubscribe_from_stream({str(stream_id)}, {str(channels)}, "
                        f"{str(markets)}) finished ...")
        except TypeError as error_msg:
            logger.error(f"BybitWebSocketApiManager.unsubscribe_from_stream({str(stream_id)}) - TypeError - "
                         f"{str(error_msg)}")
            return False
        return True

    def wait_till_stream_has_started(self, stream_id, timeout: float = 0.0) -> bool:
        """
        Returns `True` as soon a specific stream has started and received its first stream data

        :param stream_id: id of a stream
        :type stream_id: str
        :param timeout: The timeout for how long to wait for the stream to stop. The function aborts if the waiting
                        time is exceeded and returns False.
        :type timeout: float

        :return: bool
        """
        timestamp = self.get_timestamp_unix()
        timeout = timestamp + timeout if timeout != 0.0 else timeout
        logger.debug(f"BybitWebSocketApiManager.wait_till_stream_has_started({stream_id}) with timeout {timeout} "
                     f"started!")
        try:
            while self.stream_list[stream_id]['last_heartbeat'] is None:
                if self.get_timestamp_unix() > timeout != 0.0:
                    logger.debug(
                        f"BybitWebSocketApiManager.wait_till_stream_has_started({stream_id}) finished with `False`!")
                    return False
                time.sleep(0.1)
            logger.debug(f"BybitWebSocketApiManager.wait_till_stream_has_started({stream_id}) finished with `True`!")
            return True
        except KeyError:
            logger.debug(f"BybitWebSocketApiManager.wait_till_stream_has_started({stream_id}) finished with `False`!")
            return False

    def wait_till_stream_has_stopped(self, stream_id: str = None, timeout: float = 0.0) -> bool:
        """
        Returns `True` as soon a specific stream has stopped itself

        :param stream_id: id of a stream
        :type stream_id: str
        :param timeout: The timeout for how long to wait for the stream to stop. The function aborts if the waiting
                        time is exceeded and returns False.
        :type timeout: float
        :return: bool
        """
        if stream_id is None:
            logger.debug(f"BybitWebSocketApiManager.wait_till_stream_has_stopped() - `stream_id` is mandatory!")
            return False

        timestamp = self.get_timestamp_unix()
        timeout = timestamp + timeout if timeout != 0.0 else timeout
        logger.debug(f"BybitWebSocketApiManager.wait_till_stream_has_stopped({stream_id}) with timeout {timeout} "
                     f"started!")
        try:
            while self.stream_list[stream_id]['status'] != "stopped" \
                    and not self.stream_list[stream_id]['status'].startswith("crashed"):
                if self.get_timestamp_unix() > timeout != 0.0:
                    logger.debug(
                        f"BybitWebSocketApiManager.wait_till_stream_has_stopped({stream_id}) finished with `False`!")
                    return False
                time.sleep(0.1)
        except KeyError:
            logger.debug(f"BybitWebSocketApiManager.wait_till_stream_has_stopped({stream_id}) finished with `False`!")
            return False
        logger.debug(f"BybitWebSocketApiManager.wait_till_stream_has_stopped({stream_id}) finished with `True`!")
        return True
