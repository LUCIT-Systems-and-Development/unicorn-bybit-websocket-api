#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: unittest_bybit_websocket_api.py
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

from unicorn_bybit_websocket_api.manager import BybitWebSocketApiManager
from unicorn_bybit_websocket_api.exceptions import *
from unicorn_bybit_websocket_api.restclient import BybitWebSocketApiRestclient
from unicorn_bybit_websocket_api.licensing_manager import LucitLicensingManager, NoValidatedLucitLicense
import asyncio
import logging
import unittest
import os
import platform
import time
import threading

import tracemalloc
tracemalloc.start(25)

BYBIT_COM_API_KEY = ""
BYBIT_COM_API_SECRET = ""

BYBIT_COM_TESTNET_API_KEY = os.getenv('BYBIT_TESTNET_API_KEY')
BYBIT_COM_TESTNET_API_SECRET = os.getenv('BYBIT_TESTNET_API_SECRET')

logging.getLogger("unicorn_bybit_websocket_api")
logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")

print(f"Starting unittests!")


async def processing_of_new_data_async(data):
    print(f"`processing_of_new_data_async()` test - Received: {data}")
    await asyncio.sleep(0.001)
    print("AsyncIO Check done!")


def handle_socket_message(data):
    print(f"Received ws api data:\r\n{data}\r\n")


def processing_of_new_data(data):
    print(f"`processing_of_new_data()` test - Received: {data}")


def is_github_action_env():
    try:
        print(f"{os.environ[f'LUCIT_LICENSE_TOKEN']}")
        return True
    except KeyError:
        return False


class TestBybitComManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"\r\nTestBybitComManager:")
        cls.ubwa = BybitWebSocketApiManager(exchange="bybit.com",
                                              disable_colorama=True,
                                              debug=True)
        cls.bybit_com_api_key = ""
        cls.bybit_com_api_secret = ""

    @classmethod
    def tearDownClass(cls):
        cls.ubwa.stop_monitoring_api()
        cls.ubwa.stop_manager()
        print(f"\r\nTestBybitComManager threads:")
        for thread in threading.enumerate():
            print(thread.name)
        print(f"TestBybitComManager stopping:")

    def test_is_update_available(self):
        print(f"test_is_update_available():")
        result = self.__class__.ubwa.is_update_available()
        is_valid_result = result is True or result is False
        self.assertTrue(is_valid_result, False)

    def test_is_manager_stopping(self):
        print(f"test_is_manager_stopping():")
        self.assertEqual(self.__class__.ubwa.is_manager_stopping(), False)

    def test_get_human_uptime(self):
        print(f"test_get_human_uptime():")
        self.assertEqual(self.__class__.ubwa.get_human_uptime(60 * 60 * 60 * 61), "152d:12h:0m:0s")
        self.assertEqual(self.__class__.ubwa.get_human_uptime(60 * 60 * 24), "24h:0m:0s")
        self.assertEqual(self.__class__.ubwa.get_human_uptime(60 * 60), "60m:0s")
        self.assertEqual(self.__class__.ubwa.get_human_uptime(60), "60 seconds")

    def test_get_human_bytesize(self):
        print(f"test_get_human_bytesize():")
        self.assertEqual(self.__class__.ubwa.get_human_bytesize(1024 * 1024 * 1024 * 1024 * 1024), "1024.0 tB")
        self.assertEqual(self.__class__.ubwa.get_human_bytesize(1024 * 1024 * 1024 * 1024), "1024.0 gB")
        self.assertEqual(self.__class__.ubwa.get_human_bytesize(1024 * 1024 * 1024), "1024.0 mB")
        self.assertEqual(self.__class__.ubwa.get_human_bytesize(1024 * 1024), "1024.0 kB")
        self.assertEqual(self.__class__.ubwa.get_human_bytesize(1024), "1024 B")
        self.assertEqual(self.__class__.ubwa.get_human_bytesize(1), "1 B")

    def test_fill_up_space_centered(self):
        print(f"test_fill_up_space_centered():")
        result = "==========test text=========="
        self.assertEqual(str(self.__class__.ubwa.fill_up_space_centered(30, "test text", "=")),
                         result)

    def test_fill_up_space_right(self):
        print(f"test_fill_up_space_right():")
        result = "|test text||||||||||||||||||||"
        self.assertEqual(str(self.__class__.ubwa.fill_up_space_right(30, "test text", "|")),
                         result)

    def test_fill_up_space_left(self):
        print(f"test_fill_up_space_left():")
        result = "||||||||||||||||||||test text|"
        self.assertEqual(str(self.__class__.ubwa.fill_up_space_left(30, "test text", "|")),
                         result)

class TestBybitComManagerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"\r\nTestBybitComManagerTest:")
        cls.ubwa = BybitWebSocketApiManager(exchange="bybit.com-testnet",
                                              debug=True)
        cls.bybit_com_testnet_api_key = BYBIT_COM_TESTNET_API_KEY
        cls.bybit_com_testnet_api_secret = BYBIT_COM_TESTNET_API_SECRET

    @classmethod
    def tearDownClass(cls):
        cls.ubwa.stop_manager()
        print(f"\r\nTestBybitComManagerTest threads:")
        for thread in threading.enumerate():
            print(thread.name)
        print(f"TestBybitComManagerTest stopping:")

    def test_z_stop_manager(self):
        time.sleep(6)
        self.__class__.ubwa.stop_manager()

    class TestWSApiLive(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            print(f"\r\nTestWSApiLive:")

        @classmethod
        def tearDownClass(cls):
            print(f"\r\nTestWSApiLive threads:")
            for thread in threading.enumerate():
                print(thread.name)
            print(f"TestApiLive stopping:")

    def test_lucitlicmgr(self):
        print(f"License Manager ...")
        ubwam = BybitWebSocketApiManager(exchange='bybit.com')
        ubwam.llm.get_info()
        ubwam.llm.get_module_version()
        ubwam.llm.get_quotas()
        ubwam.llm.get_timestamp()
        ubwam.llm.get_version()
        ubwam.llm.is_verified()
        ubwam.llm.sync_time()
        ubwam.llm.test()
        ubwam.llm.process_licensing_error()
        ubwam.llm.stop()
        with self.assertRaises(NoValidatedLucitLicense):
            llm = LucitLicensingManager(api_secret="wrong", license_token="credentials",
                                        parent_shutdown_function=ubwam.stop_manager)
            time.sleep(3)
            llm.stop()


class TestApiLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"\r\nTestApiLive:")
        cls.ubwa = BybitWebSocketApiManager(exchange="bybit.com",
                                              debug=True,
                                              enable_stream_signal_buffer=True,
                                              auto_data_cleanup_stopped_streams=True)
        cls.count_receives = 0

    @classmethod
    def tearDownClass(cls):
        cls.ubwa.stop_manager()
        print(f"\r\nTestApiLive threads:")
        for thread in threading.enumerate():
            print(thread.name)
        print(f"TestApiLive stopping:")

    def test_get_new_uuid_id(self):
        self.__class__.ubwa.get_new_uuid_id()

    def test_z_invalid_exchange(self):
        with self.assertRaises(UnknownExchange):
            ubwa_error = BybitWebSocketApiManager(exchange="invalid-exchange.com")
            ubwa_error.stop_manager()

    def test_live_receives_stream_specific_with_stream_buffer(self):
        print(f"Test receiving with stream specific stream_buffer ...")
        stream_id = self.__class__.ubwa.create_stream(endpoint="public/linear", channels=['kline.1'],
                                                      markets=['btcusdt'])
        count_receives = 0
        while count_receives < 5:
            # Todo: specific (buffer=True) did not work in tests
            received = self.__class__.ubwa.pop_stream_data_from_stream_buffer()
            if received:
                print(f"Received: {received}")
                count_receives += 1
        self.assertEqual(count_receives, 5)

    def test_live_receives_asyncio_queue(self):
        async def process_asyncio_queue(stream_id=None):
            print(f"Start processing data of {stream_id} from asyncio_queue...")
            self.count_receives = 0
            while self.count_receives < 5:
                data = await self.__class__.ubwa.get_stream_data_from_asyncio_queue(stream_id)
                print(f"Received async: {data}")
                self.count_receives += 1
                self.__class__.ubwa.asyncio_queue_task_done(stream_id)
            print(f"Closing asyncio_queue consumer!")

        print(f"Test receiving with stream specific asyncio_queue ...")
        stream_id_1 = self.__class__.ubwa.create_stream(endpoint="public/linear", channels=['kline.1'],
                                                        markets=['btcusdt', 'ethusdt'],
                                                        process_asyncio_queue=process_asyncio_queue)
        while self.count_receives < 5:
            time.sleep(1)
        self.assertEqual(self.count_receives, 5)
        time.sleep(3)
        self.__class__.ubwa.stop_stream(stream_id=stream_id_1)

    def z_test_live(self):
        print(f"Live test ...")
        stream_id = self.__class__.ubwa.create_stream(endpoint="public/linear", channels=['kline.1'],
                                                      markets=['btcusdt'])
        self.__class__.ubwa.get_stream_info(stream_id=stream_id)
        self.__class__.ubwa.get_stream_statistic(stream_id=stream_id)
        self.__class__.ubwa.get_total_receives()
        self.__class__.ubwa.get_total_received_bytes()
        self.__class__.ubwa.increase_reconnect_counter(stream_id=stream_id)
        self.__class__.ubwa.is_update_available_check_command()
        self.__class__.ubwa.print_summary()
        self.__class__.ubwa.print_stream_info(stream_id=stream_id)
        self.__class__.ubwa.pop_stream_signal_from_stream_signal_buffer()


if __name__ == '__main__':
    try:
        unittest.main()
    except KeyboardInterrupt:
        pass
