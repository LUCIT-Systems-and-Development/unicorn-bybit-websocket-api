#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: unicorn_bybit_websocket_api/connection_settings.py
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

from enum import Enum

import sys
if sys.version_info >= (3, 9):
    from typing import Type
    WEBSOCKET_BASE_URI: Type[str] = str
    VERSION: Type[str] = str
    ARGS_LIMIT: Type[int] = int
    MAX_SUBSCRIPTIONS_PER_STREAM_SPOT: Type[int] = int
    MAX_SUBSCRIPTIONS_PER_STREAM_LINEAR: Type[int] = int
    MAX_SUBSCRIPTIONS_PER_STREAM_INVERSE: Type[int] = int
    MAX_SUBSCRIPTIONS_PER_STREAM_OPTION: Type[int] = int
else:
    WEBSOCKET_BASE_URI = str
    VERSION = str
    ARGS_LIMIT = int
    MAX_SUBSCRIPTIONS_PER_STREAM_SPOT = int
    MAX_SUBSCRIPTIONS_PER_STREAM_LINEAR = int
    MAX_SUBSCRIPTIONS_PER_STREAM_INVERSE = int
    MAX_SUBSCRIPTIONS_PER_STREAM_OPTION = int


class Exchanges(str, Enum):
    BYBIT = "bybit.com"
    BYBIT_TESTNET = "bybit.com-testnet"

# only python 3.9+
# CONNECTION_SETTINGS: dict[str, Tuple[WEBSOCKET_BASE_URI, VERSION, ARGS_LIMIT, MAX_SUBSCRIPTIONS_PER_STREAM_SPOT,
#   MAX_SUBSCRIPTIONS_PER_STREAM_LINEAR, MAX_SUBSCRIPTIONS_PER_STREAM_INVERSE, MAX_SUBSCRIPTIONS_PER_STREAM_OPTION]]
# ARGS Limits: https://bybit-exchange.github.io/docs/v5/ws/connect#public-channel---args-limits

CONNECTION_SETTINGS = {
    Exchanges.BYBIT: ("wss://stream.bybit.com", "v5", 21000, 10, 2000, 0, 0),
    Exchanges.BYBIT_TESTNET: ("wss://stream-testnet.bybit.com", "v5", 21000, 10, 2000, 0, 0),
}
