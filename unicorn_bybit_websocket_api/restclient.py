#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯
#
# File: unicorn_bybit_websocket_api/restclient.py
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

from typing import Optional
import logging
import requests
import threading


__logger__: logging.getLogger = logging.getLogger("unicorn_bybit_websocket_api")

logger = __logger__


class BybitWebSocketApiRestclient(object):
    def __init__(self,
                 debug: Optional[bool] = False,
                 disable_colorama: Optional[bool] = False,
                 exchange: Optional[str] = "bybit.com",
                 lucit_api_secret: Optional[str] = None,
                 lucit_license_ini: str = None,
                 lucit_license_profile: Optional[str] = None,
                 lucit_license_token: Optional[str] = None,
                 restful_base_uri: Optional[str] = None,
                 show_secrets_in_logs: Optional[bool] = False,
                 socks5_proxy_server: Optional[str] = None,
                 socks5_proxy_user: Optional[str] = None,
                 socks5_proxy_pass: Optional[str] = None,
                 socks5_proxy_ssl_verification: Optional[bool] = True,
                 stream_list: dict = None,
                 warn_on_update: Optional[bool] = True):
        """
        Create a restclient instance!

        """
        self.threading_lock = threading.Lock()
        self.debug = debug
        self.disable_colorama = disable_colorama
        self.exchange = exchange
        self.lucit_api_secret: Optional[str] = lucit_api_secret
        self.lucit_license_ini = lucit_license_ini
        self.lucit_license_profile = lucit_license_profile
        self.lucit_license_token = lucit_license_token
        self.restful_base_uri = restful_base_uri
        self.show_secrets_in_logs = show_secrets_in_logs
        self.socks5_proxy_server = socks5_proxy_server
        self.socks5_proxy_user = socks5_proxy_user
        self.socks5_proxy_pass = socks5_proxy_pass
        self.socks5_proxy_ssl_verification = socks5_proxy_ssl_verification
        self.stream_list = stream_list
        self.warn_on_update = warn_on_update
        self.sigterm = False

    def get_symbols(self):
        response = requests.get("https://api.bybit.com/v2/public/symbols")
        data = response.json()
        return data
