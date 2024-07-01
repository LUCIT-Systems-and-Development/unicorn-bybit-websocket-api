#!/usr/bin/env bash
# -*- coding: utf-8 -*-
#
# File: sphinx/install_sphinx.sh
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
set -xeuo pipefail
python3 -m pip install sphinx --upgrade
python3 -m pip install python-docs-theme-lucit --upgrade
python3 -m pip install rich --upgrade
python3 -m pip install myst-parser --upgrade
python3 -m pip install sphinx-markdown-tables --upgrade