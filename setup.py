#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: setup.py
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

from Cython.Build import cythonize
from setuptools import setup, find_packages, Extension
import os
import shutil
import subprocess

name = "unicorn-bybit-websocket-api"
source_dir = "unicorn_bybit_websocket_api"

stubs_dir = "stubs"
extensions = [
    Extension("*", [f"{source_dir}/*.py"]),
]

# Setup
print("Generating stub files ...")
os.makedirs(stubs_dir, exist_ok=True)
for filename in os.listdir(source_dir):
    if filename.endswith('.py'):
        source_path = os.path.join(source_dir, filename)
        subprocess.run(['stubgen', '-o', stubs_dir, source_path], check=True)
for stub_file in os.listdir(os.path.join(stubs_dir, source_dir)):
    if stub_file.endswith('.pyi'):
        source_stub_path = os.path.join(stubs_dir, source_dir, stub_file)
        if os.path.exists(os.path.join(source_dir, stub_file)):
            print(f"Skipped moving {source_stub_path} because {os.path.join(source_dir, stub_file)} already exists!")
        else:
            shutil.move(source_stub_path, source_dir)
            print(f"Moved {source_stub_path} to {source_dir}!")
shutil.rmtree(os.path.join(stubs_dir))
print("Stub files generated and moved successfully.")

with open("README.md", "r") as fh:
    print("Using README.md content as `long_description` ...")
    long_description = fh.read()

setup(
    name=name,
    version="0.1.0",
    author="LUCIT Systems and Development",
    author_email='info@lucit.tech',
    url="https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api",
    description="A Python SDK by LUCIT to use the Bybit Websocket API`s in a simple, fast, flexible, robust and "
                "fully-featured way.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='LSOSL - LUCIT Synergetic Open Source License',
    install_requires=['colorama', 'requests>=2.31.0', 'websocket-client', 'websockets==11.0.3', 'flask_restful',
                      'cheroot', 'flask', 'lucit-licensing-python>=1.8.2', 'ujson', 'psutil', 'PySocks',
                      'typing_extensions', 'Cython'],
    keywords='bybit, asyncio, async, asynchronous, concurrent, websocket-api, webstream-api, '
             'bybit-websocket, bybit-webstream, webstream, websocket, api',
    project_urls={
        'Howto': 'https://www.lucit.tech/unicorn-bybit-websocket-api.html#howto',
        'Documentation': 'https://unicorn-bybit-websocket-api.docs.lucit.tech',
        'Wiki': 'https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/wiki',
        'Author': 'https://www.lucit.tech',
        'Changes': 'https://unicorn-bybit-websocket-api.docs.lucit.tech/changelog.html',
        'License': 'https://unicorn-bybit-websocket-api.docs.lucit.tech/license.html',
        'Issue Tracker': 'https://github.com/LUCIT-Systems-and-Development/unicorn-bybit-websocket-api/issues',
        'Chat': 'https://gitter.im/unicorn-trading-suite/unicorn-bybit-websocket-api',
        'Telegram': 'https://t.me/unicorndevs',
        'Get Support': 'https://www.lucit.tech/get-support.html',
        'LUCIT Online Shop': 'https://shop.lucit.services/software',
    },
    packages=find_packages(exclude=[f"dev/{source_dir}"], include=[source_dir]),
    ext_modules=cythonize(extensions, compiler_directives={'language_level': "3"}),
    python_requires='>=3.7.0',
    package_data={'': ['*.so', '*.dll', '*.py', '*.pyd', '*.pyi']},
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Cython",
        "License :: Other/Proprietary License",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Framework :: AsyncIO",
    ],
)
