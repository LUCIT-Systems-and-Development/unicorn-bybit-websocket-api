#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯

from unicorn_bybit_websocket_api import BybitWebSocketApiManager
import aiosqlite
import asyncio
import logging
import os
import re

exchange = "bybit.com"
sqlite_db_file = '../../bybit_spot_ohlcv_1m.db'

logging.getLogger("unicorn_bybit_websocket_api")
logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")


class BybitDataProcessor:
    def __init__(self):
        self.db = None
        self.ubbwa = BybitWebSocketApiManager(exchange=exchange,
                                              enable_stream_signal_buffer=True,
                                              process_stream_signals=self.receive_stream_signal,
                                              output_default="dict")

    async def main(self):
        self.db = AsyncDatabase(sqlite_db_file)
        await self.db.create_connection()
        await self.db.create_table()
        markets = ['ethbtc', 'btcusdt', 'ethusdt', 'xrpbtc', 'solbtc']
        self.ubbwa.create_stream(channels="kline.1",
                                 endpoint="public/spot",
                                 markets=markets,
                                 process_asyncio_queue=self.process_ohlcv_datasets,
                                 stream_label="OHLCV")
        while self.ubbwa.is_manager_stopping() is False:
            await asyncio.sleep(1)
            saved_datasets = await self.db.count_ohlcv_records()
            self.ubbwa.print_summary(add_string=f"saved datasets: {saved_datasets}")

    async def process_ohlcv_datasets(self, stream_id=None):
        print(f"Saving the data from webstream {self.ubbwa.get_stream_label(stream_id=stream_id)} to the database ...")
        while self.ubbwa.is_stop_request(stream_id=stream_id) is False:
            kline = await self.ubbwa.get_stream_data_from_asyncio_queue(stream_id)
            ohlcv = {}
            try:
                ohlcv = kline['data'][0]
            except KeyError:
                pass
            if ohlcv.get('confirm') is True:
                await self.db.insert_ohlcv_data(kline)
            self.ubbwa.asyncio_queue_task_done(stream_id)

    def receive_stream_signal(self, signal_type=None, stream_id=None, data_record=None, error_msg=None):
        print(f"Received stream_signal for stream '{self.ubbwa.get_stream_label(stream_id=stream_id)}': "
              f"{signal_type} - {stream_id} - {data_record} - {error_msg}")


class AsyncDatabase:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = None

    async def close(self):
        await self.conn.close()

    async def count_ohlcv_records(self):
        async with self.conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM ohlcv")
            result = await cur.fetchone()
            return result[0] if result else 0

    async def create_connection(self):
        self.conn = await aiosqlite.connect(self.db_file)

    async def create_table(self):
        try:
            await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS ohlcv (
                    date TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL
                )
            """)
            await self.conn.commit()
        except Exception as error_msg:
            print(f"Not able to create SQLite table: {error_msg}")

    async def insert_ohlcv_data(self, data):
        try:
            ohlcv = data['data'].pop()
            channel, symbol = self.split_string(data['topic'])
        except KeyError as error_msg:
            print(f"ERROR: {error_msg} - Can not save to OHLCV DB: {data}")
            return None

        sql = '''
        INSERT INTO ohlcv(date, channel, symbol, open, high, low, close, volume)
        VALUES(?,?,?,?,?,?,?,?)
        '''
        ohlcv_data = (ohlcv['start'],
                      channel,
                      symbol,
                      float(ohlcv['open']),
                      float(ohlcv['high']),
                      float(ohlcv['low']),
                      float(ohlcv['close']),
                      float(ohlcv['volume']))
        async with self.conn.cursor() as cur:
            await cur.execute(sql, ohlcv_data)
        await self.conn.commit()
        return cur.lastrowid

    @staticmethod
    def split_string(input_string):
        pattern = r'^(.*\..*)\.([^.]*)$'
        match = re.match(pattern, input_string)
        if match:
            value_1 = match.group(1)
            value_2 = match.group(2)
            return value_1, value_2
        else:
            return None, None


if __name__ == "__main__":
    bdp = BybitDataProcessor()
    try:
        asyncio.run(bdp.main())
    except KeyboardInterrupt:
        print("\r\nGracefully stopping ...")
        bdp.ubbwa.stop_manager()
        asyncio.run(bdp.db.close())
    except Exception as e:
        print(f"\r\nError: {e}")
        print("Gracefully stopping ...")
        bdp.ubbwa.stop_manager()
        asyncio.run(bdp.db.close())
