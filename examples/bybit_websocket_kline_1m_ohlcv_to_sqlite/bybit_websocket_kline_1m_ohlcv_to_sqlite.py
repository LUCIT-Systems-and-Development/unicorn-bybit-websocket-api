#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ¯\_(ツ)_/¯

from unicorn_bybit_websocket_api import BybitWebSocketApiManager
import aiosqlite
import asyncio
import logging
import os

exchange = "bybit.com"
sqlite_db_file = 'ohlcv.db'

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
                                              process_stream_signals=self.receive_stream_signal)

    async def main(self):
        self.db = AsyncDatabase(sqlite_db_file)
        await self.db.create_connection()
        await self.db.create_table()
        markets = []
        rest_data = self.ubbwa.restclient.get_symbols()
        for symbol in rest_data['result']:
            if symbol['quote_currency'] == 'USDT' and symbol['status'] == 'Trading':
                markets.append(symbol['name'])

        self.ubbwa.create_stream(channels="kline.1",
                                 endpoint="public/linear",
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
            if kline.get('event_type') == "kline":
                if kline['kline']['is_closed'] is True or kline['event_time'] >= kline['kline']['kline_close_time']:
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
        sql = '''
        INSERT INTO ohlcv(date, symbol, open, high, low, close, volume)
        VALUES(?,?,?,?,?,?,?)
        '''
        ohlcv_data = (data['kline']['kline_start_time'],
                      data['kline']['symbol'],
                      float(data['kline']['open_price']),
                      float(data['kline']['high_price']),
                      float(data['kline']['low_price']),
                      float(data['kline']['close_price']),
                      float(data['kline']['base_volume']))
        async with self.conn.cursor() as cur:
            await cur.execute(sql, ohlcv_data)
        await self.conn.commit()
        return cur.lastrowid


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
