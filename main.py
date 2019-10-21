# id: 1FBNDCT9pSzuZ-eM-TemIL-j
# secret: UkzgGZCpwTVVVxXSJIfIOB8G4NcZjPUBx9BqngY0K6e_bJEC
# --> Test Net
"""
Rolling_avg_time(mili) --> 1
Trades_in_db(min count) --> 2
indv_trade_count(last n trades) --> 3
indv_trade_thresh(time before last trade, mili) --> 4
exposure_threshold_long(Min amount of long exposure) --> 5
exposure_threshold_short(Min amount of sell exposure) --> 6
sell_last_trades_logic(low time threshold for known large trades) --> 7
sell_count_trades_logic(low count in low time threshold) --> 8
"""
import websockets
from collections import OrderedDict
import asyncio
import time
import json
import datetime
from bitmex_websocket import BitMEXWebsocket
import bitmex
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd=""
)

print(mydb)

results = OrderedDict()
ws = BitMEXWebsocket(endpoint="https://testnet.bitmex.com/api/v1", symbol="XBTUSD",
                     api_key='1FBNDCT9pSzuZ-eM-TemIL-j', api_secret='UkzgGZCpwTVVVxXSJIfIOB8G4NcZjPUBx9BqngY0K6e_bJEC')
client = bitmex.bitmex(api_key='1FBNDCT9pSzuZ-eM-TemIL-j',
                       api_secret='UkzgGZCpwTVVVxXSJIfIOB8G4NcZjPUBx9BqngY0K6e_bJEC')


def check(trades):
    sum_size_arr, sum_price_arr = dict(), dict()

    for trade in trades:
        trade_key = trade['timestamp'] + trade['side']
        if trade_key not in sum_size_arr:
            sum_size_arr[trade_key] = {}
            sum_size_arr[trade_key]['total_size'] = trade['size']
        else:
            sum_size_arr[trade_key]['total_size'] += trade['size']

    for trade in trades:
        trade_key = trade['timestamp'] + trade['side']
        if trade_key not in sum_size_arr:
            print("No Trade Key")
        else:
            size_item = sum_size_arr[trade_key]
        if trade_key not in sum_price_arr:
            sum_price_arr[trade_key] = {}
            sum_price_arr[trade_key]['symbol'] = trade['symbol']
            sum_price_arr[trade_key]['total_price'] = trade['price'] * trade['size'] / size_item['total_size']
            sum_price_arr[trade_key]['size_sum'] = trade['size']
            sum_price_arr[trade_key]['timestamp'] = to_mili(trade['timestamp'])
            sum_price_arr[trade_key]['side'] = trade['side']
        else:
            sum_price_arr[trade_key]['total_price'] += trade['price'] * trade['size'] / size_item['total_size']
            sum_price_arr[trade_key]['size_sum'] += trade['size']

    for k in sum_price_arr:
        results_key = to_mili_key(k.split('Z', 1)[0])
        if sum_price_arr[k]['size_sum'] >= 100000 and results_key not in results:
            # todo: change 100k to argv and have results.append be changed to a connection with sql or redis
            results[results_key] = {}
            results[results_key]['price'] = sum_price_arr[k]['total_price']
            results[results_key]['size'] = sum_price_arr[k]['size_sum']
            results[results_key]['side'] = sum_price_arr[k]['side']
            results[results_key]['timestamp'] = sum_price_arr[k]['timestamp']

async def read():
    uri = "wss://www.bitmex.com/realtime?subscribe=trade:XBTUSD"
    async with websockets.connect(uri) as websocket:
        while True:
            response = await websocket.recv()
            json1_data = json.loads(response)
            if 'data' in json1_data:
                check(json1_data['data'])


def to_mili(timestamp):
    dt_obj = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    return dt_obj.timestamp() * 1000


def to_mili_key(timestamp):
    dt_obj = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
    return dt_obj.timestamp() * 1000




if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(read())




