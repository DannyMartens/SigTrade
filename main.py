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
import sys
import pandas as pd
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

    ### After Each New Message Parse see if there is a long and short opportunity. ###
    long_logic(rolling_avg_time=int(sys.argv[1]), trades_in_db=int(sys.argv[2]),
               indv_trade_count=int(sys.argv[3]), indv_trade_threshold=int(sys.argv[4]), exposure_threshold_long=int(sys.argv[5]))
    short_logic(rolling_avg_time=int(sys.argv[1]), trades_in_db=int(sys.argv[2]),
                indv_trade_count=int(sys.argv[3]), indv_trade_threshold=int(sys.argv[4]), exposure_threshold_short=int(sys.argv[6]))


def long_logic(rolling_avg_time, trades_in_db, indv_trade_count, indv_trade_threshold, exposure_threshold_long):
    positions = ws.positions()
    bid_price = ws.get_instrument()['bidPrice']
    if positions:
        positions = positions[0]['currentQty']
    else:
        positions = 0

    exposure, trade_count = rolling_total_exposure(rolling_avg_time)
    last_trades = last_n_trades(indv_trade_count)

    if positions > 0:
        if long_sell_logic(last_trades, exposure, trade_count):
            client.Order.Order_new(symbol='XBTUSD', orderQty=-positions, ordType='Market', execInst='Close').result()
            # TODO: switch to limit orders.
            time.sleep(2)
    elif positions == 0:
        if buy_long_logic(trades_in_db, exposure_threshold_long, last_trades,
                          indv_trade_threshold, trade_count, exposure):
            # TODO: Maybe use limit orders if can't find a profit
            # order_id = None
            # if not ws.open_orders(order_id):
                order_id = client.Order.Order_new(symbol='XBTUSD', orderQty=10, ordType='Limit',
                                                  price=bid_price).result()
                print(order_id)
            # else:
            #     order_id = client.Order.Order_new(symbol='XBTUSD', orderQty=10, ordType='Limit',
            #                                       price=bid_price).result()
                time.sleep(2)


def buy_long_logic(trades_in_db, exposure_threshold_long, last_trades, indv_trade_threshold, exposure, trade_count):
    if trade_count > trades_in_db:
        if exposure > exposure_threshold_long and last_trades_logic(last_trades, indv_trade_threshold, 'Buy', 5):
            client.Order.Order_new(symbol='XBTUSD', orderQty=10, ordType='Market').result()
            time.sleep(2)


def short_logic(rolling_avg_time, trades_in_db, indv_trade_count, indv_trade_threshold, exposure_threshold_short):
    positions = ws.positions()
    ask_price = ws.get_instrument()['askPrice']

    if positions:
        positions = positions[0]['currentQty']
    else:
        positions = 0
    exposure, trade_count = rolling_total_exposure(rolling_avg_time)
    last_trades = last_n_trades(indv_trade_count)
    if trade_count > trades_in_db:
        if exposure < exposure_threshold_short and last_trades_logic(last_trades, indv_trade_threshold, 'Sell', 5)\
                and positions == 0:
            order_id = client.Order.Order_new(symbol='XBTUSD', orderQty=-10, ordType='Limit', price=ask_price).result()
            print(order_id)

            time.sleep(2)
        elif positions < 0:
            if short_buy_logic(last_trades):
                client.Order.Order_new(symbol='XBTUSD', orderQty=-positions, ordType='Market', execInst='Close').result()
                time.sleep(2)


def long_sell_logic(last_trades, avg, trade_count):
    if (last_trades_logic(last_trades, int(sys.argv[7]), 'Sell', int(sys.argv[8]))) or (trade_count < 5) or (avg < 500000):
        return True
    else:
        return False


def short_buy_logic(last_trades):
    avg, trade_count = rolling_total_exposure(45000)
    if (last_trades_logic(last_trades, int(sys.argv[7]), 'Buy', int(sys.argv[8]))) or (trade_count < 5) or (avg > -500000):
        return True
    else:
        return False


def last_trades_logic(trades, time_threshold, side, count_threshold):
    trade_count = 0
    lower_time = datetime.datetime.utcnow().timestamp() * 1000 - time_threshold
    for trade in trades:
        if trades[trade]['side'] == side and trades[trade]['timestamp'] > lower_time:
            trade_count += 1
        else:
            pass
    if trade_count >= count_threshold:
        return True
    else:
        return False


def last_n_trades(n):
    rolling_results = results.copy()
    temp_results = OrderedDict()
    for x in list(reversed(list(rolling_results.values())))[0:n]:
        temp_results[x['timestamp']] = x
    return temp_results


def rolling_total_exposure(rolling_avg_time):  # 300000 = 5 min in mili sec
    lower_time = datetime.datetime.utcnow().timestamp()*1000 - rolling_avg_time
    trade_count = 0
    avg = 0
    rolling_results = results.copy()
    for timestamp in rolling_results:
        if lower_time <= timestamp:
            trade_count += 1
            if results[timestamp]['side'] == 'Buy':
                avg += results[timestamp]['size']
            else:
                avg -= results[timestamp]['size']
        else:
            del results[timestamp]
    return avg, trade_count


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




