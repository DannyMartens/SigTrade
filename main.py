# id: 1FBNDCT9pSzuZ-eM-TemIL-j
# secret: UkzgGZCpwTVVVxXSJIfIOB8G4NcZjPUBx9BqngY0K6e_bJEC
# --> Test Net
"""
CREATE TABLE `sig_trades`.`tick` (
  `timestamp` DOUBLE NOT NULL,
  `side` VARCHAR(45) NOT NULL,
  `size` INT NULL,
  `price` FLOAT NULL,
  `tickDirection` VARCHAR(45) NULL,
  PRIMARY KEY (`timestamp`));
"""
import websockets
from collections import OrderedDict
import asyncio
import json
import datetime
import mysql.connector

cnx = mysql.connector.connect(user='root', password="",
                              host='127.0.0.1',
                              database='sig_trades')
cursor = cnx.cursor()
results = OrderedDict()

def sig_trade_check(trades):
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
        temp_results = {}
        results_key = to_mili_key(k.split('Z', 1)[0])
        if sum_price_arr[k]['size_sum'] >= 75000 and results_key not in results:
            # temp_results[results_key] = {}
            # results[results_key]['price'] = round(sum_price_arr[k]['total_price'] * 2)/2
            # results[results_key]['size'] = sum_price_arr[k]['size_sum']
            # results[results_key]['side'] = sum_price_arr[k]['side']
            # results[results_key]['timestamp'] = sum_price_arr[k]['timestamp']
            temp_results = {
                'timestamp': results_key,
                'price': round(sum_price_arr[k]['total_price'] * 2)/2,
                'size': sum_price_arr[k]['size_sum'],
                'side': sum_price_arr[k]['side']
            }
            placeholder = ", ".join(["%s"] * len(temp_results))
            stmt = "insert into `{table}` ({columns}) values ({values});".format(table='aggr',
                                                                                 columns=",".join(temp_results),
                                                                                 values=placeholder)
            cursor.execute(stmt, list(temp_results.values()))


def tick_to_sql(trades):
    for trade in trades:
        # put timestamp, side, size, price, tickDirection, grossValue
        rem_list = ['symbol', 'trdMatchID', 'homeNotional', 'foreignNotional', 'grossValue']
        [trade.pop(key) for key in rem_list]
        trade['timestamp'] = to_mili(trade['timestamp'])
        placeholder = ", ".join(["%s"] * len(trade))
        stmt = "insert into `{table}` ({columns}) values ({values});".format(table='tick',
                                                                             columns=",".join(trade),
                                                                             values=placeholder)
        cursor.execute(stmt, list(trade.values()))

#  Reading For Sig Trades
async def read():

    uri = "wss://www.bitmex.com/realtime?subscribe=trade:XBTUSD"
    async with websockets.connect(uri) as websocket:
        while True:
            response = await websocket.recv()
            json1_data = json.loads(response)
            if 'data' in json1_data:
                sig_trade_check(json1_data['data'])
                tick_to_sql(json1_data['data'])
                cnx.commit()


def to_mili(timestamp):
    dt_obj = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    return dt_obj.timestamp() * 1000


def to_mili_key(timestamp):
    dt_obj = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
    return dt_obj.timestamp() * 1000


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(read())




