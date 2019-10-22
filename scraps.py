import websockets
from collections import OrderedDict
import asyncio
import time
import json
import datetime
import mysql.connector
import sys



# async def read():
#     uri = "wss://www.bitmex.com/realtime?subscribe=trade:XBTUSD"
#     async with websockets.connect(uri) as websocket:
#         while True:
#             response = await websocket.recv()
#             json1_data = json.loads(response)
#             print(json1_data)
#
#
# def to_mili(timestamp):
#     dt_obj = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
#     return dt_obj.timestamp() * 1000
#
#
# def to_mili_key(timestamp):
#     dt_obj = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
#     return dt_obj.timestamp() * 1000


if __name__ == '__main__':
    # asyncio.get_event_loop().run_until_complete(read())
    cnx = mysql.connector.connect(user='root', password="",
                                  host='127.0.0.1',
                                  database='sig_trades')
    cursor = cnx.cursor()
    query = ("SELECT * FROM sig_trades.tick")
    cursor.execute(query)
    for price in cursor:
        print(price)



