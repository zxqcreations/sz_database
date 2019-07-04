import requests as req
import time
import numpy as np
import json
import re, os
from datetime import datetime as dt

#proxy = {'https': 'https://child-prc.intel.com:913',
#         'http': 'http://child-prc.intel.com:913'}
proxy = {}
headers = {
    "Host": "yunhq.sse.com.cn:32041",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
    "Referer": "http://www.sse.com.cn/market/price/trends/",
}
stock_dataset_folder = 'stockdata'
if stock_dataset_folder not in os.listdir('./'):
    os.mkdir('./' + stock_dataset_folder)
stock_dataset_folder += '/'


def download_full_stock_data():
    index_data_set = './indexList/sz_all_list.csv'
    with open(index_data_set, 'r', encoding="utf-8-sig") as f:
        header = f.readline()
        while True:
            ori_data = f.readline()
            if not ori_data:
                break
            datas = ori_data.split(',')
            stock_id = datas[7]
            stock_name = datas[10]
            print('Fetching', stock_name, stock_id)
            res = get_single_stock_data(stock_id, '-300', '-1')
            if not res:
                time.sleep(1)
                continue
            stock_price = json.loads(res)
            result = stock_price['kline']
            stock_file = stock_dataset_folder+str(stock_id) + '.csv'
            dataset = ['id,date,open,high,low,close,volume']
            for _id, item in enumerate(result):
                item_data = [_id]+item
                item_data = [str(i) for i in item_data]
                item_data_str = ','.join(item_data)
                dataset.append(item_data_str)
            stock_price_history = '.\n'.join(dataset)
            with open(stock_file, 'w', encoding="utf-8-sig") as s:
                s.write(stock_price_history)
            time.sleep(1)
        

def update_stock_data():
    index_data_set = './indexList/sz_SSE_Composite_Index_list.csv'
    with open(index_data_set, 'r', encoding="utf-8-sig") as f:
        header = f.readline()
        while True:
            ori_data = f.readline()
            if not ori_data:
                break
            datas = ori_data.split(',')
            stock_id = datas[3]
            stock_name = datas[2]
            stock_file = stock_dataset_folder+str(stock_id) + '.csv'
            with open(stock_file, 'r+', encoding="utf-8-sig") as s:
                #f.seek(-200,2)
                tmp_lines = s.readlines()
                last_line = tmp_lines[-1]
                last_item = last_line.split(',')
                last_time = last_item[1]
                begin = (dt(int(last_time[:4]), 
                            int(last_time[4:6]),
                            int(last_time[6:8]))-dt.now()).days
                print('Fetching', stock_name, stock_id, begin)
                res = get_single_stock_data(stock_id, str(begin), '-1')
                if not res:
                    time.sleep(1)
                    continue
                stock_price = json.loads(res)
                result = stock_price['kline']
                
                if len(result)>0:
                    result_str = ','.join(result)
                    s.write(result_str)
            time.sleep(1)
            

def get_random_serial():
    timestamp = int(time.time() * 1000)
    random_serial = 'jQuery11120' + str(int(np.random.rand() * 1e16)) + '_' + str(timestamp - 1)
    return timestamp, random_serial


def get_single_stock_data(index, begin, end):
    timestamp, random_serial = get_random_serial()
    url = 'http://yunhq.sse.com.cn:32041/v1/sh1/dayk/'+str(index) \
            +'?callback='+random_serial+'&select=date,open,high,low,close,volume&begin=' \
            +begin+'&end='+end+'&_=' \
            +str(timestamp)
    res = req.get(url, proxies=proxy, headers=headers).text
    res = re.search('\((.*)\)', res)
    if res:
        res=res.groups()[0]
    return res


if __name__ == '__main__':
    download_full_stock_data()
    #update_stock_data()