import requests as req
import time
import numpy as np
import json
import re, os

proxy = {'https': 'https://child-prc.intel.com:913',
         'http': 'http://child-prc.intel.com:913'}
headers = {
    "Host": "www.sse.com.cn",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
    "Referer": "http://www.sse.com.cn/market/sseindex/indexlist/constlist/index_new.shtml?COMPANY_CODE=000001&cfg=yes&INDEX_Code=000001&type=1"
}
index_dataset_folder = 'indexList'
if index_dataset_folder not in os.listdir('./'):
    os.mkdir('./' + index_dataset_folder)
index_dataset_folder += '/'


def get_random_serial():
    timestamp = int(time.time() * 1000)
    random_serial = 'jQuery11120' + str(int(np.random.rand() * 1e16)) + '_' + str(timestamp - 1)
    return timestamp, random_serial


def get_sz_data_by_database(database, indexcode=None):
    indexcode = '' if not indexcode else '&indexCode=' + str(indexcode)
    timestamp, random_serial = get_random_serial()
    url = 'http://query.sse.com.cn/commonSoaQuery.do?jsonCallBack=' + random_serial \
          + '&sqlId=' + database + indexcode + '&isPagination=false&_=' + str(timestamp)
    res = req.get(url, proxies=proxy, headers=headers).text
    res = re.search('\((.*)\)', res).groups()[0]
    return res


def sz_all_list():
    stock_list_file = index_dataset_folder + 'sz_all_list.csv'
    res = get_sz_data_by_database('DB_SZZSLB_ZSLB')

    stock_list = json.loads(res)
    result = stock_list['result']

    dataset = [
        'id,handbookUrl,nIndexFullNameEn,nIndexNameEn,tIndexCode,nIndexCode,indexReleaseChannel,indexCode,issueVolumn,introEn,indexFullName,nIndexFullName,totalReturnIntro,tIndexNameEn,indexFullNameEn,isNetLncomeIndex,intro,tIndexFullName,indexBaseDay,ifIndexCode,numOfStockes,netReturnIntroEn,indexBasePoint,indicsSeqDescEn,indexName,netReturnIntro,isPriceIndex,updateTime,indicsSeqDesc,indexDataSourceType,launchDay,methodologyNameEn,methodologyName,tIndexFullNameEn,handbookEnUrl,indicsSeq,nIndexName,indexNameEn,totalReturnIntroEn,tIndexName,isTotalReturnIndex']

    for _id, item in enumerate(result):
        item_data = str(_id) + ',' + str(item['handbookUrl']).replace(',', '') + ',' + str(
            item['nIndexFullNameEn']).replace(',', '') + ',' + str(item['nIndexNameEn']).replace(',', '') + ',' + str(
            item['tIndexCode']).replace(',', '') + ',' + str(item['nIndexCode']).replace(',', '') + ',' + str(
            item['indexReleaseChannel']).replace(',', '') + ',' + str(item['indexCode']).replace(',', '') + ',' + str(
            item['issueVolumn']).replace(',', '') + ',' + str(item['introEn']).replace(',', '') + ',' + str(
            item['indexFullName']).replace(',', '') + ',' + str(item['nIndexFullName']).replace(',', '') + ',' + str(
            item['totalReturnIntro']).replace(',', '') + ',' + str(item['tIndexNameEn']).replace(',', '') + ',' + str(
            item['indexFullNameEn']).replace(',', '') + ',' + str(item['isNetLncomeIndex']).replace(',',
                                                                                                    '') + ',' + str(
            item['intro']).replace(',', '') + ',' + str(item['tIndexFullName']).replace(',', '') + ',' + str(
            item['indexBaseDay']).replace(',', '') + ',' + str(item['ifIndexCode']).replace(',', '') + ',' + str(
            item['numOfStockes']).replace(',', '') + ',' + str(item['netReturnIntroEn']).replace(',', '') + ',' + str(
            item['indexBasePoint']).replace(',', '') + ',' + str(item['indicsSeqDescEn']).replace(',', '') + ',' + str(
            item['indexName']).replace(',', '') + ',' + str(item['netReturnIntro']).replace(',', '') + ',' + str(
            item['isPriceIndex']).replace(',', '') + ',' + str(item['updateTime']).replace(',', '') + ',' + str(
            item['indicsSeqDesc']).replace(',', '') + ',' + str(item['indexDataSourceType']).replace(',',
                                                                                                     '') + ',' + str(
            item['launchDay']).replace(',', '') + ',' + str(item['methodologyNameEn']).replace(',', '') + ',' + str(
            item['methodologyName']).replace(',', '') + ',' + str(item['tIndexFullNameEn']).replace(',',
                                                                                                    '') + ',' + str(
            item['handbookEnUrl']).replace(',', '') + ',' + str(item['indicsSeq']).replace(',', '') + ',' + str(
            item['nIndexName']).replace(',', '') + ',' + str(item['indexNameEn']).replace(',', '') + ',' + str(
            item['totalReturnIntroEn']).replace(',', '') + ',' + str(item['tIndexName']).replace(',', '') + ',' + str(
            item['isTotalReturnIndex']).replace(',', '')
        dataset.append(item_data)
        index_full_name_en = item['indexFullNameEn'].replace(' ', '_').replace(',', '')
        index_no = item['indexCode']
        index_name = item['indexName']
        sz_sub_list(index_no, index_full_name_en, index_name)
        time.sleep(2)

    with open(stock_list_file, 'w', encoding="utf-8-sig") as w:
        stock_list_csv_type_result = '\n'.join(dataset)
        w.write(stock_list_csv_type_result)


def sz_sub_list(index, sub_name, sub_name_cn):
    print('Fetching', sub_name_cn)
    stock_list_file = index_dataset_folder + 'sz_' + sub_name + '_list.csv'
    res = get_sz_data_by_database('DB_SZZSLB_CFGLB', str(index))

    stock_list = json.loads(res)
    result = stock_list['result']

    dataset = ['id,securityAbbrEn,securityAbbr,securityCode,marketSource,inDate']

    for _id, item in enumerate(result):
        item_data = str(_id) + ',' + item['securityAbbrEn'].replace(',', '') + ',' + item['securityAbbr'] \
                    + ',' + item['securityCode'] + ',' + item['marketSource'] + ',' + item['inDate']
        dataset.append(item_data)

    with open(stock_list_file, 'w', encoding="utf-8-sig") as w:
        stock_list_csv_type_result = '\n'.join(dataset)
        w.write(stock_list_csv_type_result)


if __name__ == '__main__':
    sz_all_list()
