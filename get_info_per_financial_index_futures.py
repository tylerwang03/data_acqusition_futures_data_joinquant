import pandas as pd
from datetime import datetime, timedelta
import jqdatasdk as jq
import os


class FuturesInfo:
    data_exist = []
    empty = []

    def __init__(self, f_start, f_end, f_kind):
        self.f_start = f_start
        self.f_end = f_end
        self.f_kind = f_kind
        self.path = the_path

    def get_large_data(self):
        global data_exist
        global empty
        f = open("futures_list.txt", "w+")
        lis = f.read()
        data_exist = lis.split(" ")
        # log in the  JQ account
        jq.auth('18224433211', 'Haohao19971003.')
        all_futures = jq.get_all_securities(['futures'])
        # name the index of the DataFrame
        all_futures.index.name = 'id'
        # convert index column to list
        futures_idx = all_futures.index.values.tolist()
        substring = self.f_kind
        empty = []
        for idx in futures_idx:
            if substring in idx:
                empty.append(idx)
        count = 0
        for item in empty:
            future_info_yearly = jq.get_price(security=item, start_date=datetime.strptime(self.f_start, '%Y-%m-%d'),
                                              end_date=datetime.strptime(self.f_end, '%Y-%m-%d'), frequency='1m')
            str = item + '.csv'
            future_info_yearly.index.name = 'time'
            future_info_yearly.dropna(inplace=True)
            future_info_yearly.to_csv(os.path.join(self.path, str), mode='a')
            if item not in data_exist:
                data_exist.append(item)

            count += 1
            print(count)
        f = open("futures_list.txt", "w")
        f.write(" ".join(empty))
        f.close()
        for item in data_exist:
            if item == "":
                continue
            str = item + '.csv'
            df = pd.read_csv(self.path + str)
            df.drop_duplicates(subset=['time'], inplace=True)
            df.sort_values(by='time', inplace=True)
            df = df[df.time.str.contains('time') == False]
            df.to_csv(os.path.join(self.path, str), index=False, index_label='time')
        print(data_exist)


    def get_missing_data(self):
        global data_exist
        global path
        f = open("futures_list.txt", "r+")
        lis = f.read()
        data_exist = lis.split(" ")
        futures_dict = {}
        substring = self.f_kind
        for item in data_exist:
            if item == "":
                continue
            if substring in item:
                str = item + '.csv'
                future_info = pd.read_csv(self.path + str)
                # groupby datetime and get the size of futures of that day
                date_with_data = future_info.groupby(['time']).size()
                all_date = pd.date_range(start=datetime.strptime(self.f_start, '%Y-%m-%d'),
                                     end=datetime.strptime(self.f_end, '%Y-%m-%d'))
                date_with_data.index = pd.DatetimeIndex(date_with_data.index)
                # set new index to data series, if its NaN then 0
                date_with_data = date_with_data.reindex(all_date, fill_value=0)
                # get the date of missing data
                date_with_data = date_with_data[date_with_data.index.dayofweek < 5]
                miss_date_list = []
                for index, value in date_with_data.items():
                    if value == 0:
                        miss_date_list.append(index.strftime('%Y-%m-%d'))
                        futures_dict[item] = miss_date_list
        for key, value in futures_dict.items():
            print(key, end=":")
            print(value)
        jq.auth('18224433211', 'Haohao19971003.')
        count = 0
        for item in futures_dict:
            date_list = []
            for time in futures_dict[item]:
                date_list.append(datetime.strptime(time, '%Y-%m-%d'))
            max_date = max(date_list)
            min_date = min(date_list)
            future_info = jq.get_price(security=item,
                                           start_date=min_date,
                                           end_date=max_date, frequency='1m')
            str = item + '.csv'
            future_info.to_csv(os.path.join(self.path, str), mode='a')
            count += 1
            print(count)
        for item in data_exist:
            if item == "":
                continue
            str = item + '.csv'
            df = pd.read_csv(self.path + str)
            df.drop_duplicates(subset=['time'], inplace=True)
            df.sort_values(by='time', inplace=True)
            df.dropna(inplace = True)
            df.to_csv(os.path.join(self.path, str), index=False, index_label='time')




# 交易所名称	编码
# 上海期货交易所	XSGE
# 大连商品交易所	XDCE
# 郑州商商品交易所	XZCE
# 中国金融期货交易所	CCFX
# 所有期货数据 ''
# this part is used for initial downloading
the_path = '/Users/tylerwang/Desktop/get_futures_data/'
# get_large = FuturesInfo('2015-01-01','2015-02-01','CCFX')
# get_large.get_large_data()

# # # this part is used for appending new data
get_miss = FuturesInfo('2003-01-01', '2015-02-01', 'CCFX')
get_miss.get_missing_data()
