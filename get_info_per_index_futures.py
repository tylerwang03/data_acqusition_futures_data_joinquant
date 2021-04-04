import pandas as pd
from datetime import datetime, timedelta
import jqdatasdk as jq
import os


class FuturesInfo:
    data_exist = []

    def __init__(self, f_start, f_end):
        self.f_start = f_start
        self.f_end = f_end
        self.path = the_path

    def get_large_data(self):
        global data_exist
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
        substring = ['IC9999.CCFX','IC8888.CCFX','IF9999.CCFX','IF8888.CCFX','IH9999.CCFX','IH8888.CCFX',
                     'T9999.CCFX','T8888.CCFX','TF9999.CCFX','TF8888.CCFX','TS9999.CCFX','TS8888.CCFX']
        empty = []
        for idx in futures_idx:
            for element in substring:
                if idx == element:
                    empty.append(idx)
        count = 0
        for item in empty:
            future_info_yearly = jq.get_price(security=item, start_date=datetime.strptime(self.f_start, '%Y-%m-%d'),
                                              end_date=datetime.strptime(self.f_end, '%Y-%m-%d'), frequency='1m')
            str = item + '.csv'
            future_info_yearly.index.name = 'time'
            future_info_yearly.dropna(inplace=True)
            if not future_info_yearly.empty:
                future_info_yearly.to_csv(os.path.join(self.path, str), mode='a')
                if item not in data_exist:
                    data_exist.append(item)

            count += 1
            print(count)
        f = open("futures_list.txt", "w")
        f.write(" ".join(data_exist))
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
        for item in data_exist:
            if item == "":
                continue
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
            for time in futures_dict[item]:
                future_info = jq.get_price(security=item,
                                           start_date=datetime.strptime(time, '%Y-%m-%d'),
                                           end_date=datetime.strptime(time, '%Y-%m-%d'), frequency='1m')
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
            df = df[df.time.str.contains('time') == False]
            df.to_csv(os.path.join(self.path, str), index=False, index_label='time')

the_path = '/Users/tylerwang/Desktop/get_futures_data/'
get_large = FuturesInfo('2015-01-01','2016-01-01')
get_large.get_large_data()

# get_miss = FuturesInfo('2016-02-25', '2016-06-01')
# get_miss.get_missing_data()
