import pandas as pd
from datetime import datetime, timedelta
import jqdatasdk as jq


# use the function to log in jq and get the list of objects
def get_futures_list():
    # log in the  JQ account
    jq.auth('18224433211', 'Haohao19971003.')
    all_futures = jq.get_all_securities(['futures'])
    # name the index of the DataFrame
    all_futures.index.name = 'id'
    # convert index column to list
    futures_idx = all_futures.index.values.tolist()
    substring = '8888'
    empty = []
    for idx in futures_idx:
        if substring in idx:
            empty.append(idx)
    return empty


# use the function to get large size of data in a range of time
def get_data_yearly(f_list, f_start, f_end):
    future_info_yearly = jq.get_price(security=f_list, start_date=datetime.strptime(f_start, '%Y-%m-%d'),
                                      end_date=datetime.strptime(f_end, '%Y-%m-%d'), frequency='1d')
    return future_info_yearly


# use the function to manipulate dataframe before append to the csv
def manipulate_df(f_df):
    f_df.dropna(inplace=True)
    f_df.sort_values(by=['time'], inplace=True)
    f_df.to_csv('futures_info.csv', mode='a', index=False, index_label='time')


# use the function to give a range of dates and print the dates that are not in the csv
def get_missing_date(start, end):
    future_info = pd.read_csv('futures_info.csv')
    # groupby datetime and get the size of futures of that day
    date_with_data = future_info.groupby(['time']).size()
    all_date = pd.date_range(start=datetime.strptime(start, '%Y-%m-%d'), end=datetime.strptime(end, '%Y-%m-%d'))
    date_with_data.index = pd.DatetimeIndex(date_with_data.index)
    # set new index to data series, if its NaN then 0
    date_with_data = date_with_data.reindex(all_date, fill_value=0)
    miss_date_list = []
    # get the date of missing data
    for index, value in date_with_data.items():
        if value == 0:
            miss_date_list.append(index.strftime('%Y-%m-%d'))
    print(miss_date_list)
    return miss_date_list


# use the function to iterate missing dates and get the info of futures on that day.
def get_miss_data(f_date, f_list):
    info_df = []
    for day in f_date:
        future_info = jq.get_price(security=f_list,
                                   start_date=datetime.strptime(day, '%Y-%m-%d'),
                                   end_date=datetime.strptime(day, '%Y-%m-%d'), frequency='1d')
        info_df.append(future_info)
    futures_df = pd.concat(info_df)
    return futures_df


def csv_clean():
    df = pd.read_csv('futures_info.csv')
    df.drop_duplicates(subset=['time', 'code'], inplace=True)
    df.sort_values(by='time', inplace=True)
    df = df[df.time.str.contains('time') == False]
    df.to_csv('futures_info.csv', index=False, index_label='time')


# give a range of dates and get all futures info within the time range
futures_list = get_futures_list()
futures_df = get_data_yearly(futures_list, '2017-07-12', '2017-07-20')
manipulate_df(futures_df)
csv_clean()

# # give a range of dates; print the dates that are not on the csv; get all futures info on missing days; append to csv
# miss_date = get_missing_date('2017-07-10', '2017-07-23')
# futures_list = get_futures_list()
# future_info = get_miss_data(miss_date, futures_list)
# manipulate_df(future_info)
# csv_clean()
