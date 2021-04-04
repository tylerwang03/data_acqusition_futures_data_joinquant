from jqdata import finance
from jqdata import *
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from dateutil.parser import parse


def initialize(context):
    # 设置交易手续费
    set_order_cost(OrderCost(open_commission=0.0013, close_commission=0.0013), type='stock')
    # 对比指数:上证指数
    set_benchmark('000001.XSHG')
    # 每个月运行一次，开盘前调仓
    run_monthly(monthly, 1, time='9:00')
    set_option('use_real_price', True)
    # 三年定期利率
    s = {'2002-02-21': 2.52, '2004-10-29': 3.24, '2006-08-19': 3.69, '2007-03-18': 3.96, '2007-05-19': 4.41,
         '2007-07-21': 4.68, '2007-08-22': 4.95, '2007-09-15': 5.22, '2007-12-21': 5.4, '2008-10-09': 5.13,
         '2008-10-30': 4.77, '2008-11-27': 3.6, '2008-12-23': 3.33, '2010-10-20': 3.85, '2010-12-26': 4.15,
         '2011-02-09': 4.5, '2011-04-06': 4.75, '2011-07-07': 5.00, '2012-06-08': 4.65, '2012-07-06': 4.25,
         '2014-11-22': 4.00, '2015-03-01': 3.75, '2015-05-11': 3.50, '2015-06-28': 3.25, '2015-08-26': 3.00,
         '2015-10-24': 2.75}
    s = pd.Series(s)
    g.bond_yield = s[s.index <= context.current_dt.strftime('%Y-%m-%d')][-1]
    g.today = context.current_dt.strftime('%Y-%m-%d')
    # 一年调仓四次
    g.month = 1
    g.period = 3
    # A股为选股池
    g.scu = get_index_stocks('000002.XSHG')


# 调整非交易日数据为最近交易日数据
def shift_trading_day(date):
    tradingday = get_all_trade_days()
    date1 = datetime.date(date)
    # 得到date之后shift天那一天在列表中的行标号 返回一个数
    for i in tradingday[::-1]:
        if i <= date1:
            return i


# 股票的盈利回报率(市盈率倒数)应大于3年期定期存款利率收益的 2 倍
def condition_a(context):
    df = get_fundamentals(query(valuation.code, valuation.pe_ratio).
                          filter(valuation.code.in_(g.scu), 1 / valuation.pe_ratio * 100 > g.bond_yield * 2),
                          date=g.today)
    buylist = list(df['code'])
    return buylist


# 股票的市盈率应小于其过去五年最高市盈率的 40%
def condition_b(context):
    gfc = get_fundamentals_continuously(query(valuation.code, valuation.pe_ratio_lyr).
                                        filter(valuation.code.in_(g.scu)), end_date=g.today, count=250 * 5, panel=False)
    five_year_max = np.max(gfc['pe_ratio_lyr'])
    df = get_fundamentals(query(valuation.code, valuation.pe_ratio_lyr).
                          filter(valuation.code.in_(g.scu), valuation.pe_ratio_lyr < five_year_max * 0.4), date=g.today)
    buylist = list(df['code'])
    return buylist


# 股票派息率应大于3年期定期存款利率收益率的 2/3.
def condition_c(context):
    frq = finance.run_query(
        query(finance.STK_XR_XD.code, finance.STK_XR_XD.bonus_ratio_rmb, finance.STK_XR_XD.report_date).
        filter(finance.STK_XR_XD.code.in_(g.scu), finance.STK_XR_XD.bonus_ratio_rmb > g.bond_yield * (2 / 3)))
    pre_list = list(frq['code'])
    df = get_fundamentals(query(valuation.code).
                          filter(valuation.code.in_(pre_list)), date=g.today)
    buylist = list(df['code'])
    return buylist


# 股价要低于每股有形资产净值的 2/3.
def condition_d(context):
    df = get_fundamentals(
        query(valuation.code, valuation.market_cap, balance.total_current_assets, balance.fixed_assets,
              balance.total_liability).
        filter(valuation.code.in_(g.scu), valuation.market_cap < (2 / 3) * (
                    balance.total_current_assets + balance.fixed_assets - balance.total_liability)), date=g.today)
    buylist = list(df['code'])
    return buylist


# 股价要低于每股净流动资产(流动资产-总负债)的 2/3
def condition_e(context):
    df = get_fundamentals(
        query(valuation.code, valuation.market_cap, balance.total_current_assets, balance.total_liability).
        filter(valuation.code.in_(g.scu),
               valuation.market_cap < (2 / 3) * (balance.total_current_assets - balance.total_liability)), date=g.today)
    buylist = list(df['code'])
    return buylist


# 总负债要小于有形资产净值
def condition_f(context):
    df = get_fundamentals(
        query(valuation.code, balance.total_current_assets, balance.total_liability, balance.fixed_assets).
        filter(valuation.code.in_(g.scu),
               balance.total_liability < balance.fixed_assets + balance.total_current_assets - balance.total_liability),
        date=g.today)
    buylist = list(df['code'])
    return buylist


# 流动比率(流动资产/流动负债)要大于 2
def condition_g(context):
    df = get_fundamentals(query(valuation.code, balance.total_current_assets, balance.total_current_liability).
                          filter(valuation.code.in_(g.scu),
                                 balance.total_current_assets / balance.total_current_liability > 2), date=g.today)
    buylist = list(df['code'])
    return buylist


# 总负债要要小于净流动资产的 2 倍
def condition_h(context):
    df = get_fundamentals(query(valuation.code, balance.total_current_assets, balance.total_liability).
                          filter(valuation.code.in_(g.scu), balance.total_liability < (
                balance.total_current_assets - balance.total_liability) * 2), date=g.today)
    buylist = list(df['code'])
    return buylist


# 过去 10 年的平均年化盈利增长率要大于 7%
def condition_i(context):
    stocks = get_index_stocks('000002.XSHG', date=None)
    pre_list = []
    for i in stocks:
        now = get_price(i, start_date=shift_trading_day(context.current_dt - timedelta(days=1)),
                        end_date=shift_trading_day(context.current_dt - timedelta(days=1)), frequency='daily',
                        fields=['close'])
        ago = get_price(i,
                        start_date=shift_trading_day(context.current_dt - timedelta(days=1) - relativedelta(years=10)),
                        end_date=shift_trading_day(context.current_dt - timedelta(days=1) - relativedelta(years=10)),
                        frequency='daily', fields=['close'])
        if (not now.empty and not ago.empty):
            if now.values / ago.values ** (1 / 10) - 1 > 0.07:
                pre_list.append(i)
    return pre_list


# 过去十年中不能超过 2 次的盈利增长率小于-5
def condition_j(context):
    stocks = get_index_stocks('000002.XSHG', date=None)
    pre_list = []
    count = 0
    for i in stocks:
        for j in range(10):
            now = get_price(i, start_date=shift_trading_day(
                context.current_dt - timedelta(days=1) - relativedelta(years=j)),
                            end_date=shift_trading_day(context.current_dt - timedelta(days=1) - relativedelta(years=j)),
                            frequency='daily', fields=['close'])
            ago = get_price(i, start_date=shift_trading_day(
                context.current_dt - timedelta(days=1) - relativedelta(years=j + 1)), end_date=shift_trading_day(
                context.current_dt - timedelta(days=1) - relativedelta(years=j + 1)), frequency='daily',
                            fields=['close'])
            if (not now.empty and not ago.empty):
                if (now.values - ago.values) / ago.values > -0.05:
                    count = count + 1
                    if count >= 8:
                        pre_list.append(i)
                        count = 0
                        break
    return pre_list


# 根据选出的股票做出交易指令
def trade(context, buylist):
    for stock in context.portfolio.positions:
        if stock not in buylist:
            order_target(stock, 0)
    # 每次调仓，卖出所有不在buylist里的股票，然后所有在buylist的股票平分整个配置基金
    position_per_stk = context.portfolio.total_value / len(buylist)
    for stock in buylist:
        order_target_value(stock, position_per_stk)
    return


def monthly(context):
    # 月份除以3，如果余1则执行：第一个月调仓，第四个月调仓，第七个月调仓，第十个月调仓
    if g.month % g.period == 1:
        buylistA = condition_a(context)
        # buylistB = condition_b(context)
        # buylistC = condition_c(context)
        # buylistD = condition_d(context)
        # buylistE = condition_e(context)
        # buylistF = condition_f(context)
        # buylistG = condition_g(context)
        # buylistH = condition_h(context)
        # buylistI = condition_i(context)
        # buylistJ = condition_j(context)
        # buylist = list(set(buylistB) & set(buylistJ))
        trade(context, buylistA)
    else:
        pass
    g.month = g.month + 1