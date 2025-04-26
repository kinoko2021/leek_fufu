# coding=utf-8
from __future__ import print_function, absolute_import, unicode_literals
from gm.api import *

import datetime
import pandas as pd
import numpy as np

# ============ 工具函数 ============
def get_next_trading_day(current_date):
    """获取下一个交易日"""
    next_date = current_date + datetime.timedelta(days=1)
    trading_dates = get_trading_dates(exchange='SHSE', start_date=next_date, end_date=next_date + datetime.timedelta(days=365))
    return trading_dates[0] if trading_dates else None

def select_stocks(context):
    print(f"开始选股...")
    print("平仓后账户：", context.account().cash["available"])
    selected_symbols = []
    current_date = context.now.date()
    all_stocks = get_symbol_infos(1010, sec_type2=101001, exchanges=['SHSE', 'SZSE'], symbols=None, df=False)
    print(f"【获取股票列表】共{len(all_stocks)}只股票")

    # 排除ST和退市,并且上市时间大于10年
    all_stocks = [s for s in all_stocks if not ("ST" in s["sec_name"] or "退" in s["sec_name"]) and (current_date - s["listed_date"].date()).days > max(context.profit_years, context.eps_years) * 365]
    all_symbols = [s["symbol"] for s in all_stocks]
    print(f"【排除ST和退市,筛选上市时间大于{max(context.profit_years, context.eps_years)}年】共{len(all_stocks)}只股票")

    # # ========== 条件1：市值筛选 ==========
    tot_mvs = stk_get_daily_mktvalue_pt(symbols=all_symbols, fields="tot_mv", trade_date=None, df=False)
    all_symbols = [s["symbol"] for s in tot_mvs if s["tot_mv"] >= context.market_cap_min]
    market_cap_dict = {s["symbol"]: s["tot_mv"] for s in tot_mvs}  # 存储市值数据
    print(f"【市值筛选】共{len(all_symbols)}只股票")

    # # ========== 条件2：流动比率大于阈值 ==========
    current_rates = stk_get_finance_deriv_pt(all_symbols, fields="curr_rate", rpt_type=12, data_type=101, date=None, df=False)
    all_symbols = [s["symbol"] for s in current_rates if s["curr_rate"] >= context.current_ratio_min]
    current_ratio_dict = {s["symbol"]: s["curr_rate"] for s in current_rates}  # 存储流动比率数据
    print(f"【流动比率筛选】共{len(all_symbols)}只股票")

    # ========== 条件3：持续盈利检查 ==========
    for i in range(context.profit_years):
        query_date = (current_date.replace(year=current_date.year - i, month=12, day=31)).strftime('%Y-%m-%d')
        net_profs = stk_get_fundamentals_income_pt(all_symbols, rpt_type=12, data_type=101, date=query_date, fields="net_prof", df=False)
        all_symbols = [s["symbol"] for s in net_profs if s["net_prof"] > 0]
    print(f"【持续盈利检查】共{len(all_symbols)}只股票")
    
    # ========== 条件4：股息率筛选 ==========
    dividends = stk_get_daily_valuation_pt(all_symbols, fields="dy_lfy", df=False)
    all_symbols = [s["symbol"] for s in dividends if s["dy_lfy"] > context.dividend_yield_min]
    print(f"【股息率筛选】共{len(all_symbols)}只股票")

    symbol_name_dict = {s["symbol"]: s["sec_name"] for s in all_stocks}  # 股票代码与名称映射表
    selected_stocks = []  # 存储符合条件股票的信息
    print("【开始逐个筛选，检查EPS、PE、PB】")
    for symbol in all_symbols:
        try:
            # ========== 条件5：EPS N年增长 ==========
            start_date = (current_date.replace(year=current_date.year - context.eps_years, month=12, day=31)).strftime('%Y-%m-%d')
            end_date = current_date.replace(month=12, day=31).strftime('%Y-%m-%d')
            epses = stk_get_fundamentals_income(symbol, rpt_type=12, data_type=101, start_date=start_date, end_date=end_date, fields="eps_base", df=False)
            if len(epses) < context.eps_years - 1:
                continue
            current_3y_eps_avg = np.mean([eps["eps_base"] for eps in epses[-3:]])
            last_3y_eps_avg = np.mean([eps["eps_base"] for eps in epses[0:3]])
            eps_growth = (current_3y_eps_avg - last_3y_eps_avg) / last_3y_eps_avg
            if eps_growth < context.eps_growth_min:
                continue

            # # ========== 条件6：三年平均利润下的PE ==========
            # tclose = stk_get_daily_basic(symbol, fields="tclose", start_date=None, end_date=None, df=False)[0]['tclose']
            # pe = tclose / current_3y_eps_avg
            # if pe > context.pe_max or pe <= 0:
            #     continue
            # ========== 条件6：当前PE ==========
            pe = stk_get_daily_valuation(symbol=symbol, fields='pe_lyr', start_date=None, end_date=None, df=False)[0]['pe_lyr']
            if pe > context.pe_max or pe <= 0:
                continue
            

            # ========== 条件7：PB筛选 ==========
            pb_lyr = stk_get_daily_valuation(symbol=symbol, fields='pb_lyr', start_date=None, end_date=None, df=False)[0]['pb_lyr']
            if pb_lyr > context.pb_max:
                continue

            # ========== 条件8：PE*PB ==========
            if pe * pb_lyr > context.pe_pb_max:
                continue
            selected_stocks.append({
                "代码": symbol,
                "名称": symbol_name_dict.get(symbol, "未知"),
                "当前市值(元)": market_cap_dict.get(symbol, "N/A"),
                "流动比率": current_ratio_dict.get(symbol, "N/A"),
                "近三年EPS均值": current_3y_eps_avg,
                "PE": pe,
                "PB": pb_lyr
            })
            print(f"【Nice!!】找到符合条件股票：{symbol} - {symbol_name_dict.get(symbol, "未知")}")
        except Exception as e:
            print("error:::", str(e))
            continue

    return selected_stocks

def check_positions(context, current_date):
    """检查持仓是否符合卖出条件"""
    for position in get_position(account_id=None):  # 遍历所有持仓
        if position['symbol'] == context.hs300_symbol:
            continue
        # 计算绝对收益率
        if position["fpnl"] > 0.01:  # 浮盈超过10%
            # 卖出
            order_volume(position['symbol'], position['volume'], OrderSide_Sell,
                        OrderType_Market, PositionEffect_Close)

            print(f"【卖出】{position['symbol']}，浮盈：{position["market_value"] - position['cost']:.2f}元")

# ============ 策略代码 ============
def init(context):
    # 策略参数设置
    context.market_cap_min = 2e10          # 最小市值
    context.current_ratio_min = 2         # 流动比率下限
    context.pe_max = 30                   # 市盈率上限
    context.pb_max = 6                  # 市净率上限
    context.pe_pb_max = 150              # PE*PB上限
    context.eps_growth_min = 0.00          # EPS最小增长率
    context.profit_years = 4             # 持续盈利考察期（年）
    context.eps_years = 4                # eps考察期（年）
    context.dividend_yield_min = 1    # 股息率下限
    context.dividend_yield_year = 3     # 股息率考察期（年）

    # 全局变量
    context.hs300_symbol = 'SHSE.000300'  # 沪深300ETF代码
    context.first_trading_day_processed = False
    context.second_trading_day_processed = False
    context.current_year = 0
    context.frozen_cash = 0  # 冻结资金

    # 每日定时任务
    schedule(schedule_func=algo, date_rule='1d', time_rule='9:40:00')

def algo(context):
    current_date = context.now.date()
    current_year = current_date.year

    # 年度重置逻辑
    if context.current_year != current_year:
        context.current_year = current_year
        context.first_trading_day_processed = False
        context.second_trading_day_processed = False
        context.hs300_buy_price = None

    # 获取当年所有交易日
    trading_dates = get_trading_dates('SHSE', f'{current_year}-01-01', f'{current_year}-12-31')
    if not trading_dates:
        return

    # 第一个交易日处理
    if current_date == datetime.datetime.strptime(trading_dates[0], '%Y-%m-%d').date() and not context.first_trading_day_processed:
        print(f"【首个交易日】{current_date} 清空持仓")
        for position in get_position(account_id=None):  # 遍历所有持仓
            if position["volume"] > 0:  # 只处理有持仓的标的
                order_volume(position["symbol"], position["volume"], side=OrderSide_Sell , order_type=OrderType_Market ,position_effect=PositionEffect_Close)
        context.first_trading_day_processed = True

    # 第二个交易日处理
    elif len(trading_dates) >= 2 and datetime.datetime.strptime(trading_dates[1], '%Y-%m-%d').date() and not context.second_trading_day_processed:
        print(f"【第二个交易日】{current_date} 开始选股")
        selected_stocks = select_stocks(context)
        print(f"\n【开始执行买入操作】共{len(selected_stocks)}只股票")
        # 计算每只股票分配金额（可用资金的95%用于容错）
        cash_per_stock = context.account().cash["available"] * 0.95 / len(selected_stocks)
        context.frozen_cash = context.account().cash["available"] * 0.045
        for stock in selected_stocks:
            symbol = stock['代码']
            # 使用市价单买入
            order_value(symbol=symbol,
                        value=cash_per_stock,
                        side=PositionSide_Long,
                        order_type=OrderType_Market,
                        position_effect=PositionEffect_Open)
            print(f"买入 {stock['名称']}({symbol})，金额：{cash_per_stock:.2f}元")
        print("【买入操作完成】进入持有阶段")
        context.second_trading_day_processed = True

    # 其他交易日处理(预留)
    # else:
        # 每日持仓检查
        # check_positions(context, current_date)
        # # 处理待买入订单
        # amount = context.account().cash["available"] - context.frozen_cash
        # if amount > 0:
        #     order_value(context.hs300_symbol, amount, OrderSide_Buy,
        #             OrderType_Market, PositionEffect_Open)
        #     print(f"【买入】{context.hs300_symbol}，金额：{amount:.2f}元")



if __name__ == '__main__':
    '''
        strategy_id策略ID, 由系统生成
        filename文件名, 请与本文件名保持一致
        mode运行模式, 实时模式:MODE_LIVE回测模式:MODE_BACKTEST
        token绑定计算机的ID, 可在系统设置-密钥管理中生成
        backtest_start_time回测开始时间
        backtest_end_time回测结束时间
        backtest_adjust股票复权方式, 不复权:ADJUST_NONE前复权:ADJUST_PREV后复权:ADJUST_POST
        backtest_initial_cash回测初始资金
        backtest_commission_ratio回测佣金比例
        backtest_slippage_ratio回测滑点比例
        backtest_match_mode市价撮合模式，以下一tick/bar开盘价撮合:0，以当前tick/bar收盘价撮合：1
        '''
    run(strategy_id='',
        filename='main.py',
        mode=MODE_BACKTEST,
        token='',
        backtest_start_time='2020-1-1 8:00:00',
        backtest_end_time='2025-4-20 16:00:00',
        backtest_adjust=ADJUST_PREV,
        backtest_initial_cash=10000000,
        backtest_commission_ratio=0.0001,
        backtest_slippage_ratio=0.0001,
        backtest_match_mode=1)
