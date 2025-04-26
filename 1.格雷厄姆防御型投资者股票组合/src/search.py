# coding=utf-8
from __future__ import print_function, absolute_import, unicode_literals
from gm.api import *

import datetime
import pandas as pd
import numpy as np

def init(context):
    # 策略参数设置
    context.market_cap_min = 5e9          # 最小市值（20亿元）
    context.current_ratio_min = 2         # 流动比率下限
    context.pe_max = 15                   # 市盈率上限
    context.pb_max = 1.5                  # 市净率上限
    context.pe_pb_max = 22.5              # PE*PB上限
    context.eps_growth_min = 0.33       # EPS最小增长率
    context.profit_years = 10             # 持续盈利考察期（年）
    context.eps_years = 10                # eps考察期（年）
    context.dividend_yield_min = 0        # 股息率下限
    context.dividend_yield_year = 20     # 股息率考察期（年）

def handlebar(context):
    print(f"【选股时间】{context.now} 开始选股...")
    selected_symbols = []
    current_date = context.now.date()
    all_stocks = get_symbol_infos(1010, sec_type2=101001, exchanges=['SHSE', 'SZSE'], symbols=None, df=False)
    print(f"【获取股票列表】共{len(all_stocks)}只股票")

    # 排除ST和退市,并且上市时间大于10年
    all_stocks = [s for s in all_stocks if not ("ST" in s["sec_name"] or "退" in s["sec_name"]) and (current_date - s["listed_date"].date()).days > max(context.profit_years, context.eps_years, context.dividend_yield_year) * 365]
    all_symbols = [s["symbol"] for s in all_stocks]
    print(f"【排除ST和退市,筛选上市时间大于{max(context.profit_years, context.eps_years, context.dividend_yield_year)}年】共{len(all_stocks)}只股票")

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

            # ========== 条件6：三年平均利润下的PE ==========
            tclose = stk_get_daily_basic(symbol, fields="tclose", start_date=None, end_date=None, df=False)[0]['tclose']
            pe = tclose / current_3y_eps_avg
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


    if selected_stocks:
        df = pd.DataFrame(selected_stocks)
        filename = f"选股结果_{context.now.strftime('%Y%m%d%H%M%S')}.xlsx"
        df.to_excel(filename, index=False)
        print(f"【结果已保存】共筛选出{len(selected_stocks)}只股票，文件路径: {filename}")
    else:
        print(f"【选股结果】{context.now} 无符合条件股票")


# 注意：实际使用需根据掘金API更新字段名和返回值结构


if __name__ == '__main__':
    # 运行选股策略
    set_token('')  # 替换为你的token
    context = type('Context', (object,), {})()
    init(context)
    context.now = datetime.datetime(2020, 1, 1, 9, 0)  # 设置当前时间
    handlebar(context)