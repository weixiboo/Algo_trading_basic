#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 17 13:32:56 2025

@author: boow
"""
from bisect import bisect_right
import datetime
import numpy as np
import pandas as pd
from polygon import RESTClient
from polygonKey import polygonKey
import spyder_kernels.utils.iofuncs as iofuncs
from data.options.names  import read_contract_names

client = RESTClient(polygonKey)
my_ticker_list = iofuncs.load_dictionary("ticker_list.spydata")[0]['tickers']

def market_snapshot(ticker_list):
    
    snapshot = client.get_snapshot_all(market_type = 'stocks',tickers = ticker_list)

    avail_ticker = []
    vwap_today = []
    vwap_yesterday = []
    
    for i in snapshot:
        if i != None and i.day != None and i.prev_day.vwap != 0:
            avail_ticker.append(i.ticker)
            vwap_today.append(i.day.vwap)
            vwap_yesterday.append(i.prev_day.vwap)
    
    vwap_today = np.array(vwap_today)
    vwap_yesterday = np.array(vwap_yesterday)
    
    vwap_change = vwap_today/vwap_yesterday
    
    out_change = pd.Series(vwap_change,index=avail_ticker)
    out_change = out_change.sort_index()
    
    out_vwap = pd.Series(vwap_today,index=avail_ticker)
    out_vwap = out_vwap.sort_index()
    
    out_vwap_yesterday = pd.Series(vwap_yesterday,index=avail_ticker)
    out_vwap_yesterday = out_vwap_yesterday.sort_index()
    
    return out_change, out_vwap, out_vwap_yesterday




def bid_ask(ticker_list):
      
    avail_ticker = []
    bid = []
    ask = []
    
    ticker_list = np.array_split(np.array(ticker_list), np.ceil(len(ticker_list) / 250))
    
    for sub_ticker_list in ticker_list:
        snapshots = client.list_universal_snapshots(ticker_any_of= sub_ticker_list)
        for i in snapshots:
            if i != None and i.last_quote != None:
                avail_ticker.append(i.ticker)
                bid.append(i.last_quote.bid)
                ask.append(i.last_quote.ask)
    
    out = pd.DataFrame({"bid": bid,"ask": ask},index = avail_ticker)
    out = out.sort_index()
    
    return out

def my_options_chain(ticker,DTE = 2):
    #Give options chain with expiration date AT LEAST DTE > days and within a week of DTE 
    
    #import data
    today = datetime.date.today()
    data_date = today - datetime.timedelta(days = today.weekday())
    data = read_contract_names(ticker, data_date.year, data_date.month, 
                               data_date.day,True)
        
    ticker_bid_ask = bid_ask([ticker])
    
    if len(ticker_bid_ask) == 0:
        return
    
    bid = ticker_bid_ask.loc[ticker].bid
    ask = ticker_bid_ask.loc[ticker].bid
    mid_price = (bid+ask)/2
    
    if data is None:
        return
    
    #Find the closest expiration date
    today = today + datetime.timedelta(days = DTE - 1)
    exp_date = bisect_right(data['expiration_date'], today.strftime("%Y-%m-%d"))
    exp_date = data['expiration_date'][exp_date] if exp_date < len(data) else None
    
    if exp_date is None:
        return
    
    #Make sure data within a week
    if (datetime.datetime.strptime(exp_date,"%Y-%m-%d").date() - today) <= datetime.timedelta(weeks = 1):
        data = data[data['expiration_date'] == exp_date]
    else:
        return
    
    
    #Seperate call and put
    call_data = data[data.contract_type=='call']
    put_data = data[data.contract_type=='put']
    
    #Find contract closest to strike price
    call_ind = bisect_right(np.array(call_data.strike_price), mid_price)
    put_ind = bisect_right(np.array(put_data.strike_price), mid_price)
    
    call_ind_left = max(0,call_ind-4)
    call_ind_right = min(len(call_data),call_ind+4)
    
    put_ind_left = max(0,put_ind-4)
    put_ind_right = min(len(put_data),put_ind+4)
    
    #Reformat data
    call_data = call_data.iloc[call_ind_left:call_ind_right]
    put_data = put_data.iloc[put_ind_left:put_ind_right]
    
    if not ( all(call_data['cfi'] == "OCASPS") and all(put_data['cfi'] == "OPASPS")):
        return
        raise TypeError("Non standard options in the list")
        
    call_data = call_data[['ticker','expiration_date','strike_price','contract_type','underlying_ticker']]
    put_data = put_data[['ticker','expiration_date','strike_price','contract_type','underlying_ticker']]
    
    call_data.set_index('ticker',inplace = True) 
    put_data.set_index('ticker',inplace = True) 
    
    call_bid_ask = bid_ask(call_data.index)
    put_bid_ask = bid_ask(put_data.index) 

    call_data = pd.concat([call_bid_ask,call_data] , axis = 1)
    put_data = pd.concat([put_bid_ask,put_data] , axis = 1)
    
    call_data['DTE'] = pd.to_datetime(call_data['expiration_date']) - pd.Timestamp.today().normalize()
    put_data['DTE'] = pd.to_datetime(put_data['expiration_date']) - pd.Timestamp.today().normalize()
       
    return call_data,put_data

def market_status():
    a = client.get_market_status()
    print(a.exchanges)
    
    
def top_liquidity(frac = 0.85):
    avail_ticker = []
    vwap = []
    volume = []
    
    snap_shot = client.get_snapshot_all(market_type = 'stocks')
    
    for i in snap_shot:
        avail_ticker.append(i.ticker)
        vwap.append(i.day.vwap)
        volume.append(i.day.volume)
    
    liquidity = np.array(vwap)*np.array(volume)
    
    out = pd.Series(liquidity,index=avail_ticker)
    out = out.sort_values(ascending = False)
    
    count = np.searchsorted(out.cumsum(), out.sum()*frac) + 1 
    out = out.iloc[:count]
    
    return out.sort_index()


#if __name__ == "__main__":
#    1+1
