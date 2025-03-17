#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 20 14:08:25 2024

@author: boow
"""

#Purpose: get data for entire stock market and do simulation for all stock market

import datetime
import stk_data as sd
import numpy as np
import spyder_kernels.utils.iofuncs as iofuncs


class market:

    # Constructor method (initialize instance variables)
    def __init__(self,start_date,end_date):
        
        #Parameters
        self.interest_rate = 4.3 #APY
        self.interest_rate = np.log(1+self.interest_rate/100/365)
        
        #Loading the list of stock tickers
        self.ticker_list = iofuncs.load_dictionary("ticker_list.spydata")[0]['tickers']
        self.ticker_list.remove('VOO') #Remove S&P500 index from it
        self.start_date = start_date
        self.end_date = end_date
        
        
        #Import Data
        self.market_day_data = sd.read_combine_data(self.ticker_list, 1, 'day', self.start_date, self.end_date)
        self.bench_day_data = sd.read_combine_data(['VOO'], 1, 'day', self.start_date, self.end_date)

        #Calculate the %change of each day
        self.market_day_cdata = self.change_on_dict_df(self.market_day_data,shift = 1)
        self.bench_day_cdata = self.change_on_dict_df(self.bench_day_data,shift = 1)

        #Print out which stock are not avaliable
        print("Not avaliable tickers:" , set(self.ticker_list) - set(self.market_day_data['open'].columns))
        self.ticker_list = list(self.market_day_data['open'].columns)
        
        
    def change_on_dict_df(self,dict_df,shift = 1):
        out = {}
        
        for name, df in dict_df.items():
            out[name] = df/df.shift(shift)
            
        return out    
        
    def current_price(self,time):
        return self.market_day_data['vwap'].loc[time]
    
    def bench_price(self,start_time,end_time):
        return np.array(self.bench_day_data['vwap'].loc[start_time:end_time])
    
    def bench_return(self,start_time,end_time,shift = 1):
        return self.bench_day_cdata['vwap'].loc[start_time:end_time]
       
    
if __name__ == "__main__":
    #example of how to setup
    start_date = datetime.date(2024,1,1)
    end_date = datetime.date(2024,1,31)
    A = market(start_date,end_date)
    