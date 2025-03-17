#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 11:00:17 2024

@author: boow
"""

import datetime
import multiprocessing
import numpy as np
import pandas as pd
import os
import pickle
import psutil
import signal
import lz4.frame  # type: ignore
from ... import signal_handler
from data.stocks.get import get_aggs

signal.signal(signal.SIGINT, signal_handler)


def read_trades(ticker, date, multiplier ,freq, save_file = True):
    """Reads trades for a given ticker and date, then prints them."""

    #convert freq dict
    polygon_to_pandas = {"second":"S","minute":"T","hour": "H", "day":"D", 
                         "week": "W", "month":"M", "quater":"Q", "year":"Y"}
    
    
    # Construct the filename, similar to your writer script   
    directory = os.path.expanduser(f"~/Desktop/Market_Data/{ticker}/{multiplier}{freq}")
    filename = f"{ticker}-aggs-{date}-freq-{multiplier}-{freq}.pickle.lz4"
    filepath = os.path.join(directory, filename)
    
    trades = []
    if not os.path.isfile(filepath):
        #return
        trades = get_aggs((ticker, date, multiplier, freq, save_file))
    else:    
        try:
            with open(filepath, "rb") as file:
                compressed_data = file.read()
                trades = pickle.loads(lz4.frame.decompress(compressed_data))
        except FileNotFoundError:
            print(f"No file found for {ticker} at {date}")
        except Exception as e:
            print(f"An error occurred: {e} for {ticker}")

    if not trades:
        return
    else:
        trades = pd.DataFrame(trades)
        trades['timestamp'] = pd.to_datetime(trades['timestamp'], unit='ms')
        trades.set_index('timestamp', inplace=True)
        
        # #Creating a complete date range
        # date_range = pd.date_range(start=trades.index.min(), 
        # end=trades.index.max(), freq= f"{multiplier}{polygon_to_pandas[f'{freq}']}" )
        
        # # Reindexing the DataFrame to include all dates in the range
        # trades = trades.reindex(date_range)
        
        #print(trades)
        return trades.astype(pd.SparseDtype("float", np.nan))


def read_data_serial(ticker,multiplier,freq,start_date,end_date):
    #read data from polygon into dataframe

    # Loop through each weekday between the start and end dates and read the trades
    day = start_date
    data = []
    
    while day <= end_date:
        if day.weekday() < 5:  # 0-4 denotes Monday to Friday
            data.append(read_trades(ticker, day, multiplier, freq))
        day += datetime.timedelta(days=1)

    data = [df for df in data if df is not None]
    
    if not data:
        return
    else:
        data = pd.concat(data,axis = 0)
        return data

def read_data(ticker_list,multiplier,freq,start_date,end_date):
    #Reads data parallel
    num_processes = psutil.cpu_count(logical=False)
    pool = multiprocessing.Pool(processes=num_processes)    
    return pool.starmap(read_data_serial, [(ticker,multiplier,freq,start_date,end_date) for ticker in ticker_list] )

def combine_data(ticker_list,all_data):
    #Takes all the data and put them into 8 dataframe

    open_df = pd.concat([df["open"] for df in all_data if df is not None],axis = 1)
    high_df = pd.concat([df["high"] for df in all_data if df is not None],axis = 1)
    low_df = pd.concat([df["low"] for df in all_data if df is not None],axis = 1)
    close_df = pd.concat([df["close"] for df in all_data if df is not None],axis = 1)
    volume_df = pd.concat([df["volume"] for df in all_data if df is not None],axis = 1)
    vwap_df = pd.concat([df["vwap"] for df in all_data if df is not None],axis = 1)
    transactions_df = pd.concat([df["transactions"] for df in all_data if df is not None],axis = 1)
    otc_df = pd.concat([df["otc"] for df in all_data if df is not None],axis = 1)       
    
    ticker_list =[ticker for i, ticker in enumerate(ticker_list) if all_data[i] is not None]
    
    open_df.columns = ticker_list  
    high_df.columns = ticker_list  
    low_df.columns = ticker_list  
    close_df.columns = ticker_list  
    volume_df.columns = ticker_list  
    vwap_df.columns = ticker_list  
    transactions_df.columns = ticker_list  
    otc_df.columns = ticker_list  
    
    return {'open':open_df, 'high': high_df, 'low': low_df, 'close': close_df, 
            'volume': volume_df, 'vwap': vwap_df, 
            'transactions':transactions_df, 'otc': otc_df} 
    
def read_combine_data(ticker_list,multiplier,freq,start_date,end_date):
    all_data = read_data(ticker_list, multiplier, freq, start_date, end_date)
    return combine_data(ticker_list, all_data)


if __name__ == "__main__":
    1+1
