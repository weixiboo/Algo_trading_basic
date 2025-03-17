#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 11:00:17 2024

@author: boow
"""

import datetime
import concurrent.futures
import multiprocessing
import numpy as np
import pandas as pd
import os
import pickle
import psutil
import signal
import spyder_kernels.utils.iofuncs as iofuncs
import sys
import lz4.frame  # type: ignore

def signal_handler(sig, frame):
    print("You pressed Ctrl+C!")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)



# def get_aggs(ticker_date_freq_pair):
#     #freq can be second, minute, hour, day week, month, quater, year in strings
    
#     """Retrieve aggs for a given ticker and date"""
#     ticker, date, multiplier, freq, save_file = ticker_date_freq_pair
#     aggs = []
#     client = RESTClient(polygonKey)  # Uses POLYGON_API_KEY environment variable

#     for a in client.list_aggs(ticker,multiplier,freq,date,date,limit=100000):
#         aggs.append(a)
   
#     if save_file: 
#         directory = os.path.expanduser(f"~/Desktop/YOLO/Market_Data/{ticker}/{multiplier}{freq}")
#         if not os.path.exists(directory):
#             os.makedirs(directory)
        
#         filename = f"{ticker}-aggs-{date}-freq-{multiplier}-{freq}.pickle.lz4"
#         filepath = os.path.join(directory, filename)
    
    
#         with open(filepath, "wb") as file:
#             try:
#                 compressed_data = lz4.frame.compress(pickle.dumps(aggs))
#                 file.write(compressed_data)
#             except TypeError as e:
#                 print(f"Serialization Error: {e}")

#     return aggs
        

def weekdays_between(start_date, end_date):
    """Generate all weekdays between start_date and end_date"""
    day = start_date
    while day <= end_date:
        if day.weekday() < 5:  # 0-4 denotes Monday to Friday
            yield day
        day += datetime.timedelta(days=1)
        

# def get_data(tickers,multiplier,freq,start_date,end_date, save_file = True):

#     dates = list(weekdays_between(start_date, end_date))
#     # Generate a list of (ticker, date) pairs
#     ticker_date_freq_pairs = [(ticker, date, multiplier, freq,save_file) for ticker in tickers for date in dates]

#     # Use ThreadPoolExecutor to download data in parallel
#     with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
#         executor.map(get_aggs, ticker_date_freq_pairs)

def read_trades(ticker, date, multiplier ,freq, save_file = True):
    """Reads trades for a given ticker and date, then prints them."""

    #convert freq dict
    polygon_to_pandas = {"second":"S","minute":"T","hour": "H", "day":"D", 
                         "week": "W", "month":"M", "quater":"Q", "year":"Y"}
    
    
    # Construct the filename   
    directory = os.path.expanduser(f"~/Desktop/Jordan/Data/{ticker}/{multiplier}{freq}")
    filename = f"{ticker}-aggs-{date}-freq-{multiplier}-{freq}.pickle.lz4"
    filepath = os.path.join(directory, filename)
    

    if not os.path.isfile(filepath):
        print(f"No file found for {ticker} at {date}")
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
        
        #Creating a complete date range
        date_range = pd.date_range(start=trades.index.min(), 
        end=trades.index.max(), freq= f"{multiplier}{polygon_to_pandas[f'{freq}']}" )
        
        # Reindexing the DataFrame to include all dates in the range
        trades = trades.reindex(date_range)
        
        #print(trades)
        return trades.astype(pd.SparseDtype("float", np.nan))


def read_data_serial(ticker,multiplier,freq,start_date,end_date):
    #read data from polygon into dataframe for one sigle stock,multiplier,freq,start_date,end_date

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
    #Reads data parallel for a bunch of all stock  
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
    #Load the list of stock tickers 
    ticker_list = iofuncs.load_dictionary("ticker_list.spydata")[0]['tickers']
    
    #State the time we want to work with
    start_date = datetime.date(2024, 7,18)
    end_date = datetime.date(2024, 8, 2)
    
    #We are reading the data for list of stock, 1 hour for each row, from start date to end date
    all_data = read_data(ticker_list,1,'hour',start_date,end_date)
    b = combine_data(ticker_list,all_data)
    
    #Gives the open price of each stock.
    #Can replace 'open' with 'high', 'low', 'close', 'volume', 'vwap', 'transactions' and 'otc'
    #b['open'] 
    
    #I commented out the code that can't run without a datastreaming subscription
    #Remember to change the directory when reading the data (line 88)
