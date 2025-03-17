#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  2 09:53:37 2025

@author: boow
"""

import datetime
import concurrent.futures
import os
import pickle
from polygon import RESTClient
from polygonKey import polygonKey
import signal
import spyder_kernels.utils.iofuncs as iofuncs
import lz4.frame  # type: ignore
from ... import signal_handler
from ... import weekdays_between

signal.signal(signal.SIGINT, signal_handler)


def get_aggs(ticker_date_freq_pair):
    #freq can be second, minute, hour, day week, month, quater, year in strings
    
    """Retrieve aggs for a given ticker and date"""
    ticker, date, multiplier, freq, save_file = ticker_date_freq_pair
    aggs = []
    client = RESTClient(polygonKey)  # Uses POLYGON_API_KEY environment variable

    for a in client.list_aggs(ticker,multiplier,freq,date,date,limit=100000):
        aggs.append(a)
   
    if save_file: 
        directory = os.path.expanduser(f"~/Desktop/Market_Data/{ticker}/{multiplier}{freq}")
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        filename = f"{ticker}-aggs-{date}-freq-{multiplier}-{freq}.pickle.lz4"
        filepath = os.path.join(directory, filename)
    
    
        with open(filepath, "wb") as file:
            try:
                compressed_data = lz4.frame.compress(pickle.dumps(aggs))
                file.write(compressed_data)
                print(f"{ticker}-aggs-{date}-freq-{multiplier}-{freq}")
            except TypeError as e:
                print(f"Serialization Error: {e}")

    return aggs

def get_data(tickers,multiplier,freq,start_date,end_date, save_file = True):

    dates = list(weekdays_between(start_date, end_date))
    # Generate a list of (ticker, date) pairs
    ticker_date_freq_pairs = [(ticker, date, multiplier, freq,save_file) for ticker in tickers for date in dates]

    # Use ThreadPoolExecutor to download data in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        executor.map(get_aggs, ticker_date_freq_pairs)
        
def update_data():
    ticker_list = iofuncs.load_dictionary("ticker_list.spydata")[0]['tickers']
    directory = os.path.expanduser("~/Desktop/YOLO/Market_Data/")   
    filename = "stk_last_update.pickle.lz4"
    filepath = os.path.join(directory, filename)
    

    with open(filepath, "rb") as file:
        compressed_data = file.read()
        start_date = pickle.loads(lz4.frame.decompress(compressed_data))
        end_date = datetime.date.today()

       
    print("Getting 1 day data")
    get_data(ticker_list,1,"day",start_date,end_date)
    
    print("Getting 1 hour data")
    get_data(ticker_list,1,"hour",start_date,end_date)
    
    print("Getting 15 minute data")
    get_data(ticker_list,15,"minute",start_date,end_date)
    
    print("Getting 15 second data")
    get_data(ticker_list,15,"second",start_date,end_date)
    
    print("Getting 1 minute data")
    get_data(ticker_list,1,"minute",start_date,end_date)
    
    print("Getting 1 second data")
    get_data(ticker_list,1,"second",start_date,end_date)
    


    with open(filepath, "wb") as file:
        try:
            compressed_data = lz4.frame.compress(pickle.dumps(end_date))
            file.write(compressed_data)
        except TypeError as e:
            print(f"Serialization Error: {e}")

