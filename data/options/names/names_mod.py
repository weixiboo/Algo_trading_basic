#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  2 09:53:37 2025

@author: boow
"""

import datetime
import concurrent.futures
import logging
import pandas as pd
import os
import pickle
from polygon import RESTClient
from polygonKey import polygonKey
import signal
import spyder_kernels.utils.iofuncs as iofuncs
import lz4.frame  # type: ignore
from data.common import signal_handler
from data.common import mondays_between

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


signal.signal(signal.SIGINT, signal_handler)

def get_contract_names(ticker_date_freq_pair):
    #freq can be second, minute, hour, day week, month, quater, year in strings
    
    """Retrieve aggs for a given ticker and date"""
    ticker, date, save_file = ticker_date_freq_pair
    contract_names = []
    client = RESTClient(polygonKey)  # Uses POLYGON_API_KEY environment variable
    
    year = date.year
    month = date.month
    day = date.day
    
    for c in client.list_options_contracts(underlying_ticker = ticker,
                                           as_of= date,limit = 1000):
        contract_names.append(c) 
   
    if save_file: 
        directory = os.path.expanduser(f"~/Desktop/YOLO/Market_Data/{ticker}/contract_names/{year}/{month}")
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        filename = f"contract_names_{year}_{month}_{day}.pickle.lz4"
        filepath = os.path.join(directory, filename)
    
    
        with open(filepath, "wb") as file:
            try:
                compressed_data = lz4.frame.compress(pickle.dumps(contract_names))
                file.write(compressed_data)
            except TypeError as e:
                print(f"Serialization Error: {e}")
        
        print(f"Downloaded contract names for {ticker}: {year}-{month}-{day}")
        return contract_names
    else:            
        return contract_names

def get_all_contract_names(tickers,start_date,end_date, save_file = True):

    dates = list(mondays_between(start_date, end_date))
    # Generate a list of (ticker, date) pairs
    ticker_date_freq_pairs = [(ticker, date,save_file) for ticker in tickers for date in dates]

    # Use ThreadPoolExecutor to download data in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        executor.map(get_contract_names, ticker_date_freq_pairs)

def read_contract_names(ticker,year,month,day,save_file = False):
    
    directory = os.path.expanduser(f"~/Desktop/YOLO/Market_Data/{ticker}/contract_names/{year}/{month}")
    filename = f"contract_names_{year}_{month}_{day}.pickle.lz4"
    filepath = os.path.join(directory, filename)
    contract_names = []

    if not os.path.isfile(filepath):
        if save_file == False:
            contract_names = get_contract_names((ticker, datetime.date(year,month,day), save_file))
        else:
            contract_names = get_contract_names((ticker, datetime.date(year,month,day), True))
        
    if not contract_names:
        try:
            with open(filepath, "rb") as file:
                compressed_data = file.read()
                contract_names = pickle.loads(lz4.frame.decompress(compressed_data))
        except FileNotFoundError:
            print(f"No file found for {ticker} at {year} {month} {day}")
        except Exception as e:
            print(f"An error occurred: {e} for {ticker}")

    if not contract_names:
        return
    else:
        return pd.DataFrame(contract_names)

def update_options_name():
    ticker_list = iofuncs.load_dictionary("ticker_list.spydata")[0]['tickers']
    directory = os.path.expanduser("~/Desktop/YOLO/Market_Data/")   
    filename = "options_last_update.pickle.lz4"
    filepath = os.path.join(directory, filename)
    

    with open(filepath, "rb") as file:
        compressed_data = file.read()
        start_date = pickle.loads(lz4.frame.decompress(compressed_data))
        end_date = datetime.date.today()

       
    print("Getting Options Contract Names")
    get_all_contract_names(ticker_list,start_date,end_date,True)
    

    with open(filepath, "wb") as file:
        try:
            compressed_data = lz4.frame.compress(pickle.dumps(end_date))
            file.write(compressed_data)
        except TypeError as e:
            print(f"Serialization Error: {e}")

