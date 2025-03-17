#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  9 12:20:53 2024

@author: boow
"""

import logging
import pandas as pd
from options import parse_option_symbol 
import os
import pickle
import signal
import lz4.frame  # type: ignore
from data.common  import signal_handler
from data.options.get import get_options_data

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
signal.signal(signal.SIGINT, signal_handler)




def read_options_data(options_ticker, date, multiplier, freq):
    
    options_info = parse_option_symbol(options_ticker)
    
    stk_ticker = options_info['underlying_symbol']
    call_put = options_info['call_or_put']
    exp_date = options_info['expiry']
    strike_price = options_info['strike_price']
    
    directory = os.path.expanduser(f"~/Desktop/Market_Data/{stk_ticker}/options_data/exp-{exp_date}/{call_put}/{strike_price}/{multiplier}{freq}")
    filename = f"{options_ticker}-aggs-{date}-to-{exp_date}-freq-{multiplier}-{freq}.pickle.lz4"
    filepath = os.path.join(directory, filename)
    

    if not os.path.isfile(filepath):
        options_data = get_options_data((options_ticker, date, exp_date, multiplier, freq, True))
    else:    
        try:
            with open(filepath, "rb") as file:
                compressed_data = file.read()
                options_data = pickle.loads(lz4.frame.decompress(compressed_data))
        except FileNotFoundError:
            print(f"No file found for {options_ticker} at {date}")
        except Exception as e:
            print(f"An error occurred: {e} for {options_ticker}")

    if not options_data:
        return
    else:
        options_data = pd.DataFrame(options_data)
        options_data['timestamp'] = pd.to_datetime(options_data['timestamp'], unit='ms')
        options_data.set_index('timestamp', inplace=True)
        
        return options_data
