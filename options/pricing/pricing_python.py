#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 17:22:31 2025

@author: boow
"""
#from pricing_c import options_price 
import data
from .pricing_c import single_option_price
import pandas as pd
import psutil
import multiprocessing
import numpy as np

def options_price(stock_prices,K,iscall,DTE,r):
    return single_options_price(stock_prices,K,iscall,DTE,r)

