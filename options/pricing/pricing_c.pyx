#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct  4 18:33:40 2024

@author: boow
"""
from cython.parallel cimport prange
cimport cython
from libc.math cimport exp
import numpy as np
cimport numpy as np
import pandas as pd

@cython.boundscheck(False)  # Disable bounds checking for performance
@cython.wraparound(False)   # Disable negative index checking for performance
def single_option_price(double[:,:] stock_prices, double K, bint iscall, long DTE,double r):    
    # Declare arrays and variables
    cdef int num_simulations = stock_prices.shape[0]
    cdef int num_steps = stock_prices.shape[1]
    cdef double option_payoffs
    cdef double discount
    cdef int i,j
    
    cdef np.ndarray[double, ndim=1] option_price

    assert DTE == num_steps
    
    
    option_price = np.empty((num_simulations), dtype=np.float64) 

    discount = exp(-r)
    
 
    if iscall:
        for i in range(num_simulations):
            # Calculate option payoffs at expiration
            option_payoffs = max(stock_prices[i, num_steps -1] - K, 0)
            for j in range(num_steps):
                # Discount the expected payoffs back to present value
                # Take the maximum of option price and intrinsic value
                option_payoffs = max(option_payoffs * discount,stock_prices[i,j] - K)
            
            option_price[i] = option_payoffs
    else:       
        for i in range(num_simulations):
            # Calculate option payoffs at expiration
            option_payoffs = max(K - stock_prices[i, num_steps -1], 0)
            for j in range(num_steps):
                # Discount the expected payoffs back to present value
                # Take the maximum of option price and intrinsic value
                option_payoffs = max(option_payoffs * discount,K - stock_prices[i,j]) 
            
            option_price[i] = option_payoffs
        
    return option_price



#Quality Check:
# import options_pricing as op
# A = np.random.normal(1,0.01,size = (100000,5))
# B = A*100
# op.single_option_price(B,100,True,0.0001)
# op.option_price(B,np.ones((60))*100,True,0.0001)
