#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  9 21:13:41 2024

@author: boow
"""
import datetime as dt
import re


def parse_option_symbol(option_symbol):
    # Remove "O:" prefix if it exists
    if option_symbol.startswith("O:"):
        option_symbol = option_symbol[2:]
    
    # Regular expression to parse the option symbol
    pattern = r"^(?P<underlying_symbol>[A-Za-z]+)" + \
              r"(?P<year>\d{2})(?P<month>\d{2})(?P<day>\d{2})" + \
              r"(?P<call_or_put>[CP])" + \
              r"(?P<strike_price>\d{8})$"

    # Match the symbol to the pattern
    match = re.match(pattern, option_symbol)
    
    if not match:
        raise ValueError(f"Invalid option symbol format: {option_symbol}")
    
    # Extract data from match groups
    data = match.groupdict()
    
    # Parse the data into a dictionary
    parsed_data = {
        'underlying_symbol': data['underlying_symbol'],
        'expiry': dt.date(2000 + int(data['year']), int(data['month']), int(data['day'])),
        'call_or_put': 'call' if data['call_or_put'] == 'C' else 'put',
        'strike_price': int(data['strike_price']) / 1000,
        'option_symbol': option_symbol
    }
    
    return parsed_data
