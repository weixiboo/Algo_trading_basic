#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  2 09:48:15 2025

@author: boow
"""
import datetime
import sys

def signal_handler(sig, frame):
    print("You pressed Ctrl+C!")
    sys.exit(0)
    
def weekdays_between(start_date, end_date):
    """Generate all weekdays between start_date and end_date"""
    day = start_date
    while day <= end_date:
        if day.weekday() < 5:  # 0-4 denotes Monday to Friday
            yield day
        day += datetime.timedelta(days=1)
        
def first_day_of_months_between(start_date, end_date):
    month = start_date.month
    year = start_date.year
    
    day = datetime.date(year,month,1)
    
    while day <= end_date:
        yield day
        year = year + month // 12
        month = month % 12 + 1
        day = datetime.date(year,month,1)
        
def mondays_between(start_date, end_date):
    """Generate all mondays between start_date and end_date"""
    day = start_date
    
    if day.weekday() != 0:
        day -= datetime.timedelta(days = day.weekday()) 
        day += datetime.timedelta(days = 7)
    while day < end_date:
        if day.weekday() == 0:  # 0-4 denotes Monday to Friday
            yield day
        day += datetime.timedelta(days=7)