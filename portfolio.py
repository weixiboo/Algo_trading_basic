
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  8 18:59:05 2024

@author: boow
"""

#Purpose: keep track of portfolio in a simulation or for real
#We are assuming that the ticker is doesn't change
import numpy as np
import pandas as pd

class portfolio:
    # Constructor method (initialize instance variables)
    def __init__(self,ticker_list,cash,time_list = None):
        
        #Take in ticker list and remove Date
        self.ticker_list = list(ticker_list)

        if 'Date' in self.ticker_list:
            self.ticker_list.remove('Date')
            
        #Intialize cash amount and parameters  
        self.cash = cash
        self.last_av_price = None
        self.not_enough_money = False
        
        #create dataframe for history
        if time_list is not None:
            self.history = pd.DataFrame(np.nan,index = time_list, 
                columns = ticker_list + ['Cash','Worth','Modeled ROI','Real ROI','Bench ROI'])
            
        #Create dataframe for long asset
        self.shares = sorted(self.ticker_list)
        self.shares = {'Ticker': self.ticker_list, 
                       'Quantity': 0.0, 'Price Bought': 0.0}
        self.shares = pd.DataFrame(self.shares)
        

        self.options = pd.DataFrame(
           {
               "option_ticker": pd.Series(dtype = 'str'),
               "ticker": pd.Series(dtype = 'str'),
               "option_type": pd.Series(dtype = 'str'),
               "strike_price": pd.Series(dtype = 'float'),
               "expiration_date": pd.Series(dtype = 'datetime64[ms]'),
               "quantity": pd.Series(dtype = 'int'),
               "premium": pd.Series(dtype = 'float'),
           }
       )
        
    def buy(self, ticker, quantity, price):
        assert (quantity > 0) & (price > 0)
        
        #If ticker is in dataframe we buy
        if ticker in self.shares['Ticker'].values and self.cash > quantity*price:
            
            #Record old quantity and price bought
            old_quantity = self.shares.loc[self.shares['Ticker'] == ticker, 'Quantity'].values
            old_price = self.shares.loc[self.shares['Ticker'] == ticker, 'Price Bought'].values
            
            #Update quantity and price bought
            self.shares.loc[self.shares['Ticker'] == ticker, 'Quantity'] += quantity
            self.shares.loc[self.shares['Ticker'] == ticker, 'Price Bought'] = (old_price
                    *old_quantity + price*quantity)/(old_quantity+quantity)
            #Update cash holding
            self.cash -= quantity*price
            
        elif self.cash > quantity*price:
            print("Ticker not in system")
        else:
            print("Not enough money")
            
    def sell(self, ticker, quantity, price):
        #Check current quantity
        current_quantity = self.shares.loc[self.shares['Ticker'] == ticker, 'Quantity'].values
        
        #Sell if enought quantity
        if ticker in self.shares['Ticker'].values and  current_quantity >= quantity:

            #Update quantity and cash
            self.shares.loc[self.shares['Ticker'] == ticker, 'Quantity'] -= quantity
            self.cash += quantity*price
            
            #Update price bought
            if current_quantity == quantity:
                self.shares.loc[self.shares['Ticker'] == ticker, 'Price Bought'] = 0.0
        else:
            print("Not enough shares to sell")
    
    def open_contract(self, ticker, option_type, strike_price, expiration_date, quantity, premium, option_ticker):
        assert premium > 0 
        
        if not self.option_exists(option_ticker) and self.cash > quantity*premium:
            
            # Concatenate 
            self.options = pd.concat([pd.DataFrame(
                [[option_ticker,ticker, option_type, strike_price,expiration_date, quantity, premium]],
                columns=self.options.columns), self.options], ignore_index=True)
            
            self.cash = self.cash - quantity*premium
            
        elif self.cash > quantity*premium:
            assert np.sign(self.options.loc[self.options['option_ticker'] == option_ticker, 'quantity'].values[0]) == np.sign(quantity)
            
            #New premium
            old_quantity = self.options.loc[self.options['option_ticker'] == option_ticker, 'quantity'].values[0]
            old_premium = self.options.loc[self.options['option_ticker'] == option_ticker, 'premium'].values[0]
            
            #Update quantity and price bought
            self.options.loc[self.options['option_ticker'] == option_ticker, 'quantity'] += quantity
            self.options.loc[self.options['option_ticker'] == option_ticker, 'premium'] = (old_premium
                    *old_quantity + premium*quantity)/(old_quantity+quantity)
            #Update cash holding
            self.cash -= quantity*premium

        else:
            print("Not enough money")


    def close_contract(self, ticker, option_type, strike_price, expiration_date, quantity, premium, option_ticker):
        #To close 1 long position, enter 1 as quantity.
        #To close 1 short position, enter -1 as quantity.
        
        if not self.option_exists(option_ticker):
            print("Cannot close what is not opened")
        else:
            existing_quantity = self.options.loc[self.options['option_ticker'] == option_ticker, 'quantity'].values[0]
            assert premium > 0 and np.sign(existing_quantity) == np.sign(quantity) and np.abs(existing_quantity) >= np.abs(quantity)
            
            if quantity < 0 and self.cash < np.abs(quantity)*premium:
                print("Not enough money to close position")
                return
            
            if existing_quantity == quantity:
               self.options = self.options[self.options['option_ticker'] != option_ticker]
            else:           
                #Subtract quantity to the existing quantity
                self.options.loc[self.options['option_ticker'] == option_ticker, 'quantity'] -= quantity
            
            self.cash = self.cash + quantity*premium


    def option_exists(self, option_ticker):
        return self.options['option_ticker'].isin([option_ticker]).any() 

  
        
            
    def total_worth(self,current_price):
        
        if self.last_av_price is None:
            self.last_av_price = current_price.sparse.to_dense().copy()
        else:
            self.last_av_price.loc[current_price.index[current_price.notna()]] \
                = current_price.loc[current_price.index[current_price.notna()]]
        
        #Return total worth base on current price
        num_shares = pd.Series(self.shares['Quantity'].values, index = self.shares['Ticker'])
        num_shares = num_shares.loc[num_shares>0]
                
        temp_price = self.last_av_price.loc[num_shares.index[num_shares>0]].copy()
        nan_ind = temp_price.index[temp_price.isna()]
        
        if not not list(nan_ind):
            nan_price = pd.Series(self.shares['Price Bought'].values, index = self.shares['Ticker'])
            temp_price[nan_ind] = nan_price[nan_ind].values
            self.last_av_price[nan_ind] = nan_price[nan_ind].values
        
        return np.dot(temp_price[num_shares.index],num_shares) + self.cash

    def num_shares_owned(self):
        #Return number of shares owned
        return pd.Series(self.shares['Quantity'].values, index = self.shares['Ticker'])
        
    def record_history(self,ind,current_price,model_ROI):
        #Update Stock Quantity
        num_shares = self.num_shares_owned()
        self.history.loc[ind,num_shares.index] = num_shares
        
        #Update Total Cash
        self.history.loc[ind,'Cash'] = self.cash
        
        #Update Worth
        self.history.loc[ind,'Worth'] = self.total_worth(current_price)
        
        #Update Total Cash
        self.history.loc[ind,'Modeled ROI'] = model_ROI
        
        #Calculate Real ROI
        if ind != self.history.index.min():
            self.history.loc[ind,'Real ROI'] = self.history.loc[ind,'Worth']\
                /self.history.loc[self.history.index[self.history.index.get_loc(ind)-1],'Worth']        

    def to_num_share(self,x,current_price):
        #x is the percentage allocation should be 1d
        
        #Calculate total worth and convert it to number of shares
        total_worth = self.total_worth(current_price)
        num_share = x*total_worth
        num_share = np.floor(num_share/current_price)
        
        #calculate the real portfolio allocation percentage
        x_discrete = num_share*current_price/total_worth
        
        return x_discrete,num_share
    

        
        
        
        
        
        