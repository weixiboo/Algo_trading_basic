#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#Purpose: backtest 
#We are assuming that the ticker is doesn't change

from alive_progress import alive_bar, config_handler
import datetime
import market_class as mc
import numpy as np
import pandas as pd
import portfolio as pft
from scipy.stats import gmean
import matplotlib.pyplot as plt

config_handler.set_global(length = 20,max_cols = 120,
                          spinner = 'classic', bar = 'classic2',
                          monitor = '[{count}/{total}]',stats = '[eta: {eta}]',
                          elapsed = False, receipt = False)

class back_test:

    def __init__(self,start_date,end_date):
        self.start_date = start_date
        self.end_date = end_date
        
        #initialize market class: it provides data 
        self.market = mc.market(self.start_date,self.end_date)
        
        #intialize parameters
        
        #The list of time we are working with
        self.times = self.market.market_day_data['open'].index
        

    
    def run_sim(self,look_back,disp = True, do_analysis = False):
        #Run the simulation to see how we do with historical data
        #Look back is how much historical data we are looking at
        #We do this to ensure we are not looking into the future
        
        #[day 2 look back] <- [day 1 look back] <- [today]

        #initialize portfolio: it records portfolio
        self.portfolio = pft.portfolio(self.market.ticker_list, 3e3, self.times[look_back:-1])
        
        #Main backtest loop
        with alive_bar(len(self.times) - look_back -1, disable = not disp) as bar:
            for i in np.arange(look_back,len(self.times)-1):
                #current time
                current_time = self.times[i]
                
                #Calculate new order to buy #Note the buying time is the "next day"
                modeled_ROI, new_order = self.strategy(self.times[i-look_back]
                                        ,current_time,self.times[i+1])
    
                
                #Place trade #current price is vwap for now
                self.trader(new_order,self.market.current_price(self.times[i+1]))
                
                #Record worth
                self.portfolio.record_history(current_time,
                        self.market.current_price(current_time),modeled_ROI)
                
                
                current_worth = np.round(self.portfolio.history.loc[current_time,'Worth'],2)
                current_modeled_ROI = np.round(self.portfolio.history.loc[current_time,'Modeled ROI'],2)
                current_real_ROI = np.round(self.portfolio.history.loc[current_time,'Real ROI'],2)
                
                #Print total worth
                bar.text = (f'-- Asset: {current_worth}'
                            f' -- Model ROI: {current_modeled_ROI}'
                            f' -- Real ROI:  {current_real_ROI}')
                bar()
                
            
            if disp:
                print('Final Asset: ',current_worth,'--',
                      'look_back:', look_back,'--', '\n')
            
        self.portfolio.history['Bench ROI'] = self.market.bench_return(self.times[look_back], self.times[-1])
        
        if do_analysis:
            self.analysis(self.portfolio.history)
        
        return current_worth#self.portfolio.history
   
    def strategy(self,start_time,current_time,buying_time):
        #Gives new portfolio
        
        #x is percentage of money allcated for each stock
        #Place holder for now
        x = 1/len(self.market.ticker_list)
        #f is the predicted return based on the 
        f = lambda a: 1.1
        
        #Convert allocation percentage to number of shares       
        x_discrete,num_share = self.portfolio.to_num_share(x, self.market.current_price(buying_time))
        
        #Check if profitable
        if f(x_discrete) > 1:
            return f(x_discrete),num_share
        else:
            return 1, pd.Series(0,index = x.index)
    
    def trader(self,new_order,current_price):
        
        #Gets current shares and figure out how much to buy and sell
        shares_owned = self.portfolio.num_shares_owned()
        orders = new_order - shares_owned

        #Figure out the index of stocks to buy/sell
        buy_ind = np.where(orders>0)[0]
        sell_ind = np.where(orders<0)[0]
        
        #portfolio takes in positive shares
        orders = np.abs(orders)
        
        #Buying and selling
        for i in sell_ind:
            self.portfolio.sell(orders.index[i], orders.iloc[i], current_price[orders.index[i]])
        for i in buy_ind:
            self.portfolio.buy(orders.index[i], orders.iloc[i], current_price[orders.index[i]])
            
    def analysis(self,history):
        
        worth = np.array(history['Worth'][1:])
        real_return = np.array(history['Real ROI'][1:])
        modeled_return = np.array(history['Modeled ROI'][:-1])
        bench_return = np.array(history['Bench ROI'][1:])
        
        #Looking at the geometric mean of each day
        print('Real return')
        print('--Geom mean:',gmean(real_return),'--Std: ',np.std(real_return),'\n')
        
        print('Modeled return')
        print('--Geom mean:',gmean(modeled_return),'--Std: ',np.std(modeled_return))
        print('Corr modeled vs real: ',np.corrcoef(modeled_return,real_return)[0,1],'\n')
        
        #Benchmark is the S&P 500
        print('Bench return')
        print('--Geom mean:',gmean(bench_return),'--Std: ',np.std(bench_return))
        print('Corr bench vs real: ',np.corrcoef(bench_return,real_return)[0,1],'\n')
        

        plt.scatter(modeled_return,real_return)
        plt.title('Modeled return vs real return')
        plt.xlabel('Modeled return')
        plt.ylabel('Real return')
        plt.show()
        
        plt.hist(real_return,alpha = 0.5); plt.hist(modeled_return,alpha = 0.5)
        plt.title('Historgram of real return and modeled return')
        plt.show()
        
        
        
        plt.scatter(bench_return,real_return)
        plt.title('Bench return vs real return')
        plt.xlabel('Bench return')
        plt.ylabel('Real return')
        plt.show()
        
        plt.hist(real_return,alpha = 0.5,bins= 50); plt.hist(bench_return,alpha = 0.5 ,bins=50)
        plt.title('Historgram of real return and bench return')
        plt.show()
        
        plt.plot(np.log(worth))
        plt.plot(np.log(np.cumprod(np.concatenate(([1.0],bench_return)))*worth[0]))
        plt.title('Return before comission')
        plt.xlabel('Time')
        plt.ylabel('Worth')
        plt.show()
        
        plt.plot(np.log(np.cumprod(np.concatenate(([1.0],real_return*0.998)))*worth[0]))
        plt.plot(np.log(np.cumprod(np.concatenate(([1.0],bench_return)))*worth[0]))
        plt.title('Return after comission')
        plt.xlabel('Time')
        plt.ylabel('Worth')
        plt.show()
        

        

        
if __name__ == "__main__":
    #Example of how to use it
    #Remember to change the directory when reading the data (stk_data.py: line 88)
    
    start_date = datetime.date(2024, 1, 31)
    end_date = datetime.date(2024, 8, 3)
    B = back_test(start_date,end_date)
    a = B.run_sim(2,True,True)
        
