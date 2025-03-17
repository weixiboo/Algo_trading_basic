from bisect import bisect_right
from collections import defaultdict
import datetime
import concurrent.futures
import logging
from options import parse_option_symbol 
import os
import pickle
from polygon import RESTClient
from polygonKey import polygonKey
import signal
import lz4.frame  # type: ignore
from data.common  import signal_handler
from data.common import first_day_of_months_between
from data.common  import mondays_between
from data.options.names  import read_contract_names

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
signal.signal(signal.SIGINT, signal_handler)

def get_options_data(ticker_date_freq_pair):
    #freq can be second, minute, hour, day week, month, quater, year in strings
    
    """Retrieve aggs for a given ticker and date"""
    options_ticker, start_date, end_date, multiplier, freq, save_file = ticker_date_freq_pair
    client = RESTClient(polygonKey)  # Uses POLYGON_API_KEY environment variable
    
    options_data = client.get_aggs(ticker = options_ticker, 
                                 multiplier = multiplier,
                                 timespan = freq,
                                 from_ = start_date,
                                 to = end_date)
        
    options_info = parse_option_symbol(options_ticker)
    
    stk_ticker = options_info['underlying_symbol']
    call_put = options_info['call_or_put']
    exp_date = options_info['expiry']
    strike_price = options_info['strike_price']
       
    if save_file: 
        directory = os.path.expanduser(f"~/Desktop/Market_Data/{stk_ticker}/options_data/exp-{exp_date}/{call_put}/{strike_price}/{multiplier}{freq}")
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        filename = f"{options_ticker}-aggs-{start_date}-to-{end_date}-freq-{multiplier}-{freq}.pickle.lz4"
        filepath = os.path.join(directory, filename)
    
    
        with open(filepath, "wb") as file:
            try:
                compressed_data = lz4.frame.compress(pickle.dumps(options_data))
                file.write(compressed_data)
            except TypeError as e:
                print(f"Serialization Error: {e}")
        
        print(f"Downloaded data for {options_ticker} {start_date} to {end_date}")
    else:            
        return options_data
        
def get_all_options_data(tickers,start_date,end_date,multiplier,freq,save_file = True):

    fst_day_of_month = list(first_day_of_months_between(start_date, end_date))
    mondays = list(mondays_between(start_date, end_date))
    
    mondays_dict = defaultdict(list)
    for monday in mondays:
        # Use (year, month) tuple as the key
        key = (monday.year, monday.month)
        mondays_dict[key].append(monday)
    
    for ticker in tickers:
        for day in fst_day_of_month:
            contract_names = read_contract_names(ticker, day.year, day.month)
            monday_list = mondays_dict.get((day.year,day.month),[])
            
            if contract_names is not None:
                options_tickers = []
                for monday in monday_list:
                    exp_date = bisect_right(contract_names['expiration_date'], monday.strftime("%Y-%m-%d"))
                    exp_date = contract_names['expiration_date'][exp_date] if exp_date < len(contract_names) else None
                    
                    if exp_date is None:
                        break
                    
                    if (datetime.datetime.strptime(exp_date,"%Y-%m-%d").date() - monday) <= datetime.timedelta(weeks = 1):
                        options_tickers.append(list(contract_names[contract_names['expiration_date'] == exp_date]['ticker']))
                    else:
                        options_tickers.append([])
                    
                options_req_tup = [(options_tickers[i][j], monday_list[i],monday_list[i]
                                    + datetime.timedelta(days = 4), multiplier, freq,save_file) 
                                   for i in range(len(options_tickers)) for j in range(len(options_tickers[i]))]
    
                with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
                    executor.map(get_options_data, options_req_tup)    

        
