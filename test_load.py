#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import requests
import pprint
import requests
import json
import pprint
import pickle
import pandas as pd
import numpy as np


#criteria:
#- all tokens between rank 50-800 on CG/CMC.
#- USDT pair on Binance, OKX, Kucoin and Bybit
#- USD pair on Coinbase
#- < 3k depth within +/- 2% 
#-volume of the pair
#-Fully Diluted Value of the token.


with open('market_cap_top_1000.pkl', 'rb') as file:
    data_market_cap = pickle.load(file)
data_market_cap = pd.DataFrame(data_market_cap)
columns_to_keep_market_cap = ['id', 'market_cap', 'total_volume', 'fully_diluted_valuation', 'market_cap_rank']
data_market_cap = data_market_cap[columns_to_keep_market_cap]


######
######

def load_and_process_exchange_data(filename, columns_to_keep, column_mapping, target_value):
    """
    This is a function to load the exchange specific data.
    
    :param filename: filename of the .pkl files generated in coingecko_get_data.
    :param columns_to_keep: Defines which columns are needed from the raw data.
    :param column_mapping: renames column names in the df to exchange
    specific names.
    :param target_value: defines the target of the pair as USDT (e.g. BTC-USDT).
    
    :return: Description of the return value, if applicable.

    """
    with open(filename, 'rb') as file:
        result = pickle.load(file)
    
    result_df = pd.DataFrame(result)
    result_df = result_df[columns_to_keep]
    result_df = result_df.rename(columns=column_mapping)
    result_df = result_df[result_df['target'] == target_value]
    
    return result_df

columns_to_keep = ['base','target','volume', 'cost_to_move_up_usd', 'cost_to_move_down_usd', 'bid_ask_spread_percentage', 'coin_id']
target_value = 'USDT'

# Define column mappings for each exchange
column_mapping_gate = {'volume': 'volume_gate', 'cost_to_move_up_usd': 'up_depth_gate', 'cost_to_move_down_usd': 'down_depth_gate', 'bid_ask_spread_percentage': 'spread_pct_gate', 'coin_id': 'id'}
column_mapping_binance = {'volume': 'volume_binance', 'cost_to_move_up_usd': 'up_depth_binance', 'cost_to_move_down_usd': 'down_depth_binance', 'bid_ask_spread_percentage': 'spread_pct_binance', 'coin_id': 'id'}
column_mapping_bybit = {'volume': 'volume_bybit', 'cost_to_move_up_usd': 'up_depth_bybit', 'cost_to_move_down_usd': 'down_depth_bybit', 'bid_ask_spread_percentage': 'spread_pct_bybit', 'coin_id': 'id'}
column_mapping_kucoin = {'volume': 'volume_kucoin', 'cost_to_move_up_usd': 'up_depth_kucoin', 'cost_to_move_down_usd': 'down_depth_kucoin', 'bid_ask_spread_percentage': 'spread_pct_kucoin', 'coin_id': 'id'}
column_mapping_okx = {'volume': 'volume_okx', 'cost_to_move_up_usd': 'up_depth_okx', 'cost_to_move_down_usd': 'down_depth_okx', 'bid_ask_spread_percentage': 'spread_pct_okx', 'coin_id': 'id'}

# Load and process data for each exchange
result_depth_gate = load_and_process_exchange_data('result_depth_gate.pkl', columns_to_keep, column_mapping_gate, target_value)
result_depth_binance = load_and_process_exchange_data('result_depth_binance.pkl', columns_to_keep, column_mapping_binance, target_value)
result_depth_bybit = load_and_process_exchange_data('result_depth_bybit.pkl', columns_to_keep, column_mapping_bybit, target_value)
result_depth_kucoin = load_and_process_exchange_data('result_depth_kucoin.pkl', columns_to_keep, column_mapping_kucoin, target_value)
result_depth_okx = load_and_process_exchange_data('result_depth_okx.pkl', columns_to_keep, column_mapping_okx, target_value)


######
######


with open('coinbase_data.pkl', 'rb') as file:
    result_depth_coinbase = pickle.load(file)
result_depth_coinbase = [d for d in result_depth_coinbase if d is not None]
result_depth_coinbase = pd.DataFrame(result_depth_coinbase)
result_depth_coinbase = result_depth_coinbase[columns_to_keep]
column_mapping_coinbase = {'volume': 'volume_coinbase', 'cost_to_move_up_usd': 'up_depth_coinbase', 'cost_to_move_down_usd': 'down_depth_coinbase', 'bid_ask_spread_percentage': 'spread_pct_coinbase', 'coin_id': 'id'}
result_depth_coinbase = result_depth_coinbase.rename(columns=column_mapping_coinbase)
result_depth_coinbase = result_depth_coinbase[result_depth_coinbase['target'] == 'USD']

# here we create the main dataframe main_df which contains the global data
# and also the data for each exchange. This can then be filtered to capture
# the data we need, i.e. tokens where the liquidity is low for the market cap

main_df = data_market_cap.copy()

dfs_to_merge = [result_depth_gate, result_depth_binance, result_depth_bybit,result_depth_okx,result_depth_kucoin,result_depth_coinbase]

for df in dfs_to_merge:
    main_df = pd.merge(main_df, df, on='id', how='left')

main_df = main_df.fillna(0)

filtered_df = main_df[(main_df['up_depth_binance'] < 3000) & (main_df['up_depth_binance'] != 0) |
                      (main_df['down_depth_binance'] < 3000) & (main_df['down_depth_binance'] != 0) |                     
                      (main_df['up_depth_bybit'] < 3000) & (main_df['up_depth_bybit'] != 0) |
                      (main_df['down_depth_bybit'] < 3000) & (main_df['down_depth_bybit'] != 0) |                      
                      (main_df['up_depth_okx'] < 3000) & (main_df['up_depth_okx'] != 0) |
                      (main_df['down_depth_okx'] < 3000) & (main_df['down_depth_okx'] != 0) |       
                      (main_df['up_depth_coinbase'] < 3000) & (main_df['up_depth_coinbase'] != 0) |
                      (main_df['down_depth_coinbase'] < 3000) & (main_df['down_depth_coinbase'] != 0) |                   
                      (main_df['up_depth_kucoin'] < 3000) & (main_df['up_depth_kucoin'] != 0) |
                      (main_df['down_depth_kucoin'] < 3000) & (main_df['down_depth_kucoin'] != 0) |                    
                      (main_df['up_depth_gate'] < 3000) & (main_df['up_depth_gate'] != 0) |
                      (main_df['down_depth_gate'] < 3000) & (main_df['down_depth_gate'] != 0)]

filtered_df = filtered_df.drop(columns=['base_x', 'base_y', 'target_x', 'target_y'])
filtered_df = filtered_df.replace(0, np.nan)


filtered_df.to_csv('tokens_with_poor_liquidity.csv', index=False)




