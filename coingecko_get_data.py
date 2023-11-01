#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import requests
import json
import pprint
import pickle
import time  
import pandas as pd


# Functions for collecting market cap and other global data


def get_coin_market_cap(page):
    
    """
    Function uses the coingecko api to get global data for tokens
    including market cap, volume etc.
    
    :param page: defines which 'page' of data will be extracted, each page
    contains data for 250 tokens in descending market cap order.
    
    :return: return global token data

    """
    
    # Define the URL for the API endpoint
    url = "https://api.coingecko.com/api/v3/coins/markets"

    # Define query parameters
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 250,
        'page': page,
        'sparkline': 'false',
        'locale': 'en'
    }
    # Make the GET request
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None


def get_coin_market_cap_for_pages(start_page, end_page):
    """
    Runs the get_coin_market_cap for a defined number of pages
    where each page contains 250 rows of global token data.
    
    :param start_page: The first page to collect data from.
    :param end_page: The last page

    :return: Returns data such as volume, market cap, fully dilluted market
    cap for top 1000 tokens by market cap.

    """
    all_data = []  # Initialize an empty list to store data for all pages

    for page in range(start_page, end_page + 1):
        page_data = get_coin_market_cap(page)  # Get data for a specific page
        if page_data:
            all_data.extend(page_data)  # Append data for the current page to the list
        time.sleep(30)  # Pause for N seconds before the next iteration

    return all_data


# Functions for getting depth and volume data for pairs on different exchanges


def get_depth(page, exchange):
    """
    Function to collect the depth along with other exchange based metrics
    using coingecko api.
    
    :param page: defines which 'page' of data will be extracted, each page
    contains data for 250 tokens in descending market cap order.
    :param exchange: defines which exchange the data will be collected from.
    
    :return: The depth, exchange volume, spread etc for the given exchange and
    page.

    """
    # Define the base URL
    base_url = "https://api.coingecko.com/api/v3/coins/tether/tickers"

    # Define query parameters
    params = {
        'exchange_ids': exchange,
        'include_exchange_logo': 'false',
        'page': page,
        'depth': 'true'   
    }

    # Make the GET request
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print("Request failed with status code:", response.status_code)
        return None


def get_depth_for_N_pages(exchange, start_page, end_page):
    """
    Function to collect the depth along with other exchange based metrics
    using coingecko api for the listed pages.
    
    :param exchange: The exchange from which to collect the data.
    :param start_page: The first page from which to gather data.
    :param end_page: The last page from which to gather data.

    :return: returns all of the exchange based data for USDT pairs on the
    listed exchange.

    """
    all_data = []  # Initialize an empty list to store data for all pages

    for page in range(start_page, end_page + 1):
        page_data = get_depth(page, exchange)  # Get data for a specific page
        page_data = page_data['tickers']
        if page_data:
            all_data.extend(page_data)  # Append data for the current page to the list
        time.sleep(30)  # Pause for N seconds before the next iteration
        print('page data gathered')

    return all_data


def get_coin_data(coin_list):
    """
    This function is used to get the data for the token-USD (not USDT) pairs 
    on Coinbase. Note that this function will run slowly.
    
    :param coin_list: A list of 'ID' for each coin we want to get the data for.
    :return: returns all of the exchange based data for USD pairs on
    coinbase.

    """
    coin_data_list = []  # Initialize an empty list to store the data for each coin
    
    for coin in coin_list:
        print(coin)
        url = f'https://api.coingecko.com/api/v3/coins/{coin}/tickers?exchange_ids=gdax&depth=true'
        try:
            response = requests.get(url)
            response.raise_for_status()  # Check for any HTTP errors

            data = response.json()
            coinbase_data = data['tickers']
            
            coin_data = next((item for item in coinbase_data if item.get('target') == 'USD'), None)
            
            coin_data_list.append(coin_data)  # Add the data for this coin to the list
            time.sleep(10)

        except requests.exceptions.RequestException as e:
            print(f"An error occurred for coin {coin}: {e}")

    return coin_data_list


# Using the functions to collect data, and dumping it to .pkl files for future use


# # market cap usage:
start_page = 1
end_page = 4
market_cap_top_1000 = get_coin_market_cap_for_pages(start_page, end_page)

with open("market_cap_top_1000.pkl", "wb") as file:
      pickle.dump(market_cap_top_1000, file)

print('market cap collected')


# Define the range of pages (100 tokens per page) to iterate through.
start_page = 1
end_page = 8

# Example usage:
exchange = "binance"
result_depth_binance = get_depth_for_N_pages(exchange, start_page, end_page)
with open("result_depth_binance.pkl", "wb") as file:
      pickle.dump(result_depth_binance, file)
print('binance depth data collected')


exchange = "bybit_spot"
result_depth_bybit = get_depth_for_N_pages(exchange, start_page, end_page)
with open("result_depth_bybit.pkl", "wb") as file:
      pickle.dump(result_depth_bybit, file)
print('bybit depth data collected')

exchange = "okex"
result_depth_OKX = get_depth_for_N_pages(exchange, start_page, end_page)
with open("result_depth_okx.pkl", "wb") as file:
      pickle.dump(result_depth_OKX, file)
print('okx depth data collected')

exchange = "kucoin"
result_depth_kucoin = get_depth_for_N_pages(exchange, start_page, end_page)
with open("result_depth_kucoin.pkl", "wb") as file:
      pickle.dump(result_depth_kucoin, file)
print('kucoin depth data collected')

exchange = "gate"
result_depth_gate = get_depth_for_N_pages(exchange, start_page, end_page)
with open("result_depth_gate.pkl", "wb") as file:
      pickle.dump(result_depth_gate, file)
print('gate depth data collected')


market_cap_df = pd.DataFrame(market_cap_top_1000)
list_of_coins = market_cap_df['id']
coinbase_data = get_coin_data(list_of_coins)
with open("coinbase_data.pkl", "wb") as file:
      pickle.dump(coinbase_data, file)
print('coinbase depth data collected')











