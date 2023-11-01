# Crypto_liquidity_analysis
Scripts to identify crypto tokens with poor liquidity but medium market cap

coingecko_get_data.py is used to gather data on crypto tokens using the coingecko API.
The script generates global data including market cap, volume, etc for the top 1000 tokens on all exchanges,
as well as collecting exchange specific information such as depth and spread percentage.

generate_poor_liquidity_dataset.py loads the dataframes generated using coingecko_get_data.py and manipulates
it to return a clean .csv file containing the required information about the liquidity of tokens across exchanges.
