def trading_pair_data(pd,pair, db_connection):
    """This function extracts the trading pair information of any of the specified trading pairs
    
       input: string of the trading pair whose information is required.
       
       returns: base_currency, quote_currency,min_base_amnt,min_quote_amount ,active
    """
    
    trading_pair_data_query ="SELECT * FROM TRADING_PAIRS WHERE SYMBOL IN ('{pair}') ORDER BY TIMESTAMP DESC LIMIT 1".format(pair=pair)
    
    trading_pair_data_df = pd.read_sql(trading_pair_data_query,db_connection)
    
    base_currency =  trading_pair_data_df['BASE_CURRENCY'].iloc[0]
    quote_currency = trading_pair_data_df['QUOTE_CURRENCY'].iloc[0]
    min_base_amnt = trading_pair_data_df['MIN_BASE_AMOUNT'].iloc[0]
    min_quote_amnt = trading_pair_data_df['MIN_QUOTE_AMOUNT'].iloc[0]
    active = trading_pair_data_df['ACTIVE'].iloc[0]
    
    return base_currency,quote_currency,min_base_amnt,min_quote_amnt,active