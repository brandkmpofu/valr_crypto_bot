def get_market_summary_data(pd,db_connection):

    get_trading_whitelist_query = 'SELECT DISTINCT SYMBOL FROM TRADE_PAIR_WHITELIST WHERE ACTIVE =1'

    whitelist_df = pd.read_sql(get_trading_whitelist_query,db_connection)
    whitelist_df  = tuple(whitelist_df['SYMBOL'].tolist())
    rows_per_pair = 200


    #Load the market summary
    get_market_summary_query=\
   'SELECT * FROM MARKET_SUMMARY WHERE CURRENCY_PAIR IN {whitelist_df} ORDER BY TIMESTAMP DESC LIMIT {count_rows}'.\
    format(whitelist_df=whitelist_df,count_rows =rows_per_pair*len(whitelist_df) )

    get_market_summary_df = pd.read_sql(get_market_summary_query,db_connection)
    
    return(get_market_summary_df,whitelist_df)
