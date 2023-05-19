def trading_pair(requests,pd,db_connection,c,engine,dt):
    """
    This function extracts the data on trading pairs and updates the TRADING_PAIRS Table in the database
    input:db_connection - This is the connection to the SQL database that has been established
    returns:num_rows_affected - The number of rows of data added to the database
    """
    
    pairs_url_query = "select VARIABLE_VALUE from API_CONNECTION_PARAMS WHERE VARIABLE_NAME IN ('TRADING_PAIRS_URL')"
    pairs_url = pd.read_sql(pairs_url_query,db_connection).iloc[0][0]
    trading_pairs_data = requests.request("GET", pairs_url).json()
    count_trading_pairs= len(trading_pairs_data)

    SYMBOL=[]
    BASE_CURRENCY=[]
    QUOTE_CURRENCY=[]
    ACTIVE=[]
    MIN_BASE_AMOUNT =[]
    MAX_BASE_AMOUNT =[]
    MIN_QUOTE_AMOUNT =[]
    MAX_QUOTE_AMOUNT =[]
    MARGIN_TRADING_ALLOWED = []

    for i in range(0,count_trading_pairs):
        symbol = trading_pairs_data[i]['symbol']
        base_currency = trading_pairs_data[i]['baseCurrency']
        quote_currency = trading_pairs_data[i]['quoteCurrency']
        active = trading_pairs_data[i]['active']
        min_base_amount = round(float(trading_pairs_data[i]['minBaseAmount']),10)
        max_base_amount = round(float(trading_pairs_data[i]['maxBaseAmount']),10)
        min_quote_amount = round(float(trading_pairs_data[i]['minQuoteAmount']),10)
        max_quote_amount = round(float(trading_pairs_data[i]['maxQuoteAmount']),10)
        trading_margin = trading_pairs_data[i]['marginTradingAllowed']
    
        SYMBOL.append(symbol)
        BASE_CURRENCY.append(base_currency)
        QUOTE_CURRENCY.append(quote_currency)
        ACTIVE.append(active)
        MIN_BASE_AMOUNT.append(min_base_amount)
        MAX_BASE_AMOUNT.append(max_base_amount)
        MIN_QUOTE_AMOUNT.append(min_quote_amount)
        MAX_QUOTE_AMOUNT.append(max_quote_amount)
        MARGIN_TRADING_ALLOWED.append(trading_margin)
    

    trading_pairs_df =pd.DataFrame(list(zip(SYMBOL,BASE_CURRENCY,QUOTE_CURRENCY,ACTIVE,\
                                        MIN_BASE_AMOUNT,MAX_BASE_AMOUNT,MIN_QUOTE_AMOUNT,MAX_QUOTE_AMOUNT,\
                                       MARGIN_TRADING_ALLOWED)),\
                               columns = ['SYMBOL','BASE_CURRENCY','QUOTE_CURRENCY','ACTIVE',\
                                        'MIN_BASE_AMOUNT','MAX_BASE_AMOUNT','MIN_QUOTE_AMOUNT','MAX_QUOTE_AMOUNT',\
                                       'MARGIN_TRADING_ALLOWED'])
    
    trading_pairs_df['TIMESTAMP'] = dt.now()
    
    num_rows_affected= trading_pairs_df.\
    to_sql('TRADING_PAIRS',engine,if_exists='append',index=False,method='multi')
    
    return(num_rows_affected)