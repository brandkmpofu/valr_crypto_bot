def market_summary(requests,pd,engine,dt,c):
    """
    This function extracts the data on the market summary.This is limited to pairs with Fiat
    input:db_connection - This is the connection to the SQL database that has been established
    returns:num_rows_affected - The number of rows of data added to the database
    """
    
    
    
    market_summary_data = c.get_market_summary()
    market_summary_count= len(market_summary_data)

    CURRENCY_PAIR=[]
    LAST_TRADE_PRICE=[]
    PREVIOUS_CLOSE_PRICE=[]
    HIGH_PRICE=[]
    LOW_PRICE =[]
    CHANGE_FROM_PREVIOUS =[]

 

    for i in range(0,market_summary_count):

        currency_pair = market_summary_data[i]['currencyPair']
        last_trade_price = round(float(market_summary_data[i]['lastTradedPrice']),10)
        previous_close_price = round(float(market_summary_data[i]['previousClosePrice']),10)
        high_price = round(float(market_summary_data[i]['highPrice']),10)
        low_price = round(float(market_summary_data[i]['lowPrice']),10)
        change_from_previous = round(float(market_summary_data[i]['changeFromPrevious']),10)
   
        

        CURRENCY_PAIR.append(currency_pair)
        LAST_TRADE_PRICE.append(last_trade_price)
        PREVIOUS_CLOSE_PRICE.append(previous_close_price)
        HIGH_PRICE.append(high_price)
        LOW_PRICE.append(low_price)
        CHANGE_FROM_PREVIOUS.append(change_from_previous)

        

    market_summary_df =pd.DataFrame(list(zip(CURRENCY_PAIR,LAST_TRADE_PRICE,PREVIOUS_CLOSE_PRICE,\
                                        HIGH_PRICE,LOW_PRICE,CHANGE_FROM_PREVIOUS)),\
                               columns = ['CURRENCY_PAIR','LAST_TRADE_PRICE','PREVIOUS_CLOSE_PRICE',\
                                        'HIGH_PRICE','LOW_PRICE','CHANGE_FROM_PREVIOUS'])
    
    market_summary_df = \
    market_summary_df.query('CURRENCY_PAIR.str.contains("ZAR") or CURRENCY_PAIR.str.contains("USDC")')
    market_summary_df['TIMESTAMP'] = dt.now()
    
    num_rows_affected= market_summary_df.\
    to_sql('MARKET_SUMMARY',engine,if_exists='append',index=False,method='multi')
    
    return(num_rows_affected)
