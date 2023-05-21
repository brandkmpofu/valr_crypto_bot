#Check the average buy price for previous transactions
#This is to avoid selling at less than we bought for

def last_buy_price (pd,db_connection,dt,pair,np):   
    """This checks the last buy price of a pair
    """


    last_buy_query ="WITH intermediate AS (SELECT * FROM REQUESTED_TRADES RT \
    WHERE CURRENCY_PAIR IN ('{pair}') AND TRADE_TYPE = 'BUY' \
    ORDER BY RT.POSTING_TIMESTAMP DESC LIMIT 3) SELECT AVG(it.PRICE)  FROM intermediate it ".format(pair=pair)
    
    avg_last_buy_price = pd.read_sql(last_buy_query,db_connection)
    avg_last_buy_price['AVG(it.PRICE)'] = avg_last_buy_price['AVG(it.PRICE)'].astype(float)
    avg_last_buy_price = avg_last_buy_price['AVG(it.PRICE)'].iloc[0]

    return(avg_last_buy_price)