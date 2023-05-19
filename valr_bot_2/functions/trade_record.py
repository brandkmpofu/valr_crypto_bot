def trade_record (pd,engine,dt,limit_order,res):   
    """This function records successful trades into the database
    """
    
    TRADE_TYPE = limit_order['side']
    CURRENCY_PAIR = limit_order['pair']
    TRADE_STATUS = 'ACTIVE'
    ORDER_ID = res['id']
    QUANTITY = str(limit_order['quantity'])
    PRICE = str(limit_order['price'])
    



    trade_df =pd.DataFrame([[TRADE_TYPE,CURRENCY_PAIR,TRADE_STATUS,\
                                        ORDER_ID,QUANTITY,PRICE]],\
                               columns = ['TRADE_TYPE','CURRENCY_PAIR','TRADE_STATUS',\
                                        'ORDER_ID','QUANTITY','PRICE'])

    trade_df['POSTING_TIMESTAMP'] = dt.now()
    trade_df.to_sql('REQUESTED_TRADES',engine,if_exists='append',index=False,method='multi')