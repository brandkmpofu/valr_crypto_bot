def all_balances(requests,pd,db_connection,dt,c,engine):
    """
    This function extracts the data on trading pairs and updates the TRADING_PAIRS Table in the database
    input:db_connection - This is the connection to the SQL database that has been established
    returns:num_rows_affected - The number of rows of data added to the database
    """
    
    all_balances_data = c.get_balances()
    count_balances= len(all_balances_data)

    UPDATE_TIMESTAMP=[]
    CURRENCY=[]
    AVAILABLE_AMOUNT=[]
    TOTAL_BALANCE=[]
    RESERVED_AMOUNT=[]
    LEND_RESERVED =[]
    BORROW_RESERVED =[]
    BORROWED_AMOUNT =[]
 

    for i in range(0,count_balances):

        currency = all_balances_data[i]['currency']
        available = round(float(all_balances_data[i]['available']),10)
        total = round(float(all_balances_data[i]['total']),10)
        reserved = round(float(all_balances_data[i]['reserved']),10)
        lend_reserved = round(float(all_balances_data[i]['lendReserved']),10)
        borrowed_reserved = round(float(all_balances_data[i]['borrowReserved']),10)
        borrowed_amount = round(float(all_balances_data[i]['borrowedAmount']),10)
        

        CURRENCY.append(currency)
        AVAILABLE_AMOUNT.append(available)
        TOTAL_BALANCE.append(total)
        RESERVED_AMOUNT.append(reserved)
        LEND_RESERVED.append(lend_reserved)
        BORROW_RESERVED.append(borrowed_reserved)
        BORROWED_AMOUNT.append(borrowed_amount)
        

    all_balances_df =pd.DataFrame(list(zip(CURRENCY,AVAILABLE_AMOUNT,RESERVED_AMOUNT,\
                                        LEND_RESERVED,BORROW_RESERVED,BORROWED_AMOUNT,TOTAL_BALANCE)),\
                               columns = ['CURRENCY','AVAILABLE_AMOUNT','RESERVED_AMOUNT',\
                                        'LEND_RESERVED','BORROW_RESERVED','BORROWED_AMOUNT','TOTAL_BALANCE'])
    
    all_balances_df = all_balances_df[all_balances_df['TOTAL_BALANCE'] > 0]
    all_balances_df['TIMESTAMP'] = dt.now()
    
    num_rows_affected= all_balances_df.\
    to_sql('ACCOUNT_BALANCES',engine,if_exists='append',index=False,method='multi')
    
    return(num_rows_affected)