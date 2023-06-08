#!/usr/bin/env python
# coding: utf-8

# In[65]:


import pandas as pd
import numpy as np
import requests
from datetime import datetime as dt
import time
from valr_python import Client
from valr_python.exceptions import IncompleteOrderWarning
from decimal import Decimal
import requests
import warnings
warnings.filterwarnings("ignore")
import mysql.connector as connection
from sqlalchemy import create_engine
from functions.database_conn import database_conn
from functions.trading_pair import trading_pair
from functions.valr_client_auth import valr_client_auth
from functions.all_balances import all_balances
from functions.market_summary import market_summary
from functions.trading_pair_data import trading_pair_data
from functions.get_market_summary_data import get_market_summary_data
from functions.trade_record import trade_record
from functions.last_buy_price import last_buy_price


# In[66]:


#Establish connection with database
[db_connection,cursor,engine] = database_conn(connection,create_engine)


# In[67]:


#Establish connection with VALR client
c = valr_client_auth (Client,db_connection,pd)


# In[68]:


#extract market summary data
get_market_summary_df,whitelist_df = get_market_summary_data(pd,db_connection)


# In[69]:


#define a function to get updated balances after each trade
def new_balances(requests,pd,db_connection,dt,c,engine):
    """
    This function extracts the data on trading pairs and updates the TRADING_PAIRS Table in the database
    input:db_connection - This is the connection to the SQL database that has been established
    returns:num_rows_affected - The number of rows of data added to the database
    """
    
    all_balances_data = c.get_balances()
    count_balances= len(all_balances_data)

 
    CURRENCY=[]
    AVAILABLE_AMOUNT=[]

    for i in range(0,count_balances):

        currency = all_balances_data[i]['currency']
        available = round(float(all_balances_data[i]['available']),10)
      

        CURRENCY.append(currency)
        AVAILABLE_AMOUNT.append(available)
  
    account_balances_df =pd.DataFrame(list(zip(CURRENCY,AVAILABLE_AMOUNT)),\
                               columns = ['CURRENCY','AVAILABLE_AMOUNT'])

    return(account_balances_df)


# In[70]:


#Get the unique currency pairs to iterate over.
currency_pairs = get_market_summary_df['CURRENCY_PAIR'].unique().tolist()
order_count =0   #initialize the count of valid orders

#iterate over each currency and calculate the respective moving average
for pair in currency_pairs:
    
    ############################################################################################################
    #Define how far the moving average rolls back
    sma_roll = 180
    ema_roll = 420
    
    #Call the get pair information function
    base_currency,quote_currency,min_base_amnt,min_quote_amnt,active =\
    trading_pair_data(pd,pair,db_connection)
    
    #If there are sufficient funds to trade
    account_balances_df = new_balances(requests,pd,db_connection,dt,c,engine)
    
    #Get the average last buy price
    avg_last_buy_price = last_buy_price (pd,db_connection,dt,pair,np)
       
    #Filtered market summary data
    filtered_data = get_market_summary_df[get_market_summary_df['CURRENCY_PAIR'] == pair]
    
    #reorder the dataframe by timestamp so the moving averages aggregate properly
    filtered_data.sort_values(inplace = True, ascending =True , by =['TIMESTAMP'])
    
    #Calculate the simple moving average
    SMA = pd.Series(filtered_data['PREVIOUS_CLOSE_PRICE'].rolling(sma_roll).mean(), name = 'SMA') 
    SMA = SMA.dropna()
    filtered_data = filtered_data.join(SMA) 
    
    EMA = pd.Series(filtered_data['PREVIOUS_CLOSE_PRICE'].ewm(span = ema_roll, min_periods = ema_roll - 1).mean(), 
                 name = 'EWMA')
    
    filtered_data = filtered_data.join(EMA) 
    
    #check the trend by comparing the last trade price against the moving avg
    
    greater_than_sma = filtered_data.tail(1)['LAST_TRADE_PRICE'].iloc[0] > filtered_data.tail(1)['SMA'].iloc[0]
    greater_than_ema = filtered_data.tail(1)['LAST_TRADE_PRICE'].iloc[0] > filtered_data.tail(1)['EWMA'].iloc[0]
    
    ############################################################################################################  
    try:
        base_currency_amount =\
        account_balances_df[account_balances_df['CURRENCY']==base_currency]['AVAILABLE_AMOUNT'].iloc[0]
    except:
        base_currency_amount = 0
    
    try:
        quote_currency_amount =\
        account_balances_df[account_balances_df['CURRENCY']==quote_currency]['AVAILABLE_AMOUNT'].iloc[0]
    except:
        quote_currency_amount =0 
    
    ############################################################################################################
    #Check if buys and sells are allowed
    if (quote_currency_amount*0.20) >= min_quote_amnt:
        buy_allowed = 1
        buy_chunks = (quote_currency_amount/filtered_data.tail(1)['LAST_TRADE_PRICE'].iloc[0])*0.20
        
    elif (quote_currency_amount*0.40) >= min_quote_amnt:
        buy_allowed = 1
        buy_chunks = (quote_currency_amount/filtered_data.tail(1)['LAST_TRADE_PRICE'].iloc[0])*0.40
        
    elif (quote_currency_amount*0.60) >= min_quote_amnt:
        buy_allowed = 1
        buy_chunks = (quote_currency_amount/filtered_data.tail(1)['LAST_TRADE_PRICE'].iloc[0])*0.60
        
    elif (quote_currency_amount*0.80) >= min_quote_amnt:
        buy_allowed = 1
        buy_chunks = (quote_currency_amount/filtered_data.tail(1)['LAST_TRADE_PRICE'].iloc[0])*0.80
    
    elif (quote_currency_amount) >= min_quote_amnt:
        buy_allowed = 1
        buy_chunks = (quote_currency_amount/filtered_data.tail(1)['LAST_TRADE_PRICE'].iloc[0])
           
    else:
        buy_allowed =0
        buy_chunks =0
        
        
        
    if (base_currency_amount*0.2) >= min_base_amnt:
        sell_allowed = 1
        sell_chunks = (base_currency_amount)*0.20
        
    elif (base_currency_amount*0.4) >= min_base_amnt:
        sell_allowed = 1
        sell_chunks = (base_currency_amount)*0.4
        
    elif (base_currency_amount*0.6) >= min_base_amnt:
        sell_allowed = 1
        sell_chunks = (base_currency_amount)*0.6
    
    elif (base_currency_amount*0.8) >= min_base_amnt:
        sell_allowed = 1
        sell_chunks = (base_currency_amount)*0.8
        
    elif base_currency_amount >= min_base_amnt:
        sell_allowed = 1
        sell_chunks = base_currency_amount
        
            
    else:
        sell_allowed =0
        sell_chunks = 0
    
    ############################################################################################################
    
    #check for turning point to get lowest buy price and highest sell price
    #obtain the last 4 values of delta price change
    
    current_delta = filtered_data.tail(1)['CHANGE_FROM_PREVIOUS'].iloc[0]
    first_delta = filtered_data.tail(2)['CHANGE_FROM_PREVIOUS'].iloc[0]
    second_delta = filtered_data.tail(3)['CHANGE_FROM_PREVIOUS'].iloc[0]
    third_delta = filtered_data.tail(4)['CHANGE_FROM_PREVIOUS'].iloc[0]
    
    if ((current_delta>=0) & (current_delta > first_delta) & (current_delta > second_delta)):
        sustained_trend = 1 #sustained positve trend to buy
        
    elif ((current_delta <= 0) & (current_delta < first_delta) & (current_delta < second_delta)):
        sustained_trend = -1 #sustained neagtive trend to sell
    
    else:
        sustained_trend = 0 #no sustained trend
        
    if ((greater_than_sma == False) &(greater_than_ema == False) & (sell_allowed == 1) &\
        (filtered_data.tail(1)['LAST_TRADE_PRICE'].iloc[0] > avg_last_buy_price) & (sustained_trend == -1)):

        
        limit_order = {
                        "side":"SELL",
                        "quantity":sell_chunks,
                        "price":filtered_data.tail(1)['LAST_TRADE_PRICE'].iloc[0] ,
                        "pair": pair,
                        "post_only": False}
        
        #Execute trade
        try:
            res = c.post_limit_order(**limit_order)
            order_id = res['id'] 
            
            #Record transaction into DB
            trade_record(pd,engine,dt,limit_order,res)
            print('Sell trade for {pair} executed successfully'.format(pair=pair))
        
        
        except IncompleteOrderWarning as w:  # HTTP 202 Accepted handling for incomplete orders
            order_id = w.data['id']
            print(order_id)
        except Exception as e:
            print(e)
    
      
        
    elif ((greater_than_sma == True) &(greater_than_ema == True) & (buy_allowed == 1) & (sustained_trend == 1)):            
        
        limit_order = {
                        "side":"BUY",
                        "quantity":buy_chunks,
                        "price":filtered_data.tail(1)['LAST_TRADE_PRICE'].iloc[0] ,
                        "pair": pair,
                        "post_only": False}
        
        
        #Execute trade
        try:
            res = c.post_limit_order(**limit_order)
            order_id = res['id']  
            
            #Record transaction into DB
            trade_record(pd,engine,dt,limit_order,res)
        
            print('Buy trade for {pair} executed successfully'.format(pair=pair))
        
        except IncompleteOrderWarning as w:  # HTTP 202 Accepted handling for incomplete orders
            order_id = w.data['id']
            print(order_id)
        except Exception as e:
            print(e)
        
        
    else:
        sell_trade = 0
        buy_trade =0
        print('{time}:No Trades For {pair}'.format(pair=pair,time=dt.now()))
        
        
    
        
        
   


