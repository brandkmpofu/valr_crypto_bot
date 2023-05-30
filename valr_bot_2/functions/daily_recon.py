import pandas as pd
import numpy as np
from datetime import datetime as dt
import warnings
warnings.filterwarnings("ignore")
import mysql.connector as connection
from sqlalchemy import create_engine
from valr_python import Client
from valr_client_auth import valr_client_auth
from database_conn import database_conn
import json


#establish db connection
[db_connection,cursor,engine] = database_conn(connection,create_engine)

#Establish connection with VALR client
c = valr_client_auth (Client,db_connection,pd)


#generate the date today
date_today = dt.now().strftime("%Y-%m-%d")

cursor.execute('SELECT MAX(ab1.`TIMESTAMP`) INTO  @latest_timestamp FROM ACCOUNT_BALANCES ab1;')


accounting_query = "WITH INTERMEDIATE_TABLE AS (SELECT SUBSTR(ms.CURRENCY_PAIR,1,length(ms.CURRENCY_PAIR)-3) AS CURRENCY,\
ms.LAST_TRADE_PRICE FROM MARKET_SUMMARY ms WHERE ms.CURRENCY_PAIR LIKE '%ZAR' AND ms.`TIMESTAMP` IN \
(SELECT @latest_timestamp))\
SELECT CAST(ab.`TIMESTAMP` AS DATE) AS `DATE`,ab.CURRENCY,ab.AVAILABLE_AMOUNT,ab.RESERVED_AMOUNT, ab.TOTAL_BALANCE ,\
IFNULL (it.LAST_TRADE_PRICE,1) AS LAST_TRADE_PRICE ,\
ROUND((ab.AVAILABLE_AMOUNT*IFNULL(it.LAST_TRADE_PRICE,1)),2) AS AVAILABLE_AMOUNT_ZAR,\
ROUND((ab.RESERVED_AMOUNT*IFNULL(it.LAST_TRADE_PRICE,1)),2) AS RESERVED_AMOUNT_ZAR,\
ROUND((ab.TOTAL_BALANCE*IFNULL(it.LAST_TRADE_PRICE,1)),2) AS TOTAL_BALANCE_ZAR \
FROM ACCOUNT_BALANCES ab \
LEFT JOIN INTERMEDIATE_TABLE it ON (ab.CURRENCY = it.CURRENCY) WHERE ab.`TIMESTAMP` IN (SELECT @latest_timestamp)"

daily_recon = pd.read_sql(accounting_query,db_connection) 


daily_recon =daily_recon[['AVAILABLE_AMOUNT','AVAILABLE_AMOUNT_ZAR','CURRENCY',\
                          'DATE','LAST_TRADE_PRICE','RESERVED_AMOUNT_ZAR','TOTAL_BALANCE','TOTAL_BALANCE_ZAR']]
daily_recon.to_sql('DAILY_RECON',engine,if_exists='append',index=False,method='multi')




#####
#Determine the withdrawals and  deposits for the day
daily_prices = daily_recon[['DATE','CURRENCY','LAST_TRADE_PRICE']]


daily_prices = daily_prices.groupby(["DATE","CURRENCY"]).\
agg(LAST_TRADE_PRICE=pd.NamedAgg(column="LAST_TRADE_PRICE", aggfunc="mean"))

daily_prices = daily_prices.reset_index()


daily_prices = daily_prices[daily_prices['DATE'] == date_today]
daily_prices.drop("DATE",axis="columns",inplace=True)



all_transactions = c.get_transaction_history()

TRANSACTION_TYPE =[]
DATE =[]
CREDIT_CURRENCY = []
CREDIT_AMOUNT =[]


for i in range(0,160):

    transaction_type = all_transactions[i]['transactionType']['type']
    credit_currency = all_transactions[i]['creditCurrency']
    credit_amount = round(float(all_transactions[i]['creditValue']),2)
    date = all_transactions[i]['eventAt'][0:10]
    
    
    TRANSACTION_TYPE.append(transaction_type)
    DATE.append(date)
    CREDIT_CURRENCY.append(credit_currency)
    CREDIT_AMOUNT.append(credit_amount) 

transactions_df =pd.DataFrame(list(zip(TRANSACTION_TYPE,DATE,CREDIT_CURRENCY,CREDIT_AMOUNT)),\
                       columns = ['TRANSACTION_TYPE','DATE','CREDIT_CURRENCY','CREDIT_AMOUNT'])

#Keep only deposits and withdrawal transactions
deposit_withdrawal = transactions_df[transactions_df['TRANSACTION_TYPE'].\
                                     isin(['BLOCKCHAIN_RECEIVE','BLOCKCHAIN_SEND','INTERNAL_TRANSFER'])]

deposit_withdrawal = deposit_withdrawal[deposit_withdrawal['DATE'] == date_today]

deposit_withdrawal = deposit_withdrawal.merge(daily_prices,left_on = 'CREDIT_CURRENCY',right_on ='CURRENCY')

deposit_withdrawal['TOTAL_VALUE_ZAR'] = deposit_withdrawal['CREDIT_AMOUNT']*deposit_withdrawal['LAST_TRADE_PRICE']


#Write to databases
deposit_withdrawal.to_sql('DEPOSITS_WITHDRAWALS',engine,if_exists='append',index=False,method='multi')
