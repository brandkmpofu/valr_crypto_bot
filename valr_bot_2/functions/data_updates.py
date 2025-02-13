import pandas as pd
from datetime import datetime as dt
from valr_python import Client
import requests
import warnings
warnings.filterwarnings("ignore")
import mysql.connector as connection
from database_conn import database_conn
from valr_client_auth import valr_client_auth
from all_balances import all_balances
from market_summary import market_summary
from trading_pair import trading_pair
from sqlalchemy import create_engine

#establish db connection
[db_connection,cursor,engine] = database_conn(connection,create_engine)

#Connect to the vale client
c = valr_client_auth (Client,db_connection,pd)

#Update list of trading pairs
trading_pair(requests,pd,db_connection,c,engine,dt)

#Update balances list
all_balances(requests,pd,db_connection,dt,c,engine)

#update market summary
#market_summary(requests,pd,engine,dt,c,db_connection)

print('Job completed at {}'.format(dt.now()))
