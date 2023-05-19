def valr_client_auth (Client,db_connection,pd):

    """This function authenticates the connection to VALR.
    """
    
    api_secret_query = "select VARIABLE_VALUE from API_CONNECTION_PARAMS WHERE VARIABLE_NAME IN ('API_SECRET')"
    api_secret = pd.read_sql(api_secret_query,db_connection).iloc[0][0]
    
    api_key_query = "select VARIABLE_VALUE from API_CONNECTION_PARAMS WHERE VARIABLE_NAME IN ('API_KEY')"
    api_key = pd.read_sql(api_key_query,db_connection).iloc[0][0]
    
    
    c = Client(api_key=api_key,api_secret=api_secret)
    c.rate_limiting_support = True # honour HTTP 429 "Retry-After" header values
    
    return(c)