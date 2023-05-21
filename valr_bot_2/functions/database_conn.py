def database_conn(connection,create_engine):
    """
    This function establishes the connection to the MySQL database.
    
    inputs:
    returns:
    db_connection- connection to the database
    """
    port = '3306' #change depending on the database used
    user = ''
    host = ""
    password = "" #to be hidden
    
    # Connecting from the server
    db_connection = connection.connect(host=host,user=user,port=port,\
                                       passwd=password,use_pure=True,database= "valr_bot_db")
    cursor = db_connection.cursor()
    
    engine = create_engine("mysql+mysqldb://brand:Brandhunter1994@localhost/valr_bot_db")
    
    return(db_connection,cursor,engine)
