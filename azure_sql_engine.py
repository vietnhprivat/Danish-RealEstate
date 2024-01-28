import pyodbc
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, event
import urllib

def azure_connect(driver = 'ODBC Driver 18 for SQL Server',your_user_name = 's234859@dtu.dk', your_password_here = None, time = 6000):
    ''' 
    Establishes a connection to an Azure SQL database and executes a SELECT query.

    Parameters:
    driver (str): The ODBC driver to use for the connection.
    your_user_name (str): The username for the Azure SQL database.
    your_password_here (str): The password for the Azure SQL database.
    time (int): The connection timeout in seconds.

    Returns:
    conn (pyodbc.Connection): The connection object to the Azure SQL database.
    '''
    # Connection string
    connection_string = f'Driver={driver};Server=tcp:s234859.database.windows.net,1433;Database=bolig;Uid={your_user_name};Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout={time};Authentication=ActiveDirectoryInteractive'
    
    try:
        # Connect to the database
        local_conn = pyodbc.connect(connection_string)
        local_engine = create_engine('mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(connection_string))

        # Create a cursor object
        cursor = local_conn.cursor()

        # Execute SQL queries
        cursor.execute("SELECT * FROM test")
        cursor.fetchall()
        cursor.close()   
        print('connection established')
        return local_engine, local_conn

    except Exception as e:
        print(f"Error: {str(e)}")

def execute_query(conn, query):
    '''
    Executes a query on the Azure SQL database.

    Parameters:
    conn (pyodbc.Connection): The connection object to the Azure SQL database.
    query (str): The query to execute on the Azure SQL database.

    Returns:
    None
    '''
    # Create a cursor object
    cursor = conn.cursor()

    # Execute SQL queries
    cursor.execute(query)
    output = cursor.fetchall()
    cursor.close()   
    return output

def commit_query(conn, query):
    '''
    Executes a query on the Azure SQL database.

    Parameters:
    conn (pyodbc.Connection): The connection object to the Azure SQL database.
    query (str): The query to execute on the Azure SQL database.

    Returns:
    None
    '''
    # Create a cursor object
    cursor = conn.cursor()

    # Execute SQL queries
    cursor.execute(query)
    conn.commit()
    cursor.close()
        

def close_connection(conn, engine):
    '''
    Closes the connection to the Azure SQL database.
    
    parameters:
    conn (pyodbc.Connection): The connection object to the Azure SQL database.

    returns: 
    None
    '''
    engine.dispose()
    conn.close()
    print('connection closed')

if __name__ == '__main__':
    # create connection
    engine, conn = azure_connect(time=100_000)
    
    # Enable fast_executemany
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True    
    
    # Load data from pickle
    df_load = pd.read_pickle('BBR.pkl')
    df_new = df_load.fillna(value = np.nan)
    print(len(df_new))
    print('data loaded')

    # Use the engine with to_sql
    # df_new.to_sql('DAGI', engine, if_exists='replace', index=False, chunksize=100, method='multi')
    df_new.to_sql('BBR', engine, if_exists='replace', index=False)
    print('data uploaded')
    
    # close connection
    close_connection(conn, engine)
    
    