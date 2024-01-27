import pyodbc
import pandas as pd

def azure_connect(driver = 'ODBC Driver 18 for SQL Server',your_user_name = 's234859@dtu.dk', your_password_here = '7dqPHZ6qaSt9', time = 6000):
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
        conn = pyodbc.connect(connection_string)

        # Create a cursor object
        cursor = conn.cursor()

        # Execute SQL queries
        cursor.execute("SELECT * FROM test")
        output = cursor.fetchall()
        cursor.close()   
        print('connection established')
        # Close the cursor and connection
        # conn.close()
        return conn

    except Exception as e:
        print(f"Error: {str(e)}")
        

def load_data(query="SELECT * FROM test", conn=None):
    '''
    Loads the data from the Azure SQL database into a pandas dataframe.
    
    parameters:
    query (str): The SQL query to execute.
    conn (pyodbc.Connection): The connection object to the Azure SQL database.
    
    returns: 
    A dataframe containing the data from the Azure SQL database.
    '''
    df = pd.read_sql(query, conn)
    return df

def insert_data(df, conn, table='test'):
    '''
    Inserts data into an Azure SQL database.
    
    parameters:
    df (pandas.DataFrame): The dataframe containing the data to insert.
    conn (pyodbc.Connection): The connection object to the Azure SQL database.
    table (str): The name of the table to insert the data into.

    returns: 
    None
    '''
    
    try:
        col = str(tuple(df.columns)).replace("'", "")
        
        if len(df) == 1: # if only one row
            row = tuple(df.iloc[0])

        else: # if multiple rows
            row = tuple(df.itertuples(index=False, name=None))
            row = str(row)[1:-1]
            
        # create query    
        query = f"INSERT INTO {table} {col} VALUES {row}"
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.commit()
        cursor.close()
        print('data inserted')
    except Exception as e:
        print(f"Error: {str(e)}")

def close_connection(conn):
    '''
    Closes the connection to the Azure SQL database.
    
    parameters:
    conn (pyodbc.Connection): The connection object to the Azure SQL database.

    returns: 
    None
    '''
    conn.close()

if __name__ == '__main__':
    # create connection and load data
    conn = azure_connect(time=6000)
    df = load_data(conn=conn)
    
    # Create new row
    new_row = [
        (125, 'Jens', 'Viet', 'kollegiebakken','Lyngby')      
    ]
    df_new = pd.DataFrame(new_row, columns=df.columns)
    insert_data(df_new, conn)
    
    # check if data is inserted
    df = load_data(conn=conn)
    print(df)
    
    # close connection
    close_connection(conn)
    
    