import requests
import pandas as pd
import numpy as np
import pickle
import os

## DAGI: Data load from dagi api
def get_DAGI(kommunekode):
    """
    Fetches data from the Dataforsyningen API for a given kommunekode.

    Parameters:
    kommunekode (int): The code of the kommune to fetch data for.

    Returns:
    dagi_df (DataFrame): A DataFrame containing the data fetched from the API.
    """
    url = f'https://api.dataforsyningen.dk/adgangsadresser?kommunekode={kommunekode}&struktur=mini'
    response = requests.get(url)
    data = response.json()
    dagi_df = pd.DataFrame(data)
    return dagi_df

## BBR: page search from BBR api
def search_bbr(page=1, pagesize=1000, kommunekode='0825'):

    api_url = "https://services.datafordeler.dk/BBR/BBRPublic/1/rest/enhed"

    params = {
        "username": "ANLVUSSNAP",
        "password": "",
        "Format": "JSON",
        "Kommunekode": kommunekode,
        "pagesize": pagesize,  # Specify the maximum page size
        "page": page  # Request the first page
    }

    response = requests.get(api_url, params=params)
    data = response.json()
    bbr_df = pd.DataFrame(data)
    return bbr_df

# BBR: get all data from one kommune
def get_bbr(code='0825', status=False):
    page = 1
    bbr_kommune = pd.DataFrame()

    while True:
        df = search_bbr(page=page, pagesize=1000, kommunekode=code)
        
        if bbr_kommune.empty:
            bbr_kommune = df
        else:
            bbr_kommune = pd.concat([bbr_kommune, df])
        
        if df.empty:
            if status: print(page)
            break
        else:
            if page % 5 == 0:
                if status: print(page)
            page += 1
            
    return bbr_kommune

def process_data(path, get_data_func,amount=None, dataframe=pd.DataFrame()):
    """
    Processes data from a given path using a specified data retrieval function.

    Parameters:
    path (str): The path to the pickle file where the DataFrame is stored.
    get_data_func (function): The function used to retrieve data. This function should take a code and return a DataFrame.

    Returns:
    all_data (DataFrame): The final DataFrame after all data has been retrieved and concatenated.
    """
    if os.path.exists(path):
        all_data = pd.read_pickle(path)
    else:
        all_data = pd.DataFrame()
        all_data.to_pickle(path)

    names = np.array(dataframe['kommune'])
    codes = np.array(dataframe['kode'])

    for index, code in enumerate(codes[:amount]):
        if all_data.empty:
            all_data = get_data_func(code)
            print(names[index])
        else:
            all_data = pd.concat([all_data, get_data_func(code)])
            print(names[index])
        
        print(f'{index+1} / {len(codes[:amount])}')    
        all_data.to_pickle(path)
        print('saved')

    return all_data

if __name__ == '__main__':
    # load kommune data
    kommune_df = pd.read_pickle('kommunekoder.pkl')
    
    # Get BBR data
    process_data('BBR.pkl', get_bbr, dataframe=kommune_df)
