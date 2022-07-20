import pandas as pd
import numpy as np
import os


# Acquire Zillow data
import env

from env import host, user, password

def get_db_url(db):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

def new_familyhome_data():
    '''
    This function reads the zillow data from the Codeup db into a df.
    '''
    sql_query = """
        SELECT bedroomcnt, bathroomcnt, calculatedfinishedsquarefeet, taxvaluedollarcnt, yearbuilt, taxamount, fips
        from properties_2017
        join propertylandusetype using (propertylandusetypeid)
        
        where	propertylandusetypeid = '261';
                """
    
    # Read in DataFrame from Codeup db.
    df = pd.read_sql(sql_query, get_db_url('zillow'))

    return df

def wrangle_zillow():

    df = new_familyhome_data()
    df = df.dropna()
    df = df.astype({'fips': 'int64', 'yearbuilt': 'int64', 'bedroomcnt': 'int64'})
    df = df[df.bathroomcnt <= 6]
    df = df[df.bedroomcnt <= 6]
    df = df[df.taxvaluedollarcnt < 2_000_000]
    return df

