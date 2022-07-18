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
        join  architecturalstyletype using (architecturalstyletypeid)
        where	propertylandusetypeid = '261';
                """
    
    # Read in DataFrame from Codeup db.
    df = pd.read_sql(sql_query, get_db_url('zillow'))

    return df

def wrangle_zillow():

    df = pd.read_sql(sql_query, get_db_url('zillow'))
    df = df.astype({'fips': 'int64', 'yearbuilt': 'int64', 'bedroomcnt': 'int64'})
    return df

