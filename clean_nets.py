import pandas as pd
import re

def make_fullyear(column_list):
    """Function to redefine 'partial year', i.e 'Address99' into 'full year', i.e 'Address1999'.

    :param column_list:

    :return List of columns with redefined years
    """
    final_columns = ['0'] * len(column_list)
    for i, col in enumerate(column_list):
        # Separate potential year from the rest of the column name
        year = col[-2:]
        col_name = col[:-2]
        if re.match('\d\d', year):  # If the year is digits
            # Figure out century by checking decade
            if year[0] == '9':
                col = '{}{}{}'.format(col_name, '19', year)
            else:
                col = '{}{}{}'.format(col_name, '20', year)
        final_columns[i] = col
    return final_columns


def create_locations(location_filename_1, location_filename_2, sep=','):
    """ Creates a normalized location file for NETS data

    This file will be indexed by the BEH_ID, which is a combination of the DunsNumber and the BEH_LOC.  Other than that,
    it will contain only location information.  Columns will include:  Address, City, State, ZIP, CBSA Code.

    :param location_filename_1: First chronological filename for addresses
    :param location_filename_2: Second chronological filename for addresses.
    :param sep:  delimiter for reading.  Ex: ',' '\t'

    :return: Normalized location of type pandas.DataFrame object
    """

    # Need to do more error checking later on to try and break this
    try:
        df_99 = pd.read_csv(location_filename_1, index_col=['DunsNumber'], nrows=10**3, sep=sep)
        df_14 = pd.read_csv(location_filename_2, index_col=['DunsNumber'], nrows=10**3, sep=sep)
    except IOError as e:
        # File does not exist
        print("I/O Error: {}".format(e))
    except ValueError:
        # Index DunsNumber isn't found
        print("ValueError: Index DunsNumber not present")

    # Implementing full year changes for column names before melting, and joining into one DF
    df_99.columns = make_fullyear(df_99.columns)
    df_14.columns = make_fullyear(df_14.columns)
    df_loc = pd.concat([df_99, df_14], axis=1)
    # make citycode lowercase for formatting
    df_loc.columns = [col.upper() if 'CityCode' in col else col for col in df_loc.columns]

    # Lots of empty space, strip all str columns
    df_loc.loc[:, df_loc.select_dtypes('object').columns] =  \
        df_loc.loc[:, df_loc.select_dtypes('object').columns].apply(lambda x: x.str.strip())

    # reset index and melt to long format
    df_loc.reset_index(inplace=True)
    melt_cols = ['Address', 'City', 'State', 'ZIP', 'CITYCODE', 'FipsCounty', 'CBSA']
    df_loc_long = pd.wide_to_long(df_loc, melt_cols, i='DunsNumber', j='Year')



    pass

if __name__ == "__main__":
    create_locations(r"C:\Users\jc4673\Documents\NETS\data\NETS2014_RAW\samples\NETS2014_AddressSpecial90to99_sample.csv",
                     r"C:\Users\jc4673\Documents\NETS\data\NETS2014_RAW\samples\NETS2014_AddressSpecial00to14_sample.csv")

