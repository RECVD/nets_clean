import time
import pandas as pd
import numpy as np
import re


class Checker:

    """Class to provide checks to Data Frames at various stages of wrangling.

    """

    def __init__(self, df):
        self.df = df

    def check_index(self):
        """Check that index is unique."""
        if not self.df.index.is_unique:
            raise ValueError('Created BEH_ID index is not unique')

    def check_first_last(self):
        """Check that FirstYear <= LastYear for all records"""
        year_mismatch = self.df[self.df.FirstYear > self.df.LastYear]
        if not year_mismatch.empty:
            raise ValueError('The following BEH_ID(s) contain FirstYear > LastYear:\n {}'.format(
                '\n'.join([str(x) for x in year_mismatch.index.values])))

    def check_sums(self):
        """Check that the sum of all years for a DunsNumber = the First FirstYear - the final LastYear"""
        # DunsNumber in the index makes this check much easier
        self.df.set_index('DunsNumber', append=True, inplace=True)

        # Sum of Year intervals = FirstYear - LastYear
        years_active_first_last = self.df.LastYear.groupby(level=1).apply(lambda x: x.iloc[-1]) - \
                                  self.df.FirstYear.groupby(level=1).apply(lambda x: x.iloc[0]) + 1
        beh_id_year_diff = (self.df.LastYear - self.df.FirstYear + 1).groupby(level=1).sum()

        # Reset index as it was before
        self.df.reset_index(level=1, drop=False, inplace=True)

        if not years_active_first_last.equals(beh_id_year_diff):
            raise ValueError("Some FirstYears are greater than LastYears")

    def check_all(self):
        """Perform all checks sequentially"""
        self.check_index()
        self.check_first_last()
        self.check_sums()


class Cleaner:

    """Class containing all functions for cleaning and helper functions

    """

    def __init__(self):
        pass

    def make_fullyear(self, column_list):
        """Function to redefine 'partial year', i.e 'Address99' into 'full year', i.e 'Address1999'.

        :param column_list: list of columns in a given data frame

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


    def normalize_df(self, df):
        """" Changes database from long form to normalized form, only including updates and removing redundant data

        :param df: DataFrame in the long format.  Should have MultiIndex (DunsNumber, Year)

        :return Normalized Data Frame
        """

        # Generate df of FirstYear and LastYear
        df.reset_index(inplace=True, drop=False)
        grouped = df[['DunsNumber', 'Year']].groupby('DunsNumber')
        firstyear = grouped.first().rename(columns={'Year': 'FirstYear'})
        lastyear = grouped.last().rename(columns={'Year': 'LastYear'})
        misc = pd.concat([firstyear, lastyear], axis=1)
        misc.set_index('FirstYear', append=True, inplace=True)
        df.set_index(['DunsNumber', 'Year'], inplace=True)

        # Identify whether location changed at all for any given business
        change = (df.groupby(level=0).nunique().sum(axis=1) /
                  df.shape[1] > 1).rename('Change').to_frame()
        df = change.join(df)

        # Drop duplicates, add DunsNumber as column to ensure no deletion between different businesses with identical rows
        df['DunsNumber'] = df.index.get_level_values(level=0)
        df = df.drop_duplicates()
        del df['DunsNumber']
        df.index.names = ['DunsNumber', 'FirstYear']  # formatting

        # Join FirstYear and LastYear in from misc, and fill values forward
        joined = misc.join(df, how='outer')
        joined['LastYear'] = joined['LastYear'].groupby(level=0).ffill()
        multi = joined[joined['Change'] == True]  # df with only businesses that had changes

        # Diagonal shift to fix years for multi businesses
        #shift_year = lambda joined: joined.index.get_level_values('FirstYear').to_series().shift(-1) - 1
        #lastyear = multi.groupby(level=0).apply(shift_year).combine_first(multi['LastYear']).rename('Lastyear')

        multi.reset_index(drop=False, inplace=True)
        lastyear = (multi[['DunsNumber', 'FirstYear']].groupby('DunsNumber').shift(-1) - 1).FirstYear.combine_first(
            multi['LastYear']).rename('Lastyear')
        multi['LastYear'] = lastyear
        multi.set_index(['DunsNumber', 'FirstYear'], inplace=True)

        # Join multi fixes into the original df and remove the Change column
        joined = multi.combine_first(joined)
        #joined.loc[multi.index] = multi
        joined['LastYear'] = joined['LastYear'].astype('int64')
        del joined['Change']

        return joined

    def create_locations(self, location_filename_1, location_filename_2, write_path, sep=',', chunksize=5*(10**5)):
        """ Creates a normalized location file for NETS data

        This file will be indexed by the BEH_ID, which is a combination of the DunsNumber and the BEH_LOC.  Other than
        that, it will contain only location information.  Columns will include:  Address, City, State, ZIP, CBSA Code.

        :param location_filename_1: First chronological filename for addresses
        :param location_filename_2: Second chronological filename for addresses.
        :param write_path: path to write finished location file too
        :param sep:  delimiter for reading.  Ex: ',' '\t'
        :param chunksize: size to write in.  Default is 10**5, may need to be adjusted based on the machine's memory

        :return: Normalized location of type pandas.DataFrame object
        """

        # Find which columns we need to read
        with open(location_filename_1, 'r') as f1, open(location_filename_2, 'r') as f2:
            cols_1 = f1.readline().strip().split(sep)
            cols_2 = f2.readline().strip().split(sep)
            usecols_1 = [x for x in cols_1 if 'CBSA' not in x]
            usecols_2 = [x for x in cols_2 if 'CBSA' not in x]

        # Need to do more error checking later on to try and break this
        try:
            df_99 = pd.read_csv(location_filename_1, index_col=['DunsNumber'], chunksize=chunksize, usecols=usecols_1,
                                sep=sep)
            df_14 = pd.read_csv(location_filename_2, index_col=['DunsNumber'], chunksize=chunksize, usecols=usecols_2,
                                sep=sep)
        except IOError as e:
            # File does not exist
            print("I/O Error: {}".format(e))
        except ValueError:
            # Index DunsNumber isn't found
            print("ValueError: Index DunsNumber not present")

        first = True  # Determine if this is our first chunk for writing headers
        for (chunk_99, chunk_14) in zip(df_99, df_14):
            # Implementing full year changes for column names before melting, and joining into one DF
            chunk_99.columns = self.make_fullyear(chunk_99.columns)
            chunk_14.columns = self.make_fullyear(chunk_14.columns)
            chunk_loc = pd.concat([chunk_99, chunk_14], axis=1)
            # make citycode lowercase for formatting
            chunk_loc.columns = [col.upper() if 'CityCode' in col else col for col in chunk_loc.columns]

            # Lots of empty space, strip all str columns
            chunk_loc.loc[:, chunk_loc.select_dtypes('object').columns] =  \
                chunk_loc.loc[:, chunk_loc.select_dtypes('object').columns].apply(lambda x: x.str.strip())

            # reset index and melt to long format
            chunk_loc.reset_index(inplace=True)
            melt_cols = ['Address', 'City', 'State', 'ZIP', 'CITYCODE', 'FipsCounty']
            chunk_loc_long = pd.wide_to_long(chunk_loc, melt_cols, i='DunsNumber', j='Year').sort_index().dropna(how='all')

            # change year dtype to int and normalize
            idx = chunk_loc_long.index
            chunk_loc_long.index = chunk_loc_long.index.set_levels([idx.levels[0], idx.levels[1].astype('int64')])
            normal = self.normalize_df(chunk_loc_long)
            #fill empty strings with NaN
            normal.replace('', np.nan, inplace=True)

            # change all possible dtypes to int
            for col in normal.columns:
                try:
                    normal[col] = normal[col].astype(int)
                except ValueError:
                    continue

            # Create BEH_ID and BEH_LOC
            normal.reset_index(drop=False, inplace=True)
            normal['BEH_LOC'] = normal.groupby('DunsNumber').cumcount(ascending=False)
            normal['BEH_ID'] = normal['BEH_LOC'] * (10 ** 9) + 10 ** 10 + normal['DunsNumber']
            normal.set_index('BEH_ID', inplace=True)

            # Check this chunk
            Checker(normal).check_all()
            if first:
                normal.to_csv(write_path, float_format='%.f')
                first = False
                print('.')

            else:
                normal.to_csv(write_path, mode='a', header=False, float_format='%.f')
                print('.')

def main():
    clean = Cleaner()
    clean.create_locations(r"C:\Users\jc4673\Documents\NETS\data\NETS2014_RAW\NETS2014_AddressSpecial90to99.txt",
                           r"C:\Users\jc4673\Documents\NETS\data\NETS2014_RAW\NETS2014_AddressSpecial00to14.txt",
                           r"C:\Users\jc4673\Documents\NETS\data\NETS2014_WRANGLED\NETS2014_locations.csv")

if __name__ == "__main__":
    time1 = time.time()
    main()
    print(time.time() - time1)

