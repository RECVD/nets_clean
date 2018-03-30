import pandas as pd
import numpy as np
import re


class Checker:

    """Class to provide checks to Data Frames at various stages of wrangling.

    """

    def __init__(self, df):
        self.df = df
        self.check_index()
        self.check_first_last()
        self.check_sums()

    def check_index(self):
        """Check that index is unique."""
        if not self.df.index.is_unique:
            raise ValueError('Created BEH_ID index is not unique')
        print('Unique Index Check:  Passed')

    def check_first_last(self):
        """Check that FirstYear <= LastYear for all records"""
        year_mismatch = self.df[self.df.FirstYear > self.df.LastYear]
        if not year_mismatch.empty:
            raise ValueError('The following BEH_ID(s) contain FirstYear > LastYear:\n {}'.format(
                '\n'.join([str(x) for x in year_mismatch.index.values])))
        print('FirstYear <= LastYear Check: Passed')

    def check_sums(self):
        """Check that the sum of all years for a DunsNumber = the First FirstYear - the final LastYear"""
        # DunsNumber in the index makes this check much easier
        self.df.set_index('DunsNumber', append=True, inplace=True)

        # Sum of Year intervals = FirstYear - LastYear
        years_active_first_last = self.df.LastYear.groupby(level=1).apply(lambda x: x.iloc[-1]) - \
                                  self.df.FirstYear.groupby(level=1).apply(lambda x: x.iloc[0]) + 1
        beh_id_year_diff = (self.df.LastYear - self.df.FirstYear + 1).groupby(level=1).sum()

        # Reset index as it was before
        self.df.reset_index(level=0, drop=False, inplace=True)

        if not years_active_first_last.equals(beh_id_year_diff):
            raise ValueError("Some FirstYears are greater than LastYears")
        print("Year Sum Test: Passed")


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
        shift_year = lambda joined: joined.index.get_level_values('FirstYear').to_series().shift(-1) - 1
        lastyear = multi.groupby(level=0).apply(shift_year).combine_first(multi['LastYear']).rename('Lastyear')
        multi['LastYear'] = lastyear

        # Join multi fixes into the original df and remove the Change column
        joined.loc[multi.index] = multi
        joined['LastYear'] = joined['LastYear'].astype('int64')
        del joined['Change']

        return joined

    def create_locations(self, location_filename_1, location_filename_2, sep=','):
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
            df_99 = pd.read_csv(location_filename_1, index_col=['DunsNumber'], nrows=10**4, sep=sep)
            df_14 = pd.read_csv(location_filename_2, index_col=['DunsNumber'], nrows=10**4, sep=sep)
        except IOError as e:
            # File does not exist
            print("I/O Error: {}".format(e))
        except ValueError:
            # Index DunsNumber isn't found
            print("ValueError: Index DunsNumber not present")

        # Implementing full year changes for column names before melting, and joining into one DF
        df_99.columns = self.make_fullyear(df_99.columns)
        df_14.columns = self.make_fullyear(df_14.columns)
        df_loc = pd.concat([df_99, df_14], axis=1)
        # make citycode lowercase for formatting
        df_loc.columns = [col.upper() if 'CityCode' in col else col for col in df_loc.columns]

        # Lots of empty space, strip all str columns
        df_loc.loc[:, df_loc.select_dtypes('object').columns] =  \
            df_loc.loc[:, df_loc.select_dtypes('object').columns].apply(lambda x: x.str.strip())

        # reset index and melt to long format
        df_loc.reset_index(inplace=True)
        melt_cols = ['Address', 'City', 'State', 'ZIP', 'CITYCODE', 'FipsCounty', 'CBSA']
        df_loc_long = pd.wide_to_long(df_loc, melt_cols, i='DunsNumber', j='Year').sort_index().dropna(how='all')

        # change year dtype to int and normalize
        idx = df_loc_long.index
        df_loc_long.index = df_loc_long.index.set_levels([idx.levels[0], idx.levels[1].astype('int64')])
        normal = self.normalize_df(df_loc_long)
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

        return normal


def main():
    clean = Cleaner()
    location = clean.create_locations(r"C:\Users\jc4673\Documents\NETS\data\NETS2014_RAW\samples\NETS2014_AddressSpecial90to99"
        + "_sample.csv", r"C:\Users\jc4673\Documents\NETS\data\NETS2014_RAW\samples\NETS2014_AddressSpecial00to14_sample.csv")
    Checker(location)

if __name__ == "__main__":
    main()

