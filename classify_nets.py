import numpy as np
import json
import pandas as pd


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def normal_to_long(df_normal, long_cols):
    """ Transforms a normalized dataframe into a long one, subsets to "long_cols"

    Requires a DF with FirstYear, LastYear, and BEHID
    """
    df_normal['YearsActive'] = df_normal['LastYear'] - df_normal['FirstYear'] + 1
    firstyear = df_normal['FirstYear'].tolist()
    lastyear = df_normal['LastYear'].tolist()
    year_nested = [range(x, y+1) for x,y in zip(firstyear, lastyear)]
    year = pd.Series([item for sublist in year_nested for item in sublist])

    long_index = df_normal.index.repeat(df_normal.YearsActive.tolist())
    long = df_normal[long_cols].reindex(long_index)
    long_MultiIndex = pd.MultiIndex.from_arrays([long.index.values, year.values])
    long.index = long_MultiIndex

    return long

def make_BEHID_wide(df_loc):
    """From the location file, creates a wide version of the BEH_LOC"""
    long_BEHID = normal_to_long(df_loc, 'DunsNumber')
    long_BEHID = long_BEHID.reset_index(level=0, drop=False) \
        .set_index('DunsNumber', append=True) \
        .swaplevel() \
        .rename(columns={'level_0': 'BEH_ID'})

    # correction for first/lastyear mistake in earlier iterations
    long_BEHID = long_BEHID.loc[~long_BEHID.index.duplicated()]

    wide_BEHID = long_BEHID.unstack()

    return wide_BEHID

class Classifier:
    """
    Class used in the classification of businesses based on SIC code, number of employees, annual sales,
    and company name based on the schema outlined in json_config
    """

    def __init__(self, config_file, df, delim=','):
        """Read JSON config file as global and reformat SIC ranges"""
        self.config_file = config_file
        self.delim = delim
        self.df = df
        self.df.loc[(self.df.TradeName.str.strip() == ""), 'TradeName'] = np.nan

        self.all_config = self.read_config_json(self.config_file)
        self.make_range()

    def read_config_json(self, config_filepath):
        """Reads JSON config file for classification into a dict and returns it
        -----------
        Keyword Arguments:
        config_filepath: Full file path to the JSON config file
        """
        with open(config_filepath) as f:
            config_dict = json.load(f)
        return config_dict

    def make_range(self):
        """SIC ranges in all_config are originally ranges in form such as [1,5,7,11]
        make_range splits them up into a list of tuples, such as [(1,5),(7,11)] where each tuple indicates an inclusive
        range.  Function to_zip(iterable) is nested, where iterable is a list with an even number of items.
        """

        def to_zip(iterable):
            # iterable = [int(i) for i in iterable if i]  #convert the iterable to int if it isn't empty str
            return tuple(zip(iterable[0::2], iterable[1::2]))

        for key, _ in self.all_config.items():
            try:
                self.all_config[key]['sic_range'] = to_zip(self.all_config[key]['sic_range'])
                self.all_config[key]['sic_range_2'] = to_zip(self.all_config[key]['sic_range_2'])
            except KeyError:
                continue


    def is_class_all(self, config_key):

        # Conditional and config variables
        local_config = self.all_config[config_key]
        condit_code = local_config['conditional']

        # populate final series with 0's to start
        final = pd.Series(0, index=self.df.index)

        # define all bools
        def sic_exclusive_bool():
            return self.df['BEH_SIC'].isin(local_config['sic_exclusive'])

        def sic_exclusive_2_bool():
            return self.df['BEH_SIC'].isin(local_config['sic_exclusive_2'])

        def sic_range_bool():
            match_range = pd.Series([False for _ in range(len(self.df))], index=self.df.index)
            for Range in local_config['sic_range']:
                match_range += ((self.df['BEH_SIC'] >= Range[0]) & (self.df['BEH_SIC'] <= Range[1]))
            return match_range

        def sic_range_2_bool():
            match_range_2 = pd.Series([False for _ in range(len(self.df))], index=self.df.index)
            for Range in local_config['sic_range_2']:
                match_range_2 += ((self.df['BEH_SIC'] >= Range[0]) & (self.df['BEH_SIC'] <= Range[1]))
            return match_range_2

        def name_bool(name_only=False):

            if name_only:
                return (
                self.df['TradeName'].str.contains('|'.join(local_config['name'])) | self.df['Company'].str.contains(
                    '|'.join(local_config['name'])))

            else:
                match = pd.Series([False for x in range(len(self.df))], index=self.df.index)

                sic_range_2 = sic_range_2_bool()
                sic_range = sic_range_bool()
                sic_exclusive = sic_exclusive_bool()

                tradename = self.df[sic_range_2_bool() & ~self.df['TradeName'].isnull()]
                tradename_bool = tradename['TradeName'].str.contains('|'.join(local_config['name']))
                match = tradename_bool.combine_first(match)

                company = self.df[(sic_range_bool() | sic_exclusive_bool()) & self.df['TradeName'].isnull()]
                company_bool = company['Company'].str.contains('|'.join(local_config['name']))
                match = company_bool.combine_first(match)

                return match

        def emp_bool():
            if local_config['emp'][0] == 'g':
                return self.df['Emp'] > int(local_config['emp'][1:])
            elif local_config['emp'][0] == 'l':
                return self.df['Emp'] < int(local_config['emp'][1:])
            else:
                emp_range = list(range(*[int(x) for x in local_config['emp'].split(',')]))
                emp_range[1] += 1
                return self.df['Emp'].isin(emp_range)

        def sales_bool():
            if local_config['sales'][0] == 'g':
                return self.df['Sales'] >= int(local_config['sales'][1:])
            elif local_config['sales'][0] == 'l':
                return self.df['Sales'] < int(local_config['sales'][1:])
            else:
                emp_range = range(*[int(x) for x in local_config['sales'].split(',')])
                return self.df['Sales'].isin(emp_range)

        def sales_notpresent():
            return self.df['Sales'].isnull()

        # Evaluate booleans and return based on the conditional code in the JSON file
        # just change return final to the end, its cleaner

        if condit_code == 2:
            final.loc[sic_exclusive_bool() | (sic_range_bool() & name_bool(name_only=True))] = 1
            return final

        elif condit_code == 3:
            final.loc[sic_range_bool()] = 1
            return final  # final

        elif condit_code == 4:
            final.loc[sic_exclusive_bool() | sic_range_bool()] = 1
            return final

        elif condit_code == 5:
            final.loc[sic_range_bool() & emp_bool()] = 1
            return final

        elif condit_code == 6:
            final.loc[sic_range_bool() & (sales_bool() | emp_bool())] = 1
            return final

        elif condit_code == 7:
            final.loc[sic_range_bool() | (sic_range_2_bool() & name_bool(name_only=True))] = 1
            return final

        elif condit_code == 8:
            final.loc[sic_exclusive_bool()] = 1
            return final

        elif condit_code == 9:
            final.loc[sic_exclusive_bool() | name_bool(name_only=True)] = 1
            return final

        elif condit_code == 10:
            final.loc[sic_range_bool() & emp_bool() & (sales_bool() | sales_notpresent())] = 1
            return final

        elif condit_code == 11:
            final.loc[sic_exclusive_bool() | ((sic_range_bool() | sic_exclusive_2_bool()) & name_bool(name_only=True))] = 1
            return final

        elif condit_code == 12:
            final.loc[name_bool()] = 1
            return final

        elif condit_code == 13:
            final.loc[sic_exclusive_bool() & emp_bool()] = 1
            return final

if __name__ == "__main__":
    import os
    from pathlib import Path
    from tkinter import filedialog, Tk

    root = Tk()
    root.withdraw()
    data_dir = Path(filedialog.askdirectory(initial=os.getcwd(),
                                            title='Select the root data folder'))

    sic = data_dir / 'raw' / 'NETS2014_SIC.txt'
    sic_cols = ['DunsNumber'] + ['SIC' + str(x)[-2:] for x in range(1990, 2015)]

    emp = data_dir / 'raw' / 'NETS2014_Emp.txt'
    emp_cols = ['DunsNumber'] + ['Emp' + str(x)[-2:] for x in range(1990, 2015)]

    sales = data_dir / 'raw' / 'NETS2014_Sales.txt'
    sales_cols = ['DunsNumber'] + ['Sales' + str(x)[-2:] for x in range(1990, 2015)]

    company = data_dir / 'raw' / 'NETS2014_Company.txt'
    company_cols = ['DunsNumber', 'Company', 'TradeName']

    # this will be changed eventually
    loc = data_dir / 'interim' / 'NETS2014_Locations.txt'
    loc_cols = ['BEH_ID', 'DunsNumber', 'FirstYear', 'LastYear']

    behid_wide = data_dir / 'interim' / 'NETS2014_BEHID_wide.csv'


    # cols = ['BEH_ID', 'Company', 'TradeName', 'BEH_SIC', 'Sales', 'Emp']
    # dtypes = {'BEH_ID': 'int64', 'Company': 'object', 'TradeName': 'object', 'BEH_SIC': 'int64', 'Sales': 'float64',
    #           'Emp': 'float64'}

    # prolly wanna specify dtypes later
    df_sic = pd.read_table(sic, usecols=sic_cols, index_col='BEH_ID', chunksize=10**3)
    df_emp = pd.read_table(emp, usecols=emp_cols, index_col='BEH_ID', chunksize=10**3)
    df_sales = pd.read_table(sales, usecols=sales_cols, index_col='BEH_ID', chunksize=10**3)
    df_company = pd.read_table(company, usecols=company_cols, index_col='BEH_ID', chunksize=10**3)

    # read loc_wide if it exists, create it if not
    try:
        df_behid_wide = pd.read_csv(behid_wide, index_col='BEH_ID', chunksize=10**6)
        print('Found wide BEHID file')

    except FileNotFoundError:
        print('Reading Location File')
        df_loc = pd.read_csv(loc, usecols=loc_cols, index_col=['DunsNumber', 'FirstYear'], sep=',')
        print('Creating index generator')
        duns_chunks = chunks(df_loc.index.unique(level='DunsNumber').tolist(), 10**6)
        # duns_chunks = chunks(sorted(list(set(df_loc.index.get_level_values(0)))), 10**6)
        print('Writing wide BEHID file')
        first = True
        for duns_chunk in duns_chunks:
            loc_chunk = df_loc.reindex(duns_chunk, level=0) \
                .reset_index(drop=False) \
                .set_index('BEH_ID')

            chunk_behid_wide = make_BEHID_wide(loc_chunk)
            if first:
                chunk_behid_wide.to_csv(behid_wide)
                first = False
                print('.')
            else:
                chunk_behid_wide.to_csv(behid_wide, mode='a', header=False)
                print('.')

        #now re-read it in chunks
        df_behid_wide = pd.read_csv(behid_wide, index_col='BEH_ID', chunksize=10**6)

    first = True  # switch to numbered chunks later
    for chunk in zip(df_sic):
        classy = Classifier(r"C:\Users\jc4673\Documents\NETS\config\json_config_2018_08_03.json", chunk)

        cat_names = [x for x in classy.all_config]
        df_final = pd.DataFrame(0, index=chunk.index, columns=cat_names)

        for name in cat_names:
            df_final[name] = classy.is_class_all(name)

        if first:
            df_final.to_csv(r"C:\Users\jc4673\Documents\Data\NETS2014_Categories_FINAL_fix.csv")
        else:
            with open(r"C:\Users\jc4673\Documents\Data\NETS2014_Categories_FINAL_fix.csv", 'a') as f:
                df_final.to_csv(f, header=False)





