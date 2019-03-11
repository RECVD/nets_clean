import numpy as np
import json
import pandas as pd


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
    cols = ['BEH_ID', 'Company', 'TradeName', 'BEH_SIC', 'Sales', 'Emp']
    dtypes = {'BEH_ID': 'int64', 'Company': 'object', 'TradeName': 'object', 'BEH_SIC': 'int64', 'Sales': 'float64',
              'Emp': 'float64'}
    df = pd.read_table(r"C:\Users\jc4673\Documents\Data\NETS2014_cat_loc_final.txt", usecols=cols, dtype=dtypes,
                       index_col='BEH_ID', chunksize=10**6)

    for chunk_num, chunk in enumerate(df):
        classy = Classifier(r"C:\Users\jc4673\Documents\NETS\config\json_config_2018_08_03.json", chunk)

        cat_names = [x for x in classy.all_config]
        df_final = pd.DataFrame(0, index=chunk.index, columns=cat_names)

        for name in cat_names:
            df_final[name] = classy.is_class_all(name)

        if chunk_num == 0:
            df_final.to_csv(r"C:\Users\jc4673\Documents\Data\NETS2014_Categories_FINAL_fix.csv")
            print(chunk_num)
        else:
            with open(r"C:\Users\jc4673\Documents\Data\NETS2014_Categories_FINAL_fix.csv", 'a') as f:
                df_final.to_csv(f, header=False)
                print(chunk_num)





