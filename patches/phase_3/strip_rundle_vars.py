# Strips away the Rundle walkability variables from the final phase 3 data.
# This is a patch to avoid re-running phase-3.py but will be made obsolete moving forward when it
# is incorporated into that file.

import pandas as pd


def get_non_rundle_columns(data_filename_in):
    with open(data_filename_in, 'r') as f:
        all_columns = f.readline().strip().split(',')
    rundle_codes = ["adl", "adp", "edu", "med", "pav", "pwd", "des"]
    rundle_columns = ["adr_net_{}_c_2014".format(x) for x in rundle_codes]
    non_rundle_columns = [x for x in all_columns if x not in rundle_columns]
    return non_rundle_columns


def strip_rundle_vars(data_filename_in, data_filename_out, non_rundle_columns):
    df = pd.read_csv(data_filename_in, usecols=non_rundle_columns, chunksize=10**6)

    first = True
    i = 1
    for chunk in df:
        if first:
            with open(data_filename_out, "w", newline="\n", encoding='utf-8') as f:
                chunk.to_csv(f, index=False)
            first = False
            print(i)
            i += 1
        else:
            with open(data_filename_out, "a", newline="\n", encoding='utf-8') as f:
                chunk.to_csv(f, index=False, header=False)
            print(i)
            i += 1


if __name__ == "__main__":
    data_filename_in = \
        r"C:\Users\jc4673\Documents\Columbia\NETS\nets_clean\patches\data\data_out\recvd_net_vars_v8_20190306.csv"
    data_filename_out = \
        r"C:\Users\jc4673\Documents\Columbia\NETS\nets_clean\patches\data\data_out\recvd_net_vars_v8_20190311.csv"
    non_rundle_columns = get_non_rundle_columns(data_filename_in)
    strip_rundle_vars(data_filename_in, data_filename_out, non_rundle_columns)


