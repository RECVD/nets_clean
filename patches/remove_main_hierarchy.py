import os
import time

import re
import pandas as pd
import json
from pathlib import Path


#####################################################################################################
# Strips all main category hierarchy values from a csv file where they're present, and re-writes it #
# Does so for all csv files in the folder /data/data_in                                             #
#####################################################################################################


def get_csv_files(dir_path):
    """Returns a list of all the csv files in the dir at dir_path"""
    return [x for x in os.listdir(dir_path) if x.endswith(".csv")]


def get_bad_cols(json_cat):
    """ Return search terms for the categories to remove them later

    Keyword Arguments:
        json_cat: The supercategories json data as a dict containing the composition of super cats

    Returns:
        list containing xxxh_c_ and xxxh_d_ as search terms columns
    """
    cols_nested = [(x.lower() + "h_c_", x.lower() + "h_d_") for x in json_cat.keys()]
    cols_flat = [col for x in cols_nested for col in x]

    return cols_flat

def get_good_cols(filepath, bad_cols):
    """ Read the first line of a file and filter its columns to return none of the main hierarchy ones

    Keyword Arguments:
        filepath: Path to the file we want to alter

    Returns:
        List of columns we want
    """
    with open(filepath, "r") as f:
        all_cols = f.readline().strip().split(",")

    good_cols = [x for x in all_cols
                 if not any(bad_col in x for bad_col in bad_cols)]

    return good_cols


def get_filename_out(filename_in, extension=".csv"):
    """ Constructs a new filepath for the new file based on the old one

    Keyword Arguments:
        filepath_in: The path of the original file before modification

    Returns:
        A new filepath for writing with a updated date and version
    """
    today_date = time.strftime("%Y%m%d")

    # get the version number
    regex_search = "_v[0-9]+_"
    version_num_search = re.search(regex_search, filename_in)
    version_num = int(version_num_search.group(0)[2:-1])

    # cutoff point of the actual filename before versioning
    version_num_start = version_num_search.start()

    new_filename = filename_in[:version_num_start] + "_v{}_".format(version_num+1) + today_date + extension

    return new_filename


def read_rewrite(filepath_in, dirpath_out, bad_cols, chunksize=10**6):
    """ Reads the file at filepath and rewrites a new one to the same directory

    Keyword Arguments:
        filepath_in: Full path to the input file, type pathlib.Path
        dirpath_out: Path to the directory desired for output file, type pathlib.Path
        bad_cols: The columns we want to get rid of, list format

    Returns:
        None
    """

    # Set smaller chunks for wider datasets to conserve memory
    if "z10" in str(filepath_in) or "t10" in str(filepath_in):
        chunksize = 10**4

    good_cols = get_good_cols(filepath_in, bad_cols)
    filepath_out = dirpath_out / get_filename_out(filepath_in.name)
    df = pd.read_csv(filepath_in, usecols=good_cols, chunksize=chunksize)

    for i, chunk in enumerate(df):
        # write directly to the file if its the first chunk
        if i == 0:
            with open(filepath_out, "w", newline="\n") as f:
                chunk.to_csv(f, index=False)

        # append to it otherwise
        else:
            with open(filepath_out, "a", newline="\n") as f:
                chunk.to_csv(f, index=False, header=False)


def main(json_cat):
    # Get the paths to the files we want
    root = Path.cwd()
    data_in = root / "data" / "data_in"
    data_out = root / "data" / "data_out"

    all_csv = get_csv_files(data_in)
    bad_cols = get_bad_cols(json_cat)

    for csv_file in all_csv:
        read_rewrite(data_in / csv_file, data_out, bad_cols)
        print("{} Completed Writing\n".format(csv_file))


if __name__ == "__main__":

    with open(r"C:\Users\jc4673\Documents\Columbia\NETS\nets_clean\patches\supercategories.json", "r") as f:
        super_cat = json.load(f)

    main(super_cat)
