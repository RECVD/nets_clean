import pandas as pd
import json
import re
from pathlib import Path


#############################################################################
# Code to implement the fix if mains should be a combination of auxiliaries.#
#############################################################################

"""
Config Files:
1. Main category list (json)
2. Hierarchy list ordering (text)

Steps:
1. Read data in chunks without main categories and hierarchy
(No cols with an h, no main categories)
2. Re-apply the hierarchy as a security measure.
3. Re-create the main categories from hierachy categories
4. Write to file 
"""


def load_main_cat_config(filepath):
    """Load the config file for main categories and mod so that they're combos of hierarchy categories"""
    with open(filepath, 'r') as f:
        main_cats = json.load(f)

    # Mod contents to be combinations of hierarchies
    main_cats_transformed = {}
    for key in main_cats:
        main_cats_transformed["adr_net_{}_c_2014".format(key.lower())] = \
            ["adr_net_{}h_c_2014".format(x.lower()) for x in main_cats[key]]

    return main_cats_transformed


def load_hierarchy_list(filepath):
    """ Loads the hierachy list """
    with open(filepath, 'r') as f:
        hiear_list = ["adr_net_{}_c_2014".format(line.strip().lower()) for line in f.readlines()]

    return hiear_list


def get_code(var_name_long):
    """Gets the 3-letter hierarchy agnostic code from a full NETS variable name"""
    return var_name_long[8:11]


def get_hierarchy(row, hier_list):
    """Get the hierarchy list"""
    for col in hier_list:
        if row[col] == 1:
            # extract the 3 digit code from the total variable name
            code = get_code(col)
            return "adr_net_{}h_c_2014".format(code)


def get_good_columns(filepath, main_cats):
    """Get all columns for reading except main categories and hierarchy"""
    with open(filepath, 'r') as f:
        cols = f.readline().strip().split(',')

    # exclude hierarchy categories
    search = re.compile("_.{3}h_c_")
    cols = [x for x in cols if not re.search(search, x)]

    # exclude main categories
    cols = [x for x in cols if not any(y.lower() == x for y in main_cats.keys())]

    return cols


def set_hierarchy(chunk, hier_list):
    """Creates one-hot encoded hierarchy vars based on non-hierarchy vars in chunk"""
    # Only get hierarchies for classified businesses
    des_mask = chunk[hier_list].any(axis=1)
    des = chunk[des_mask]

    hierarchy = des.apply(get_hierarchy, axis=1, hier_list=hier_list) \
        .reindex(chunk.index)
    hierarchy_dummies = pd.get_dummies(hierarchy)

    # Add unrepresented codes if present
    if len(hierarchy_dummies.columns) != len(hier_list):
        missing_codes = \
            [get_code(x) for x in hier_list if not
            any(get_code(x) == get_code(y) for y in hierarchy_dummies.columns)]
        missing_vars = ["adr_net_{}h_c_2014".format(x) for x in missing_codes]
        for var in missing_vars:
            hierarchy_dummies[var] = 0

    # Sort column names for consistency
    hierarchy_dummies = hierarchy_dummies.reindex(sorted(hierarchy_dummies.columns), axis=1)

    return hierarchy_dummies


def set_main_cats(hierarchy_dummies, main_cats):
    """Add main categories to hierarchy dummies and return"""
    for main_cat, sub_cats in main_cats.items():
        hierarchy_dummies[main_cat] = hierarchy_dummies[sub_cats].sum(axis=1)

    return hierarchy_dummies


def write_file(df, writefile, first):
    """Write df to file.  Only write column names if first=True"""
    if first:
        with open(writefile, "w", newline="\n") as f:
            df.to_csv(f, index=False)
            print(".")

    else:
        with open(writefile, "a", newline="\n") as f:
            df.to_csv(f, index=False, header=False)
            print(".")


def main(data_path, write_path, main_cats_path, hier_list_path):
    """Implement the total fix:  Read, transform, write."""
    main_cats = load_main_cat_config(main_cats_path)
    hier_list = load_hierarchy_list(hier_list_path)

    good_cols = get_good_columns(data_path, main_cats)
    df = pd.read_csv(data_path, usecols=good_cols, chunksize=10**4)

    first = True
    for chunk in df:
        # split data into nets vs other
        nets_cols = [x for x in chunk.columns if 'net' in x]
        gis_cols = [x for x in chunk.columns if 'net' not in x]

        hierarchy = set_hierarchy(chunk, hier_list)
        hierarchy_main = set_main_cats(hierarchy, main_cats)

        final_chunk = pd.concat([chunk[nets_cols], hierarchy_main, chunk[gis_cols]], axis=1)

        write_file(final_chunk, write_path, first)
        first = False


if __name__ == "__main__":
    from pathlib import Path
    root = Path.cwd()

    data_path = root.parent.parent.parent / "data" / "recvd_net_vars_v7_20180829.csv"
    write_path = root.parent / "data" / "data_out" / "recvd_net_vars_v8_20190222.csv"
    main_cats_path = root.parent.parent / 'config' / 'supercategories.json'
    hier_list_path = root.parent.parent / 'config' /'hierarchy_080318_list.txt'

    main(data_path, write_path, main_cats_path, hier_list_path)
