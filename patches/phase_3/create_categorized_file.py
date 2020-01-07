import pandas as pd

def create_categorized_file(data_filename_in, data_filename_out, hierarchy_cols):
    df = pd.read_csv(data_filename_in, chunksize=10**6)

    first = True
    i = 1
    for chunk in df:

        cat_bool = chunk[hierarchy_cols].any(axis=1)
        chunk_cat = chunk[cat_bool]

        if first:
            with open(data_filename_out, "w", newline="\n") as f:
                chunk_cat.to_csv(f, index=False, encoding='utf-8')
            first = False
            print(i)
            i += 1
        else:
            with open(data_filename_out, "a", newline="\n") as f:
                chunk_cat.to_csv(f, index=False, header=False, encoding='utf-8')
            print(i)
            i += 1


if __name__ == "__main__":
    from pathlib import Path

    data_path = Path.cwd().parent / 'data'

    data_filename_in = \
        r"C:\Users\jc4673\Documents\Columbia\NETS\nets_clean\patches\data\data_out\recvd_net_vars_v8_20190311.csv"
    data_filename_out = \
        r"C:\Users\jc4673\Documents\Columbia\NETS\nets_clean\patches\data\data_out\recvd_net_vars_v8_20190314_catOnly1.csv"

    with open(config_filepath / 'aux_hier_vars.txt', 'r') as f:
        aux_hier = [line.strip() for line in f.readlines()]

    create_categorized_file(data_filename_in, data_filename_out, aux_hier)
