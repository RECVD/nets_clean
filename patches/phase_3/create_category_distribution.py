import pandas as pd

def create_value_counts_file(data_filename_in, counts_prob_out, hier_columns):
    df = pd.read_csv(data_filename_in, usecols=hier_columns, chunksize=10**6)

    df_sums = pd.Series(0, index=hier_columns)
    for i, chunk in enumerate(df):
        df_sums += chunk.sum(axis=0)
        print(i)

    total_sum = df_sums.sum()
    prob_vec = (total_sum - df_sums) / (total_sum - df_sums).sum()
    prob_vec.to_csv(counts_prob_out)

if __name__ == "__main__":

    filename_in = r"C:\Users\jc4673\Documents\Columbia\NETS\nets_clean\patches\data\data_out\recvd_net_vars_v8_20190314_catOnly.csv"
    counts_prob_out =r"C:\Users\jc4673\Documents\Columbia\NETS\nets_clean\patches\data\data_intermediate\category_prob_dist.csv "

    with open(config_filepath / 'aux_hier_vars.txt', 'r') as f:
        aux_hier = [line.strip() for line in f.readlines()]

    create_value_counts_file(filename_in, counts_prob_out, aux_hier)
