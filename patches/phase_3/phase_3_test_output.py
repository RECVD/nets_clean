import numpy as np

def test_output_aux_hierarchy_unique(df_output, aux_hier):
    output_hier = df_output[aux_hier]
    hier_sums = output_hier.sum(axis=1) <= 1
    assert all(hier_sums)


def test_aux_present_when_aux_hierarchy_present(df_output, aux_hier, aux_no_hier):
    output_aux_no_hier = df_output[sorted(aux_no_hier)]
    output_aux_hier = df_output[sorted(aux_hier)]
    assert (output_aux_no_hier.values >= output_aux_hier.values).all()


def test_hierarchy_when_only_one_aux(df_output, aux_hier, aux_no_hier):
    output_one_aux = df_output[df_output[aux_no_hier].sum(axis=1) == 1]
    output_one_aux_no_hier = output_one_aux[sorted(aux_no_hier)]
    output_one_aux_hier = output_one_aux[sorted(aux_hier)]
    assert np.array_equal(output_one_aux_no_hier.values,
                          output_one_aux_hier)


def test_aggregate_aux_sum_greater_than_main_sum(df_output, main_cats):
    # test this with WAL category
    wal_sum = df_output['adr_net_wal_c_2014'].sum()
    wal_aux_sum = df_output[main_cats['adr_net_wal_c_2014']] \
        .sum(axis=1) \
        .sum()

    assert wal_aux_sum >= wal_sum


def test_aggregate_hier_aux_sum_equals_hier_main_sum(df_output, main_cats_hier):
    # test this with the WAL category
    wal_hier_sum = df_output['adr_net_walh_c_2014'].sum()
    wal_aux_hier_sum = df_output[main_cats_hier['adr_net_walh_c_2014']] \
        .sum(axis=1) \
        .sum()

    assert wal_aux_hier_sum == wal_hier_sum

if __name__ == "__main__":
    import os
    import pandas as pd
    from datetime import datetime
    from pathlib import Path
    import phase_3

    print("Beginning Testing at {}\n".format(str(datetime.now())))
    print("Setting up Testing Parameters:")
    config_filepath = Path.cwd().parent.parent / 'config'
    main_cats_filepath = config_filepath / 'main_categories.json'
    hierarchies_filepath = config_filepath / 'hierarchy_list.txt'
    df_dirpath = Path.cwd().parent / 'data' / 'data_intermediate'
    df_sample_path = [x for x in df_dirpath.iterdir() if "sample" in str(x)][0]
    print("Performing tests on {}\n".format(df_sample_path.name))


    main_cats = phase_3.load_main_cat_config(main_cats_filepath, hierarchies=False)
    main_cats_hier = phase_3.load_main_cat_config(main_cats_filepath, hierarchies=True)
    hier_list = phase_3.load_hierarchy_list(hierarchies_filepath)

    with open(config_filepath / 'aux_no_hier_vars.txt', 'r') as f:
        aux_no_hier = [line.strip() for line in f.readlines()]
    with open(config_filepath / 'aux_hier_vars.txt', 'r') as f:
        aux_hier = [line.strip() for line in f.readlines()]
    with open(config_filepath / 'main_no_hier_vars.txt', 'r') as f:
        main_no_hier = [line.strip() for line in f.readlines()]
    with open(config_filepath / 'main_hier_vars.txt', 'r') as f:
        main_hier = [line.strip() for line in f.readlines()]

    df_output = pd.read_csv(df_sample_path, chunksize=10**6)
    for i, chunk in enumerate(df_output):
        print("Beginning testing on chunk {}".format(i+1))
        test_output_aux_hierarchy_unique(chunk, aux_hier)
        print("Unique Hierarchy Test Passed!")
        test_aux_present_when_aux_hierarchy_present(chunk, aux_hier, aux_no_hier)
        print("Aux Present When Hierarchy Present Test Passed!")
        test_hierarchy_when_only_one_aux(chunk, aux_hier, aux_no_hier)
        print("Hierarchy == Aux When Only One Aux Test Passed!")
        test_aggregate_aux_sum_greater_than_main_sum(chunk, main_cats)
        print("Aggregate Aux Sum Greater Than Main Sum Test Passed!")
        test_aggregate_hier_aux_sum_equals_hier_main_sum(chunk, main_cats_hier)
        print("Aggregate Hierarchy Aux Sum == Hierarchy Main Sum Test Passed!\n")

    print("All tests passed!")
