import numpy as np
import pandas as pd
from pathlib import Path
import phase_3

# Initialize setup outputs
main_cats = None
main_cats_hier = None
hierarchies = None
good_cols = None
df_input = None
df_output = None

def setup_module():
    print('----------setup------------')

    config_filepath = Path.cwd().parent.parent / 'config'
    main_cats_filepath = config_filepath / 'main_categories.json'
    hierarchies_filepath = config_filepath / 'hierarchy_list.txt'
    df_filepath = Path.cwd().parent.parent.parent / 'data' / \
                  "recvd_net_vars_v7_20180829.csv"

    global main_cats
    main_cats = phase_3.load_main_cat_config(main_cats_filepath, hierarchies=False)

    global main_cats_hier
    main_cats_hier = phase_3.load_main_cat_config(main_cats_filepath, hierarchies=True)

    global hier_list
    hier_list = phase_3.load_hierarchy_list(hierarchies_filepath)

    global good_cols
    good_cols = phase_3.get_good_columns(df_filepath, main_cats, main_cats_hier)

    # For output testing
    global df_input
    df_input = pd.read_csv(df_filepath, usecols=good_cols, nrows=10**3)

    global df_input_allcols
    with open(df_filepath, 'r') as f:
        df_input_allcols = f.readline().strip().split(',')

    global df_output
    df_output = phase_3.reclassify(df_input, hier_list, main_cats, main_cats_hier)

    global aux_no_hier
    with open(config_filepath / 'aux_no_hier_vars.txt', 'r') as f:
        aux_no_hier = [line.strip() for line in f.readlines()]

    global aux_hier
    with open(config_filepath / 'aux_hier_vars.txt', 'r') as f:
        aux_hier = [line.strip() for line in f.readlines()]

    global main_no_hier
    with open(config_filepath / 'main_no_hier_vars.txt', 'r') as f:
        main_no_hier = [line.strip() for line in f.readlines()]

    global main_hier
    with open(config_filepath / 'main_hier_vars.txt', 'r') as f:
        main_hier = [line.strip() for line in f.readlines()]

def test_load_main_cat_config():
    # Number of keys, uniqueness
    assert len(set(main_cats.keys())) == 44
    assert len(set(main_cats_hier.keys())) == 44

    # Spot check
    assert main_cats['adr_net_ngt_c_2014'] == \
           ['adr_net_bar_c_2014',
            'adr_net_scb_c_2014',
            'adr_net_ncl_c_2014',
            'adr_net_gam_c_2014']
    assert main_cats_hier['adr_net_ngth_c_2014'] == \
           ['adr_net_barh_c_2014',
            'adr_net_scbh_c_2014',
            'adr_net_nclh_c_2014',
            'adr_net_gamh_c_2014']


def test_load_hierarchy_list():
    assert len(set(hier_list)) == 89


def test_get_good_columns():
    truth = sorted(['adr_net_behid_u_2014',
                         'adr_net_dunsnumber_x_2014',
                         'adr_net_behloc_x_2014',
                         'adr_net_firstyear_x_2014',
                         'adr_net_lastyear_x_2014',
                         'adr_net_behsic_x_2014',
                         'adr_net_company_x_2014',
                         'adr_net_tradename_x_2014',
                         'adr_net_adl_c_2014',
                         'adr_net_adp_c_2014',
                         'adr_net_edu_c_2014',
                         'adr_net_med_c_2014',
                         'adr_net_pav_c_2014',
                         'adr_net_pwd_c_2014',
                         'adr_net_piz_c_2014',
                         'adr_net_bkn_c_2014',
                         'adr_net_eat_c_2014',
                         'adr_net_bks_c_2014',
                         'adr_net_met_c_2014',
                         'adr_net_fvm_c_2014',
                         'adr_net_nat_c_2014',
                         'adr_net_fsh_c_2014',
                         'adr_net_cnv_c_2014',
                         'adr_net_bds_c_2014',
                         'adr_net_smk_c_2014',
                         'adr_net_gry_c_2014',
                         'adr_net_bar_c_2014',
                         'adr_net_liq_c_2014',
                         'adr_net_urg_c_2014',
                         'adr_net_hpc_c_2014',
                         'adr_net_res_c_2014',
                         'adr_net_dds_c_2014',
                         'adr_net_mul_c_2014',
                         'adr_net_vpa_c_2014',
                         'adr_net_mpa_c_2014',
                         'adr_net_bnk_c_2014',
                         'adr_net_crd_c_2014',
                         'adr_net_des_c_2014',
                         'adr_net_nut_c_2014',
                         'adr_net_beu_c_2014',
                         'adr_net_lib_c_2014',
                         'adr_net_rel_c_2014',
                         'adr_net_pos_c_2014',
                         'adr_net_ncl_c_2014',
                         'adr_net_sps_c_2014',
                         'adr_net_mag_c_2014',
                         'adr_net_zoo_c_2014',
                         'adr_net_ofd_c_2014',
                         'adr_net_ffs_c_2014',
                         'adr_net_fcs_c_2014',
                         'adr_net_qsv_c_2014',
                         'adr_net_csd_c_2014',
                         'adr_net_cfn_c_2014',
                         'adr_net_dpt_c_2014',
                         'adr_net_ddp_c_2014',
                         'adr_net_scl_c_2014',
                         'adr_net_uni_c_2014',
                         'adr_net_srv_c_2014',
                         'adr_net_cmu_c_2014',
                         'adr_net_cmp_c_2014',
                         'adr_net_cvp_c_2014',
                         'adr_net_tan_c_2014',
                         'adr_net_mas_c_2014',
                         'adr_net_dcr_c_2014',
                         'adr_net_psc_c_2014',
                         'adr_net_pol_c_2014',
                         'adr_net_fir_c_2014',
                         'adr_net_jco_c_2014',
                         'adr_net_lau_c_2014',
                         'adr_net_gss_c_2014',
                         'adr_net_scb_c_2014',
                         'adr_net_spa_c_2014',
                         'adr_net_cfs_c_2014',
                         'adr_net_eep_c_2014',
                         'adr_net_eap_c_2014',
                         'adr_net_eao_c_2014',
                         'adr_net_eeu_c_2014',
                         'adr_net_pbe_c_2014',
                         'adr_net_gam_c_2014',
                         'adr_net_arc_c_2014',
                         'adr_net_rcc_c_2014',
                         'adr_net_plo_c_2014',
                         'adr_net_slc_c_2014',
                         'adr_net_amu_c_2014',
                         'adr_net_tou_c_2014',
                         'adr_net_drg_c_2014',
                         'adr_net_mhh_c_2014',
                         'adr_net_mho_c_2014',
                         'adr_net_bhh_c_2014',
                         'adr_net_bho_c_2014',
                         'adr_net_hos_c_2014',
                         'adr_net_rtc_c_2014',
                         'adr_net_pht_c_2014',
                         'adr_net_kct_c_2014',
                         'adr_net_wrs_c_2014',
                         'adr_net_owr_c_2014',
                         'adr_net_drn_c_2014',
                         'adr_net_sct_c_2014',
                         'adr_net_smn_c_2014',
                         'adr_net_gmn_c_2014',
                         'adr_net_cnn_c_2014',
                         'adr_net_cng_c_2014',
                         'adr_net_cmn_c_2014',
                         'adr_net_wrn_c_2014',
                         'adr_gis_geocoder_x_2015',
                         'adr_gis_status_x_2015',
                         'adr_gis_score_x_2015',
                         'adr_gis_xwgs84_x_2015',
                         'adr_gis_ywgs84_x_2015',
                         'adr_gis_xalbers_x_2015',
                         'adr_gis_yalbers_x_2015',
                         'm10_cen_uid_u_2010',
                         'm10_cen_memi_x_2010',
                         'm10_cen_name_x_2010',
                         'm10_gis_area_k_2010',
                         'm10_gis_area_l_2010',
                         'c10_cen_uid_u_2010',
                         'c10_gis_area_k_2010',
                         'c10_gis_area_l_2010',
                         'z10_cen_uid_u_2010',
                         'z10_gis_area_k_2010',
                         'z10_gis_area_l_2010',
                         't10_cen_uid_u_2010',
                         't10_gis_area_k_2010',
                         't10_gis_area_l_2010'])

    assert [x for x in good_cols if x not in truth] == []
    assert [x for x in truth if x not in good_cols] == []

def test_output_shape():
    assert len(df_input) == len(df_output)
    # Subtract one because taking DES out of the hierarchy
    # Add it back because of adding NOT
    assert len(df_input_allcols) == len(df_output.columns)
