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
    from pathlib import Path
    import phase_3

    print("Setting up Testing Parameters:")
    config_filepath = Path.cwd().parent.parent / 'config'
    main_cats_filepath = config_filepath / 'main_categories.json'
    hierarchies_filepath = config_filepath / 'hierarchy_list.txt'
    df_dirpath = Path.cwd().parent / 'data' / 'data_intermediate'
    df_sample_path = df_dirpath / 'recvd_net_vars_v8_20190318_sample.csv'
    df_filenames = os.listdir(df_dirpath)


    main_cats = phase_3.load_main_cat_config(main_cats_filepath, hierarchies=False)
    main_cats_hier = phase_3.load_main_cat_config(main_cats_filepath, hierarchies=True)
    hier_list = phase_3.load_hierarchy_list(hierarchies_filepath)
    good_cols = phase_3.get_good_columns(df_dirpath / df_filenames[0], main_cats, main_cats_hier)

    aux_no_hier = [
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
        'adr_net_wrn_c_2014'
    ]

    aux_hier = [
        'adr_net_amuh_c_2014',
        'adr_net_arch_c_2014',
        'adr_net_barh_c_2014',
        'adr_net_bdsh_c_2014',
        'adr_net_beuh_c_2014',
        'adr_net_bhhh_c_2014',
        'adr_net_bhoh_c_2014',
        'adr_net_bknh_c_2014',
        'adr_net_bksh_c_2014',
        'adr_net_bnkh_c_2014',
        'adr_net_cfnh_c_2014',
        'adr_net_cfsh_c_2014',
        'adr_net_cmnh_c_2014',
        'adr_net_cmph_c_2014',
        'adr_net_cmuh_c_2014',
        'adr_net_cngh_c_2014',
        'adr_net_cnnh_c_2014',
        'adr_net_cnvh_c_2014',
        'adr_net_crdh_c_2014',
        'adr_net_csdh_c_2014',
        'adr_net_cvph_c_2014',
        'adr_net_dcrh_c_2014',
        'adr_net_ddph_c_2014',
        'adr_net_ddsh_c_2014',
        'adr_net_dpth_c_2014',
        'adr_net_drgh_c_2014',
        'adr_net_drnh_c_2014',
        'adr_net_eaoh_c_2014',
        'adr_net_eaph_c_2014',
        'adr_net_eath_c_2014',
        'adr_net_eeph_c_2014',
        'adr_net_eeuh_c_2014',
        'adr_net_fcsh_c_2014',
        'adr_net_ffsh_c_2014',
        'adr_net_firh_c_2014',
        'adr_net_fshh_c_2014',
        'adr_net_fvmh_c_2014',
        'adr_net_gamh_c_2014',
        'adr_net_gmnh_c_2014',
        'adr_net_gryh_c_2014',
        'adr_net_gssh_c_2014',
        'adr_net_hosh_c_2014',
        'adr_net_hpch_c_2014',
        'adr_net_jcoh_c_2014',
        'adr_net_kcth_c_2014',
        'adr_net_lauh_c_2014',
        'adr_net_libh_c_2014',
        'adr_net_liqh_c_2014',
        'adr_net_magh_c_2014',
        'adr_net_mash_c_2014',
        'adr_net_meth_c_2014',
        'adr_net_mhhh_c_2014',
        'adr_net_mhoh_c_2014',
        'adr_net_mpah_c_2014',
        'adr_net_mulh_c_2014',
        'adr_net_nath_c_2014',
        'adr_net_nclh_c_2014',
        'adr_net_nuth_c_2014',
        'adr_net_ofdh_c_2014',
        'adr_net_owrh_c_2014',
        'adr_net_pbeh_c_2014',
        'adr_net_phth_c_2014',
        'adr_net_pizh_c_2014',
        'adr_net_ploh_c_2014',
        'adr_net_polh_c_2014',
        'adr_net_posh_c_2014',
        'adr_net_psch_c_2014',
        'adr_net_qsvh_c_2014',
        'adr_net_rcch_c_2014',
        'adr_net_relh_c_2014',
        'adr_net_resh_c_2014',
        'adr_net_rtch_c_2014',
        'adr_net_scbh_c_2014',
        'adr_net_sclh_c_2014',
        'adr_net_scth_c_2014',
        'adr_net_slch_c_2014',
        'adr_net_smkh_c_2014',
        'adr_net_smnh_c_2014',
        'adr_net_spah_c_2014',
        'adr_net_spsh_c_2014',
        'adr_net_srvh_c_2014',
        'adr_net_tanh_c_2014',
        'adr_net_touh_c_2014',
        'adr_net_unih_c_2014',
        'adr_net_urgh_c_2014',
        'adr_net_vpah_c_2014',
        'adr_net_wrnh_c_2014',
        'adr_net_wrsh_c_2014',
        'adr_net_zooh_c_2014'
    ]

    main_no_hier = [
        'adr_net_wra_c_2014',
        'adr_net_sma_c_2014',
        'adr_net_cna_c_2014',
        'adr_net_bka_c_2014',
        'adr_net_usr_c_2014',
        'adr_net_usu_c_2014',
        'adr_net_hsr_c_2014',
        'adr_net_hsu_c_2014',
        'adr_net_fsa_c_2014',
        'adr_net_cfa_c_2014',
        'adr_net_ffa_c_2014',
        'adr_net_eta_c_2014',
        'adr_net_rsa_c_2014',
        'adr_net_aur_c_2014',
        'adr_net_auu_c_2014',
        'adr_net_aal_c_2014',
        'adr_net_apa_c_2014',
        'adr_net_acp_c_2014',
        'adr_net_fin_c_2014',
        'adr_net_dep_c_2014',
        'adr_net_sfc_c_2014',
        'adr_net_fsr_c_2014',
        'adr_net_fsu_c_2014',
        'adr_net_sid_c_2014',
        'adr_net_ngt_c_2014',
        'adr_net_cul_c_2014',
        'adr_net_cas_c_2014',
        'adr_net_saf_c_2014',
        'adr_net_edd_c_2014',
        'adr_net_dra_c_2014',
        'adr_net_mha_c_2014',
        'adr_net_bha_c_2014',
        'adr_net_mbh_c_2014',
        'adr_net_hoi_c_2014',
        'adr_net_cmb_c_2014',
        'adr_net_amb_c_2014',
        'adr_net_aec_c_2014',
        'adr_net_act_c_2014',
        'adr_net_ptm_c_2014',
        'adr_net_dpa_c_2014',
        'adr_net_rua_c_2014',
        'adr_net_cer_c_2014',
        'adr_net_ceu_c_2014',
        'adr_net_wal_c_2014'
    ]

    main_hier = [
        'adr_net_wrah_c_2014',
        'adr_net_smah_c_2014',
        'adr_net_cnah_c_2014',
        'adr_net_bkah_c_2014',
        'adr_net_usrh_c_2014',
        'adr_net_usuh_c_2014',
        'adr_net_hsrh_c_2014',
        'adr_net_hsuh_c_2014',
        'adr_net_fsah_c_2014',
        'adr_net_cfah_c_2014',
        'adr_net_ffah_c_2014',
        'adr_net_etah_c_2014',
        'adr_net_rsah_c_2014',
        'adr_net_aurh_c_2014',
        'adr_net_auuh_c_2014',
        'adr_net_aalh_c_2014',
        'adr_net_apah_c_2014',
        'adr_net_acph_c_2014',
        'adr_net_finh_c_2014',
        'adr_net_deph_c_2014',
        'adr_net_sfch_c_2014',
        'adr_net_fsrh_c_2014',
        'adr_net_fsuh_c_2014',
        'adr_net_sidh_c_2014',
        'adr_net_ngth_c_2014',
        'adr_net_culh_c_2014',
        'adr_net_cash_c_2014',
        'adr_net_safh_c_2014',
        'adr_net_eddh_c_2014',
        'adr_net_drah_c_2014',
        'adr_net_mhah_c_2014',
        'adr_net_bhah_c_2014',
        'adr_net_mbhh_c_2014',
        'adr_net_hoih_c_2014',
        'adr_net_cmbh_c_2014',
        'adr_net_ambh_c_2014',
        'adr_net_aech_c_2014',
        'adr_net_acth_c_2014',
        'adr_net_ptmh_c_2014',
        'adr_net_dpah_c_2014',
        'adr_net_ruah_c_2014',
        'adr_net_cerh_c_2014',
        'adr_net_ceuh_c_2014',
        'adr_net_walh_c_2014'
    ]
    #
    # for i, df_filename in enumerate(df_filenames):
    #     print("Beginning Testing on Sample {}".format(i+1))
    #     df_output = pd.read_csv(df_dirpath / df_filename)
    #
    #     test_output_aux_hierarchy_unique(df_output, aux_hier)
    #     print("Unique Hierarchy Test Passed!")
    #     test_aux_present_when_aux_hierarchy_present(df_output, aux_hier, aux_no_hier)
    #     print("Aux Present When Hierarchy Present Test Passed!")
    #     test_hierarchy_when_only_one_aux(df_output, aux_hier, aux_no_hier)
    #     print("Hierarchy == Aux When Only One Aux Test Passed!")
    #     test_aggregate_aux_sum_greater_than_main_sum(df_output, main_cats)
    #     print("Aggregate Aux Sum Greater Than Main Sum Test Passed!")
    #     test_aggregate_hier_aux_sum_equals_hier_main_sum(df_output, main_cats_hier)
    #     print("Aggregate Hierarchy Aux Sum == Hierarchy Main Sum Test Passed!\n")
    #
    # print("All tests passed!")

    df_output = pd.read_csv(df_sample_path)
    test_output_aux_hierarchy_unique(df_output, aux_hier)
    print("Unique Hierarchy Test Passed!")
    test_aux_present_when_aux_hierarchy_present(df_output, aux_hier, aux_no_hier)
    print("Aux Present When Hierarchy Present Test Passed!")
    test_hierarchy_when_only_one_aux(df_output, aux_hier, aux_no_hier)
    print("Hierarchy == Aux When Only One Aux Test Passed!")
    test_aggregate_aux_sum_greater_than_main_sum(df_output, main_cats)
    print("Aggregate Aux Sum Greater Than Main Sum Test Passed!")
    test_aggregate_hier_aux_sum_equals_hier_main_sum(df_output, main_cats_hier)
    print("Aggregate Hierarchy Aux Sum == Hierarchy Main Sum Test Passed!\n")

    print("All tests passed!")
