# Will eventually be worked into phase_3.py and this file will be deprecated
import pandas as pd

def create_categorized_file(data_filename_in, data_filename_out, hierarchy_cols):
    df = pd.read_csv(data_filename_in, chunksize=10**6, encoding='ISO-8859-1')

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
    data_filename_in = \
        r"C:\Users\jc4673\Documents\Columbia\NETS\nets_clean\patches\data\data_out\recvd_net_vars_v8_20190311.csv"
    data_filename_out = \
        r"C:\Users\jc4673\Documents\Columbia\NETS\nets_clean\patches\data\data_out\recvd_net_vars_v8_20190314_catOnly1.csv"
    hierarchy_cols = [
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

    create_categorized_file(data_filename_in, data_filename_out, hierarchy_cols)
