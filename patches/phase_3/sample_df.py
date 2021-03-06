import time
import random
from pathlib import Path


def random_sampler(read_filename, write_filename, k):
    """Write a random sample of read_filename to write_filename of size k"""
    with open(read_filename, 'rb') as fr, open(write_filename, 'wb') as fw:
        line1 = fr.readline()
        fw.write(line1)

        fr.seek(0, 2)
        filesize = fr.tell()
        random_set = sorted(random.sample(range(filesize), k))

        for i in range(k):
            fr.seek(random_set[i])
            # Skip current line (because we might be in the middle of a line)
            fr.readline()
            line = fr.readline()
            fw.write(line)


def reverse_dummies(dummies):
    """Get the original categorical from a set of dummy variables"""
    return dummies.dot(dummies.columns)


def prob_sampler(df, samp_frac, hier_colnames, probs):
    df['hier_cat'] = reverse_dummies(df[hier_colnames])
    df_probs = df.merge(probs, how='outer', on='hier_cat')
    samp = df_probs.sample(frac=samp_frac, replace=False, weights='prob', random_state=0)
    samp.drop(['hier_cat', 'prob'], axis=1, inplace=True)

    return samp



if __name__ == "__main__":
    import pandas as pd
    
    t1 = time.time()
    data_in = Path.cwd().parent / 'data' / 'data_out' / 'recvd_net_vars_v8_20190314_catOnly.csv'
    probs = Path.cwd().parent / 'data' / 'data_intermediate' / 'category_prob_dist.csv'
    data_out = Path.cwd().parent / 'data' / 'data_intermediate' / 'recvd_net_vars_v8_20190318_sample.csv'

    hier_colnames = [
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
    df = pd.read_csv(data_in, chunksize=10**6, encoding='ISO-8859-1')
    probs = pd.read_csv(probs)

    for i, chunk in enumerate(df):
        samp = prob_sampler(chunk, .1, hier_colnames, probs)

        if i == 0:
            with open(data_out, 'w', encoding='utf-8', newline='\n') as f:
                samp.to_csv(f, index=False)
            print(i)

        else:
            with open(data_out, 'a', encoding='utf-8', newline='\n') as f:
                samp.to_csv(f, header=False, index=False)
            print(i)



