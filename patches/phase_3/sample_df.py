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


if __name__ == "__main__":
    t1 = time.time()
    data_in = Path.cwd().parent / 'data' / 'data_out' / 'recvd_net_vars_v8_20190306.csv'
    data_out = Path.cwd().parent / 'data' / 'data_intermediate'
    num_samples = 10
    sample_size = 10**6

    print("Beginning Random Sampling")
    for i in range(num_samples):
        random_sampler(data_in, data_out / 'recvd_net_vars_v8_samp{}.csv'.format(i+1), sample_size)
        print("Sample {} complete.".format(i+1))
