import time
import random


def random_sampler(read_filename, write_filename, k):
    """Write a random sample of read_filename to write_filename of size k"""
    with open(read_filename, 'rb') as fr, open(write_filename, 'w') as fw:
        line1 = fr.readline().rstrip().decode() + '\n'
        fw.write(line1)

        fr.seek(0, 2)
        filesize = fr.tell()
        random_set = sorted(random.sample(range(filesize), k))

        for i in range(k):
            fr.seek(random_set[i])
            # Skip current line (because we might be in the middle of a line)
            fr.readline()
            # Append the next line to the sample set
            samp = fr.readline().rstrip().decode() + '\n'
            fw.write(samp)


if __name__ == "__main__":
    t1 = time.time()
    random_sampler(r"C:\Users\jc4673\Documents\Columbia\NETS\data\recvd_net_vars_v7_20180829.csv",
                          "test.csv",
                          10**3)
    print(time.time() - t1)
