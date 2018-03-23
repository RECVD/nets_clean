import os
import pandas as pd


def create_samples(path, sample_size):
    """
    Creates samples of the given sample size for all tab delimited .txt files in the directory
    represented by path.

    Doesn't Return anything
    """
    # Create generator for all text files
    files = [file for file in os.listdir(path) if os.path.isfile(os.path.join(path, file))]

    for i, name in enumerate(files):  # Enumerate for checking when i != 0
        # Read csv of each file.  Currently implemented as a non-random sample- to be fixed
        # later on and re-checked
        df = pd.read_table(path + "\\" + name, nrows=sample_size)
        # Compare DunsNumber values to that of the previous DF
        if i != 0:
            if not df.DunsNumber.equals(prev):
                print("Error in file %s: Indexing Mismatches other Files" % name)
                break

        prev = df.DunsNumber
        df.to_csv("%s\samples\%s_sample.csv" % (path, name[:-4]), index=False)

if __name__ == "__main__":
    create_samples(r"C:\Users\jc4673\Documents\NETS\data\NETS2014_RAW", 10**1)
