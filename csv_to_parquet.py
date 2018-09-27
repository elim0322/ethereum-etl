# There is a pull request to read csv files using Arrow (bypassing pandas)
# https://github.com/apache/arrow/pull/2576

import argparse
import csv
import pandas as pd

parser = argparse.ArgumentParser(description="Convert csv files to parquet.")
parser.add_argument("-i", "--input",  type=str, help="Input file")
parser.add_argument("-o", "--output", type=str, help="Output file")
args = parser.parse_args()

try:
    df = pd.read_csv(args.input)
except pd.errors.EmptyDataError as e:
    print(e)
    pass
else:
    df.to_parquet(args.output)
