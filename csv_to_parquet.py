# There is a pull request to read csv files using Arrow (bypassing pandas)
# https://github.com/apache/arrow/pull/2576

import argparse
import csv
import pandas as pd
import numpy  as np

parser = argparse.ArgumentParser(description="Convert csv files to parquet.")
parser.add_argument("-i", "--input",  type=str, help="Input file")
parser.add_argument("-o", "--output", type=str, help="Output file")
args = parser.parse_args()

# define dtype for each file type
if "blocks_" in args.input:
    dtype = {"number": np.int64,
             "hash": np.str,
             "parent_hash": np.str,
             "nonce": np.str,
             "sha3_uncles": np.str,
             "logs_bloom": np.str,
             "transactions_root": np.str,
             "state_root": np.str,
             "receipts_root": np.str,
             "miner": np.str,
             "difficulty": np.str,
             "total_difficulty": np.str,
             "size": np.int64,
             "extra_data": np.str,
             "gas_limit": np.int64,
             "gas_used": np.int64,
             "timestamp": np.int64,
             "transaction_count": np.int64}
elif "transactions" in args.input:
    dtype = {"hash": np.str,
             "nonce": np.int64,
             "block_hash": np.str,
             "block_number": np.int64,
             "transaction_index": np.int64,
             "from_address": np.str,
             "to_address": np.str,
             "value": np.str,
             "gas": np.int64,
             "gas_price": np.int64,
             "input": np.str}
elif "token_transfers_" in args.input:
    dtype = {"token_address": np.str,
             "from_address": np.str,
             "to_address": np.str,
             "value": np.str,
             "transaction_hash": np.str,
             "log_index": np.int64,
             "block_number": np.int64}
# elif "transaction_hashes_" in args.input:
#     dtype = {}
elif "receipts_" in args.input:
    dtype = {"transaction_hash": np.str,
             "transaction_index": np.int64,
             "block_hash": np.str,
             "block_number": np.int64,
             "cumulative_gas_used": np.int64,
             "gas_used": np.int64,
             "contract_address": np.str,
             "root": np.str,
             "status": np.int64}
elif "logs_" in args.input:
    dtype = {"log_index": np.int64,
             "transaction_hash": np.str,
             "transaction_index": np.int64,
             "block_hash": np.str,
             "block_number": np.int64,
             "address": np.str,
             "data": np.str,
             "topics": np.str}
# elif "contract_addresses"_ in args.input:
#     dtype = {}
elif "contracts_" in args.input:
    dtype = {"address": np.str,
             "bytecode": np.str,
             "function_sighashes": np.str,
             "is_erc20": np.bool,
             "is_erc721": np.bool}
# elif "token_addresses_" in args.input:
#     dtype = {}
elif "tokens_" in args.input:
    dtype = {"address": np.str,
             "symbol": np.str,
             "name": np.str,
             "decimals": np.int64,
             "total_supply": np.str}

# try loading csv files with explicitly specified dtype
try:
    df = pd.read_csv(args.input, dtype=dtype)
except pd.errors.EmptyDataError as e:
    print(e)
    pass
else:
    df.to_parquet(args.output)
