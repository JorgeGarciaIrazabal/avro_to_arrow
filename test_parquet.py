import time

import pandas as pd
import pyarrow.parquet

PARQUET_FILE = "/home/jorge/code/avro_to_arrow/big_parquet.parquet"

start = time.time()
all_dfs = []
for b in pyarrow.parquet.ParquetFile(PARQUET_FILE).iter_batches(batch_size=100_000):
    all_dfs.append(b.to_pandas(self_destruct=True))
    print("parquet file to pandas size", all_dfs[-1].shape[0], time.time() - start)
pd.concat(all_dfs)
print("parquet file to pandas", time.time() - start)