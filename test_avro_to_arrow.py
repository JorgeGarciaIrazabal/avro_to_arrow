import pandas as pd
from avro_to_arrow import avro_to_arrow_batches
import time
from fastavro import reader
from pyarrow import RecordBatch

AVRO_FILE = "/home/jorge/code/avro_to_arrow/big_avro.avro"

print("using rust avro to arrow")
start = time.time()
record_batch: RecordBatch
all_dfs = []
for i, record_batch in enumerate(avro_to_arrow_batches(AVRO_FILE, 100_000)):
    all_dfs.append(record_batch.to_pandas(self_destruct=True))
    print("avro to arrow", i * 1_000_000, time.time() - start)

print("building dataframe", time.time() - start)
full_df: pd.DataFrame = pd.concat(all_dfs)
print("done avro to arrow", time.time() - start)

print("using fastavro")
start = time.time()
