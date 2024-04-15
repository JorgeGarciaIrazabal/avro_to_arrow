import pandas as pd
from avro_to_arrow import avro_to_arrow_batches
import time
from fastavro import reader
from pyarrow import RecordBatch

AVRO_FILE = "/home/jorge/code/avro_to_arrow/big_avro.avro"

print("using rust avro to arrow")
start = time.time()
objects = []
with open(AVRO_FILE, "rb") as f:
    for i, record in enumerate(reader(f)):
        objects.append(record)
        if i % 100_000 == 0:
            print("fast_avro", i, time.time() - start)

print("building dataframe", time.time() - start)
full_df = pd.DataFrame(objects)
print("done fastavro", time.time() - start)
