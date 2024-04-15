import numpy as np
import pandas as pd
import pandavro as pdx

# Create a DataFrame with 10 columns of different types with 100 rows and random data
def create_data():
    row_count = 5_000_000
    data = {
        'A': np.random.rand(row_count),
        'B': np.random.randint(0, 100, row_count),
        'C': np.random.choice(['low', 'medium', 'high'], row_count),
        'D': np.random.choice([True, False], row_count),
        'E': np.random.choice(pd.date_range('2020-01-01', periods=365), row_count),
        'F': np.random.choice(pd.date_range('2020-01-01', periods=365, freq='H'), row_count),
        'G': np.random.choice(pd.date_range('2020-01-01', periods=365, freq='T'), row_count),
        'H': np.random.choice(pd.date_range('2020-01-01', periods=365, freq='S'), row_count),
        'I': np.random.choice(pd.date_range('2020-01-01', periods=365, freq='L'), row_count),
        'J': np.random.choice(pd.date_range('2020-01-01', periods=365, freq='U'), row_count)
    }
    return pd.DataFrame(data)

df = create_data()
print("generated data")
# Write the DataFrame to an Avro file
pdx.to_avro('big_avro.avro', df)
df.to_parquet('big_parquet.parquet')
print("data_saved")