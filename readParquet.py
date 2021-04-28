import pandas as pd
pd.read_parquet(
    'part-00000-81d34a3d-760f-4a8a-b3b1-7a64f28a07dd-c000.snappy.parquet', engine='pyarrow')
