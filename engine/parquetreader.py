import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import * # Import tất cả các hằng số từ config.py


output_parquet_path = os.path.join(STAGING_DATA_DIR, "mysql_general_20251103_151250.parquet")
df = pd.read_parquet(output_parquet_path) # Convert to Pandas DataFrame if needed
print(df.head())