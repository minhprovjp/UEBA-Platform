import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import * # Import tất cả các hằng số từ config.py


output_csv_path = PARSED_MYSQL_LOG_FILE_PATH
output_parquet_path = output_csv_path.replace('.csv', '.parquet')
df = pd.read_parquet(output_parquet_path) # Convert to Pandas DataFrame if needed
print(df.head())