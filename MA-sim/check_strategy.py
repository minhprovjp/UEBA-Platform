import pandas as pd
try:
    df = pd.read_csv('final_clean_dataset.csv')
    if 'generation_strategy' in df.columns:
        print("Generation Strategy Distribution:")
        print(df['generation_strategy'].value_counts())
    else:
        print("Column 'generation_strategy' not found.")
        
    print("\nMalicious Strategy Check:")
    malicious = df[df['generation_strategy'].str.contains('malicious', na=False)]
    print(f"Total Malicious tagged rows: {len(malicious)}")
    if not malicious.empty:
        print(malicious['generation_strategy'].value_counts().head())
        
    print("\nAI Strategy Check:")
    ai = df[df['generation_strategy'].str.contains('ai_generation', na=False)]
    print(f"Total AI tagged rows: {len(ai)}")
except Exception as e:
    print(f"Error: {e}")
