import csv
import re
from collections import Counter

filename = "final_clean_dataset.csv"
error_pattern = re.compile(r"Table '([^']+)' doesn't exist")

failures = Counter()

try:
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("error_code") == "1146":
                msg = row.get("error_message", "")
                match = error_pattern.search(msg)
                if match:
                    failures[match.group(1)] += 1
                else:
                    failures[f"Unknown 1146: {msg}"] += 1

    print("❌ Top missing tables causing Error 1146:")
    for table, count in failures.most_common():
        print(f"   {count}: {table}")

except Exception as e:
    print(f"Error: {e}")
