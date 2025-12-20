import csv
import sys
import collections
from prettytable import PrettyTable

DEFAULT_FILE = "final_clean_dataset.csv"

def check_errors(filename):
    print(f"📊 Analyzing errors in: {filename}")
    print("-" * 60)

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        print(f"❌ File not found: {filename}")
        return
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return

    total_rows = len(rows)
    error_rows = []
    
    # Error counters
    error_counts = collections.Counter()
    error_examples = {} # Code -> Example Query

    for row in rows:
        # Check for error indicators
        # Note: CSV values are strings
        err_code = row.get("error_code", "0")
        has_error = row.get("has_error", "0")
        
        if err_code != "0" or has_error == "1":
            error_rows.append(row)
            
            # Key for aggregation (Code + Message)
            msg = row.get("error_message", "").strip()
            key = f"Code {err_code}: {msg}"
            error_counts[key] += 1
            
            # Store example if new
            if key not in error_examples:
                error_examples[key] = {
                    "query": row.get("query", ""),
                    "user": row.get("user", ""),
                    "db": row.get("database", "")
                }

    # Print Summary
    print(f"Total Rows: {total_rows}")
    print(f"Total Errors: {len(error_rows)} ({len(error_rows)/total_rows*100:.2f}%)")
    print("-" * 60)

    if not error_rows:
        print("✅ No errors found in the dataset!")
        return

    # Print Breakdown Table
    t = PrettyTable(["Count", "Error Code & Message", "Example Query"])
    t.align["Error Code & Message"] = "l"
    t.align["Example Query"] = "l"
    t.max_width["Example Query"] = 60
    t.max_width["Error Code & Message"] = 50

    for key, count in error_counts.most_common():
        ex = error_examples[key]
        example_str = f"[{ex['user']}@{ex['db']}] {ex['query'][:100]}"
        if len(ex['query']) > 100: example_str += "..."
        t.add_row([count, key, example_str])

    print(t)
    print("\n💡 TIP: 'Code 1146' usually means 'Table doesn't exist'. Run populator scripts.")
    print("💡 TIP: 'Code 1044/1045' usually means 'Access denied'. Check permissions.")

if __name__ == "__main__":
    filename = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_FILE
    check_errors(filename)
