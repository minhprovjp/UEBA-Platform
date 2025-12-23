import csv
import collections
import sys

def check_errors(filename, output_file):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        print(f"Error: {e}")
        return

    error_counts = collections.Counter()
    error_examples = {}

    for row in rows:
        err_code = row.get("error_code", "0")
        has_error = row.get("has_error", "0")
        
        if err_code != "0" or has_error == "1":
            msg = row.get("error_message", "").strip()
            key = f"Code {err_code}: {msg}"
            error_counts[key] += 1
            if key not in error_examples:
                error_examples[key] = {
                    "query": row.get("query", ""),
                    "user": row.get("user", ""),
                    "db": row.get("database", "")
                }

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"Total Errors: {sum(error_counts.values())}\n")
        f.write("-" * 80 + "\n")
        for key, count in error_counts.most_common():
            ex = error_examples[key]
            f.write(f"Count: {count}\n")
            f.write(f"Error: {key}\n")
            f.write(f"User: {ex['user']}@{ex['db']}\n")
            f.write(f"Query: {ex['query']}\n")
            f.write("-" * 80 + "\n")

if __name__ == "__main__":
    check_errors("final_clean_dataset.csv", "error_report_safe.txt")
