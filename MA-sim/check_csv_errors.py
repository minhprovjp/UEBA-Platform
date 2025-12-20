import csv
import sys
import collections


DEFAULT_FILE = "final_clean_dataset.csv"

def check_errors(filename):
    
    # Redirect output to file to avoid console encoding issues
    with open("latest_error_report.txt", 'w', encoding='utf-8') as log_file:
        def log(msg):
            print(msg)
            log_file.write(msg + "\n")
            
        log(f"📊 Analyzing errors in: {filename}")
        log("-" * 60)

        try:
            with open(filename, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except FileNotFoundError:
            log(f"❌ File not found: {filename}")
            return
        except Exception as e:
            log(f"❌ Error reading file: {e}")
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
        log(f"Total Rows: {total_rows}")
        log(f"Total Errors: {len(error_rows)} ({len(error_rows)/total_rows*100:.2f}%)")
        log("-" * 60)

        if not error_rows:
            log("✅ No errors found in the dataset!")
            return

        # Print Breakdown Table
        log(f"{'Count':<8} | {'Error Code & Message':<50} | {'Example Query'}")
        log("-" * 120)

        for key, count in error_counts.most_common():
            ex = error_examples[key]
            # sanitize query for printing
            q_display = ex['query'].replace('\n', ' ').strip()
            example_str = f"[{ex['user']}@{ex['db']}] {q_display[:100]}"
            if len(q_display) > 100: example_str += "..."
            log(f"{count:<8} | {key:<50} | {example_str}")
        log("\n💡 TIP: 'Code 1146' usually means 'Table doesn't exist'. Run populator scripts.")
        log("💡 TIP: 'Code 1044/1045' usually means 'Access denied'. Check permissions.")

if __name__ == "__main__":
    # Force utf-8 for stdout if possible, though we rely on file now
    sys.stdout.reconfigure(encoding='utf-8')
    filename = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_FILE
    check_errors(filename)
