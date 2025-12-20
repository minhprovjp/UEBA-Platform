
import csv
import os

INPUT_FILE = "final_clean_dataset.csv"
TEMP_FILE = "final_clean_dataset_cleaned.csv"

def clean_csv():
    if not os.path.exists(INPUT_FILE):
        print(f"File {INPUT_FILE} not found.")
        return

    removed_count = 0
    kept_count = 0
    
    with open(INPUT_FILE, 'r', encoding='utf-8', newline='') as infile, \
         open(TEMP_FILE, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        
        for row in reader:
            # Check for error indicators
            has_error = row.get('has_error', '0')
            error_code = row.get('error_code', '0')
            # Also check for empty queries or extremely short ones if desired, but errors are main target
            
            if has_error == '1' or (error_code != '0' and error_code != ''):
                removed_count += 1
            else:
                writer.writerow(row)
                kept_count += 1

    # Replace original file
    try:
        os.replace(TEMP_FILE, INPUT_FILE)
        print(f"Cleanup complete. Removed {removed_count} error rows. Kept {kept_count} rows.")
    except Exception as e:
        print(f"Error replacing file: {e}")

if __name__ == "__main__":
    clean_csv()
