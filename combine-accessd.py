import glob
import re
import pandas as pd

# -----------------------------
# CONFIG
input_pattern = "./accessed-url/accessible_websites_*.csv"
output_file = "./accessed-url/combined_accessible_websites.csv"
# -----------------------------

# find all matching files
files = glob.glob(input_pattern)
if not files:
    raise SystemExit(f"No files match pattern: {input_pattern}")

# function to extract start and end numbers from filename for sorting
def extract_range_numbers(filename):
    nums = re.findall(r'\d+', filename)
    start = int(nums[0]) if len(nums) > 0 else 0
    end = int(nums[1]) if len(nums) > 1 else 0
    return (start, end)

# sort files by numeric range
files = sorted(files, key=extract_range_numbers)

print("Sorted files by numeric range:")
for f in files:
    print(f)

# -----------------------------
# Read and combine files
dfs = []
for f in files:
    print(f"Processing file: {f}")
    try:
        # read CSV
        df = pd.read_csv(f, dtype=str)
    except Exception as e:
        print(f"Warning: failed to read {f}: {e} — skipping")
        continue

    if 'domain' not in df.columns:
        print(f"Warning: {f} missing 'domain' column — skipping")
        continue

    # ensure 'no' column exists
    if 'no' not in df.columns:
        df['no'] = ""  # placeholder

    # keep only 'no' and 'domain'
    dfs.append(df[['no', 'domain']])

if not dfs:
    raise SystemExit("No valid input files found.")

# concatenate all
combined = pd.concat(dfs, ignore_index=True)

# normalize domain: lowercase, strip whitespace, remove leading www.
combined['domain'] = combined['domain'].astype(str).str.strip().str.lower()
combined['domain'] = combined['domain'].str.lstrip('www.')

# drop duplicates by domain, keep first occurrence (preserves original 'no')
combined = combined.drop_duplicates(subset=['domain'], keep='first')

# save combined CSV
combined.to_csv(output_file, index=False)
print(f"Combined {len(files)} files -> {output_file} ({len(combined)} unique domains)")
