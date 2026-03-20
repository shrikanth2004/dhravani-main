import os
import glob
import pandas as pd

def check_schemas():
    files = glob.glob('datasets/*/*.parquet')
    schemas = {}
    for f in files:
        try:
            df = pd.read_parquet(f)
            cols = tuple(df.columns.tolist())
            if cols not in schemas:
                schemas[cols] = []
            schemas[cols].append(f)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    print(f"Found {len(schemas)} different schemas.")
    for idx, (cols, files_list) in enumerate(schemas.items()):
        print(f"\nSchema {idx + 1} ({len(cols)} columns):")
        print(cols)
        print("Files:")
        for f in files_list:
            print(f"  - {f}")

if __name__ == "__main__":
    check_schemas()
