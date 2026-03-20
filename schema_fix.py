import os
import glob
import pandas as pd

def standardize_schema():
    target_columns = [
        'user_id', 'file_name', 'transcription', 'speaker_name', 'audio', 
        'sampling_rate', 'duration', 'language', 'gender', 'country', 
        'state', 'city', 'status', 'verified_by', 'age', 'accent', 
        'user_info', 'audio_sampling_rate', 'audio_file_name'
    ]
    
    parquet_files = glob.glob('datasets/*/*.parquet')
    
    for f in parquet_files:
        print(f"Standardizing {f}...")
        try:
            df = pd.read_parquet(f)
            # Rename legacy fields
            if 'audio_filename' in df.columns:
                df = df.rename(columns={'audio_filename': 'file_name'})
            if 'audio_path' in df.columns:
                df = df.rename(columns={'audio_path': 'audio'})
            if 'age_group' in df.columns:
                df = df.rename(columns={'age_group': 'age'})
            
            # Map standard structure (adding new cols with None/NaN if missing)
            for col in target_columns:
                if col not in df.columns:
                    df[col] = None
            
            # Reorder and filter columns exactly
            df = df[target_columns]
            
            # Save standardized
            df.to_parquet(f, index=False)
            print(f"  Fixed schema for {f}.")
        except Exception as e:
            print(f"  Could not process {f}: {e}")

if __name__ == "__main__":
    standardize_schema()
