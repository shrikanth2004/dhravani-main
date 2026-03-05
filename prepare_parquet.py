import logging
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from database_manager import engine
from language_config import LANGUAGES
from datetime import datetime

logger = logging.getLogger(__name__)

# Define base directory for Parquet files
BASE_DIR = Path('datasets')

def update_parquet_files():
    """Extract verified records and update Parquet files for each language"""
    try:
        logger.info("Starting Parquet file update process")
        
        for lang_code in LANGUAGES.keys():
            try:
                # Check if recordings table exists
                with engine.connect() as conn:
                    table_exists = conn.execute(text("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = :table_name
                        )
                    """), {"table_name": f"recordings_{lang_code}"}).scalar()

                    if not table_exists:
                        logger.debug(f"No recordings table for language: {lang_code}")
                        continue

                    # Modified query to only include verified records
                    query = text(f"""
                        SELECT r.user_id, r.audio_filename, t.transcription_text as transcription,
                               r.speaker_name, r.audio_path, r.sampling_rate, r.duration,
                               r.language, r.gender, r.country, r.state, r.city,
                               r.status, r.verified_by, r.age_group, r.accent,
                               r.domain, r.subdomain, t.domain as t_domain, t.subdomain as t_subdomain
                        FROM recordings_{lang_code} r
                        LEFT JOIN transcriptions_{lang_code} t ON r.transcription_id = t.transcription_id
                        WHERE r.status = 'verified'  -- Only get verified recordings
                    """)
                    
                    # Read verified records into DataFrame
                    df_new = pd.read_sql(query, conn)
                    
                    if df_new.empty:
                        logger.debug(f"No verified records for language: {lang_code}")
                        continue

                    # Fill missing domain/subdomain from transcription if available
                    df_new['domain'] = df_new.apply(
                        lambda row: row['domain'] or row['t_domain'], axis=1)
                    df_new['subdomain'] = df_new.apply(
                        lambda row: row['subdomain'] or row['t_subdomain'], axis=1)
                    
                    # Drop the temporary columns
                    df_new = df_new.drop(['t_domain', 't_subdomain'], axis=1)

                    # Create language directory if needed
                    lang_dir = BASE_DIR / lang_code
                    lang_dir.mkdir(parents=True, exist_ok=True)
                    
                    parquet_path = lang_dir / f"{lang_code}.parquet"
                    
                    # Merge with existing Parquet if it exists
                    if parquet_path.exists():
                        df_existing = pd.read_parquet(parquet_path)
                        
                        # Identify new records by audio_filename
                        existing_files = set(df_existing['audio_filename'])
                        df_new = df_new[~df_new['audio_filename'].isin(existing_files)]
                        
                        if not df_new.empty:
                            # Append new records
                            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                            df_combined.to_parquet(parquet_path, index=False)
                            logger.info(f"Added {len(df_new)} new records to {lang_code}.parquet")
                    else:
                        # Create new Parquet file
                        df_new.to_parquet(parquet_path, index=False)
                        logger.info(f"Created new Parquet file for {lang_code} with {len(df_new)} records")

            except Exception as e:
                logger.error(f"Error processing language {lang_code}: {str(e)}")
                continue

    except Exception as e:
        logger.error(f"Error updating Parquet files: {str(e)}")
        raise

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    update_parquet_files()
