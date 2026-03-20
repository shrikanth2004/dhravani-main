import logging
import os
from pathlib import Path
import json
import pandas as pd
from sqlalchemy import text, select, Table, Column, Integer, String, Text
from database_manager import engine, metadata_db, get_language_table, ensure_transcription_table
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

                    recordings_table = get_language_table(lang_code)
                    ensure_transcription_table(conn, lang_code)
                    conn.commit()
                    conn.commit()
                    transcription_table = Table(
                        f"transcriptions_{lang_code}", metadata_db,
                        Column('transcription_id', Integer, primary_key=True),
                        Column('transcription_text', Text),
                        extend_existing=True
                    )
                    
                    # Ensure new columns exist for existing tables
                    conn.execute(text(f"ALTER TABLE recordings_{lang_code} ADD COLUMN IF NOT EXISTS user_info TEXT"))
                    conn.execute(text(f"ALTER TABLE recordings_{lang_code} ADD COLUMN IF NOT EXISTS audio_sampling_rate INTEGER"))
                    conn.execute(text(f"ALTER TABLE recordings_{lang_code} ADD COLUMN IF NOT EXISTS audio_file_name VARCHAR"))
                    conn.commit()
                    
                    query = (
                        select(
                            recordings_table.c.user_id,
                            recordings_table.c.audio_filename.label('file_name'),
                            transcription_table.c.transcription_text.label('transcription'),
                            recordings_table.c.speaker_name,
                            recordings_table.c.audio_path.label('audio'),
                            recordings_table.c.sampling_rate,
                            recordings_table.c.duration,
                            recordings_table.c.language,
                            recordings_table.c.gender,
                            recordings_table.c.country,
                            recordings_table.c.state,
                            recordings_table.c.city,
                            recordings_table.c.status,
                            recordings_table.c.verified_by,
                            recordings_table.c.age,
                            recordings_table.c.accent,
                            recordings_table.c.mother_tongue,
                            recordings_table.c.user_info,
                            recordings_table.c.audio_sampling_rate,
                            recordings_table.c.audio_file_name
                        )
                        .select_from(
                            recordings_table.outerjoin(
                                transcription_table,
                                recordings_table.c.transcription_id == transcription_table.c.transcription_id
                            )
                        )
                        .where(recordings_table.c.status == 'verified')
                    )
                    
                    # Read verified records into DataFrame
                    df_new = pd.read_sql(query, conn)
                    
                    if not df_new.empty:
                        # Construct missing user_info for older records
                        def build_user_info(row):
                            if pd.isna(row.get('user_info')) or not row.get('user_info'):
                                user_info_dict = {
                                    'gender': str(row.get('gender', 'unknown')) if pd.notna(row.get('gender')) else 'unknown',
                                    'age': int(row.get('age', 0)) if pd.notna(row.get('age')) else 0,
                                    'country': str(row.get('country', 'unknown')) if pd.notna(row.get('country')) else 'unknown',
                                    'state': str(row.get('state', 'unknown')) if pd.notna(row.get('state')) else 'unknown',
                                    'city': str(row.get('city', 'unknown')) if pd.notna(row.get('city')) else 'unknown',
                                    'accent': str(row.get('accent', 'unknown')) if pd.notna(row.get('accent')) else 'unknown',
                                    'mother_tongue': str(row.get('mother_tongue', 'unknown')) if pd.notna(row.get('mother_tongue')) else 'unknown',
                                    'education': 'unknown',
                                    'district': 'unknown'
                                }
                                return json.dumps(user_info_dict)
                            return row.get('user_info')
                        
                        df_new['user_info'] = df_new.apply(build_user_info, axis=1)
                    
                    if df_new.empty:
                        logger.debug(f"No verified records for language: {lang_code}")
                        continue

                    # Enforce strict dtype schema so all parquets match tightly for Hugging Face
                    dtype_mapping = {
                        'user_id': 'string', 'file_name': 'string', 'transcription': 'string',
                        'speaker_name': 'string', 'audio': 'string', 'sampling_rate': 'Int64',
                        'duration': 'float64', 'language': 'string', 'gender': 'string',
                        'country': 'string', 'state': 'string', 'city': 'string',
                        'status': 'string', 'verified_by': 'string', 'age': 'Int64',
                        'accent': 'string', 'mother_tongue': 'string', 'user_info': 'string',
                        'audio_sampling_rate': 'Int64', 'audio_file_name': 'string'
                    }
                    
                    for col, dtype in dtype_mapping.items():
                        if col in df_new.columns:
                            try:
                                df_new[col] = df_new[col].astype(dtype)
                            except Exception as e:
                                logger.warning(f"Could not cast {col} to {dtype}: {e}")

                    # Create language directory if needed
                    lang_dir = BASE_DIR / lang_code
                    lang_dir.mkdir(parents=True, exist_ok=True)
                    
                    parquet_path = lang_dir / f"{lang_code}.parquet"
                    
                    # Always overwrite the parquet file with the latest full query results.
                    # This ensures that any new columns (like user_info) added to the database
                    # are correctly populated for all existing records, and schema is perfectly synced.
                    try:
                        df_new.to_parquet(parquet_path, index=False)
                        logger.info(f"Created/Updated Parquet file for {lang_code} with {len(df_new)} records")
                    except Exception as e:
                        logger.error(f"Failed to write parquet for {lang_code}: {e}")

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
