#!/usr/bin/env python3
"""
Script to load transcripts from CSV files into the PostgreSQL database.
This script reads transcript CSV files and populates the transcriptions_{language} tables.
"""

import os
import sys
import csv
import logging
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection
DATABASE_URL = os.getenv("POSTGRES_URL")
if not DATABASE_URL:
    logger.error("POSTGRES_URL environment variable not set")
    sys.exit(1)

engine = create_engine(DATABASE_URL)

# Language code mapping for transcript files - All 22 languages
TRANSCRIPT_FILE_MAPPING = {
    'as': 'transcript/as_transcript.csv',
    'bn': 'transcript/bn_transcript.csv',
    'bdo': 'transcript/bdo_transcript.csv',
    'doi': 'transcript/doi_transcript.csv',
    'gu': 'transcript/gu_transcript.csv',
    'hi': 'transcript/hi_transcript.csv',
    'kn': 'transcript/kn_transcript.csv',
    'ks': 'transcript/ks_transcript.csv',
    'kok': 'transcript/kok_transcript.csv',
    'mai': 'transcript/mai_transcript.csv',
    'ml': 'transcript/mal_transcription.csv',
    'mr': 'transcript/mr_transcript.csv',
    'mni': 'transcript/mni_transcript.csv',
    'ne': 'transcript/ne_transcript.csv',
    'ol': 'transcript/ol_transcript.csv',
    'or': 'transcript/or_transcript.csv',
    'pa': 'transcript/pa_transcript.csv',
    'sa': 'transcript/sa_transcript.csv',
    'sd': 'transcript/sd_transcript.csv',
    'ta': 'transcript/ta_transcript.csv',
    'te': 'transcript/te_transcript.csv',
    'ur': 'transcript/ur_transcript.csv',
}

def get_table_columns(conn, language_code):
    """Get existing columns in the table"""
    try:
        result = conn.execute(text(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'transcriptions_{language_code}'
            AND table_schema = 'public'
        """))
        columns = [row[0] for row in result]
        return columns
    except Exception as e:
        logger.warning(f"Error getting columns for transcriptions_{language_code}: {e}")
        return []

def ensure_transcription_table(conn, language_code):
    """Ensure the transcription table exists with the correct schema"""
    # Check if table exists
    table_exists = conn.execute(text(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'transcriptions_{language_code}'
        )
    """)).scalar()

    if not table_exists:
        logger.info(f"Creating table transcriptions_{language_code}")
        conn.execute(text(f"""
            CREATE TABLE transcriptions_{language_code} (
                transcription_id SERIAL PRIMARY KEY,
                user_id VARCHAR(255),
                transcription_text TEXT NOT NULL,
                recorded BOOLEAN DEFAULT false,
                domain VARCHAR(10),
                subdomain VARCHAR(10),
                uploaded_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.commit()
        logger.info(f"Created table transcriptions_{language_code}")
    else:
        # Table exists - check and add missing columns
        columns = get_table_columns(conn, language_code)
        
        if 'recorded' not in columns:
            conn.execute(text(f"""
                ALTER TABLE transcriptions_{language_code}
                ADD COLUMN recorded BOOLEAN DEFAULT false
            """))
            conn.commit()
            logger.info(f"Added 'recorded' column to transcriptions_{language_code}")
        
        if 'user_id' not in columns:
            conn.execute(text(f"""
                ALTER TABLE transcriptions_{language_code}
                ADD COLUMN user_id VARCHAR(255)
            """))
            conn.commit()
            logger.info(f"Added 'user_id' column to transcriptions_{language_code}")
        
        if 'domain' not in columns:
            conn.execute(text(f"""
                ALTER TABLE transcriptions_{language_code}
                ADD COLUMN domain VARCHAR(10)
            """))
            conn.commit()
            logger.info(f"Added 'domain' column to transcriptions_{language_code}")
        
        if 'subdomain' not in columns:
            conn.execute(text(f"""
                ALTER TABLE transcriptions_{language_code}
                ADD COLUMN subdomain VARCHAR(10)
            """))
            conn.commit()
            logger.info(f"Added 'subdomain' column to transcriptions_{language_code}")

def load_transcripts_from_csv(language_code, csv_file_path, user_id='admin'):
    """Load transcripts from a CSV file into the database"""
    
    if not os.path.exists(csv_file_path):
        logger.warning(f"CSV file not found: {csv_file_path}")
        return 0
    
    try:
        with engine.connect() as conn:
            # Ensure table exists with correct schema
            ensure_transcription_table(conn, language_code)
            
            # Get actual columns in the table
            columns = get_table_columns(conn, language_code)
            
            # Count existing transcripts
            existing_count = conn.execute(text(f"""
                SELECT COUNT(*) FROM transcriptions_{language_code}
            """)).scalar()
            
            logger.info(f"Existing transcripts for {language_code}: {existing_count}")
            
            # Determine which columns to use
            has_user_id = 'user_id' in columns
            has_recorded = 'recorded' in columns
            
            # Read and insert transcripts
            inserted_count = 0
            
            # Try different encodings
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
            content = None
            
            for encoding in encodings:
                try:
                    with open(csv_file_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    logger.info(f"Successfully read file with encoding: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            
            if content is None:
                logger.error(f"Could not read file with any encoding: {csv_file_path}")
                return 0
            
            # Parse CSV - each line is a separate transcript
            lines = content.splitlines()
            
            # Begin transaction for bulk insert
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Handle CSV format - take first column
                try:
                    # Try to parse as CSV row
                    reader = csv.reader([line])
                    row = next(reader)
                    transcript_text = row[0].strip() if row else ''
                except:
                    # If CSV parsing fails, treat entire line as transcript
                    transcript_text = line
                
                if transcript_text and transcript_text.lower() != 'transcript':
                    try:
                        # Build dynamic insert based on available columns
                        if has_user_id and has_recorded:
                            conn.execute(text(f"""
                                INSERT INTO transcriptions_{language_code} 
                                (user_id, transcription_text, recorded)
                                VALUES (:user_id, :transcription_text, false)
                            """), {
                                "user_id": user_id,
                                "transcription_text": transcript_text
                            })
                        elif has_recorded:
                            conn.execute(text(f"""
                                INSERT INTO transcriptions_{language_code} 
                                (transcription_text, recorded)
                                VALUES (:transcription_text, false)
                            """), {
                                "transcription_text": transcript_text
                            })
                        else:
                            conn.execute(text(f"""
                                INSERT INTO transcriptions_{language_code} 
                                (transcription_text)
                                VALUES (:transcription_text)
                            """), {
                                "transcription_text": transcript_text
                            })
                        inserted_count += 1
                    except Exception as e:
                        logger.warning(f"Error inserting transcript: {e}")
            
            conn.commit()
            logger.info(f"Inserted {inserted_count} transcripts for {language_code}")
            
            return inserted_count
            
    except Exception as e:
        logger.error(f"Error loading transcripts for {language_code}: {e}")
        return 0

def get_table_stats():
    """Get statistics about transcription tables"""
    try:
        with engine.connect() as conn:
            # Get all transcription tables
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name LIKE 'transcriptions_%'
                AND table_schema = 'public'
            """))
            
            tables = [row[0] for row in result]
            
            stats = {}
            for table in tables:
                try:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    lang_code = table.replace('transcriptions_', '')
                    stats[lang_code] = count
                except Exception as e:
                    logger.warning(f"Error getting count for {table}: {e}")
            
            return stats
            
    except Exception as e:
        logger.error(f"Error getting table stats: {e}")
        return {}

def main():
    """Main function to load all transcripts"""
    logger.info("Starting transcript loading...")
    
    # Get stats before loading
    logger.info("Current transcription table stats:")
    stats = get_table_stats()
    for lang, count in stats.items():
        logger.info(f"  {lang}: {count} transcripts")
    
    total_inserted = 0
    
    # Load each transcript file
    for lang_code, file_path in TRANSCRIPT_FILE_MAPPING.items():
        logger.info(f"\nProcessing {lang_code}: {file_path}")
        count = load_transcripts_from_csv(lang_code, file_path)
        total_inserted += count
    
    # Get stats after loading
    logger.info("\n\nFinal transcription table stats:")
    stats = get_table_stats()
    for lang, count in stats.items():
        logger.info(f"  {lang}: {count} transcripts")
    
    logger.info(f"\nTotal transcripts inserted: {total_inserted}")
    logger.info("Transcript loading complete!")

if __name__ == "__main__":
    main()

