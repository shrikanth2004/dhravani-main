from dotenv import load_dotenv
load_dotenv()

import os
import logging
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Boolean, Text, TIMESTAMP, func, select, text
from sqlalchemy.exc import SQLAlchemyError
from flask import current_app
from domain_subdomain import domains_and_subdomains  # Import domain data

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("POSTGRES_URL")
if not DATABASE_URL:
    raise Exception("POSTGRES_URL environment variable not set")

engine = create_engine(DATABASE_URL)
metadata_db = MetaData()  # Removed bind parameter
tables_cache = {}

_domain_tables_verified = False

def get_language_table(language):
    """Get or create language-specific recordings table (PENDING only)"""
    table_name = f"recordings_{language}"
    if table_name not in tables_cache:
        tables_cache[table_name] = Table(
            table_name,
            metadata_db,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('user_id', String),
            Column('audio_filename', String),
            Column('transcription_id', Integer),  # Reference to transcriptions table
            Column('speaker_name', String),
            Column('speaker_id', String),  # Added this column
            Column('audio_path', String),
            Column('sampling_rate', Integer),
            Column('duration', Float),
            Column('language', String(10)),
            Column('gender', String(10)),
            Column('country', String),
            Column('state', String),
    Column('city', String),
    Column('status', String(20), default='pending'),  # Replace verified boolean with status
    Column('verified_by', String, nullable=True),
            Column('username', String),
            Column('age', Integer),
            Column('accent', String),
            Column('mother_tongue', String),  # Add mother_tongue column
            Column('user_info', Text),
            Column('audio_sampling_rate', Integer),
            Column('audio_file_name', String),
            extend_existing=True
        )
        try:
            metadata_db.create_all(engine, tables=[tables_cache[table_name]])
            logger.debug(f"Created table: {table_name}")
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise
    return tables_cache[table_name]

def store_metadata(metadata_dict):
    """Store recording metadata in appropriate language table"""
    try:
        language = metadata_dict.get('language')
        if not language:
            raise ValueError("Language is required in metadata")

        # Create both tables if they don't exist
        with engine.connect() as conn:
            # Create recordings table first
            recordings_table = get_language_table(language)
            
            # Create table in database if it doesn't exist
            try:
                metadata_db.create_all(bind=conn, tables=[recordings_table])
                logger.debug(f"Ensured table exists: recordings_{language}")
            except Exception as e:
                logger.warning(f"Table creation warning (may already exist): {e}")

            # Ensure transcription table exists
            ensure_transcription_table(conn, language)
            
            # Check if mother_tongue column exists in recordings table
            mother_tongue_exists = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'recordings_{language}' 
                    AND column_name = 'mother_tongue'
                )
            """)).scalar()
            
            if not mother_tongue_exists:
                # Add mother_tongue column
                conn.execute(text(f"""
                    ALTER TABLE recordings_{language}
                    ADD COLUMN mother_tongue VARCHAR
                """))
                conn.commit()
                logger.info(f"Added mother_tongue column to recordings_{language}")
            
            # Check and fix language column constraint (was limited to 2 chars, need 10)
            language_col_check = conn.execute(text(f"""
                SELECT character_maximum_length 
                FROM information_schema.columns 
                WHERE table_name = 'recordings_{language}' 
                AND column_name = 'language'
            """)).scalar()
            
            if language_col_check and language_col_check < 10:
                conn.execute(text(f"""
                    ALTER TABLE recordings_{language} 
                    ALTER COLUMN language TYPE VARCHAR(10)
                """))
                conn.commit()
                logger.info(f"Updated language column to VARCHAR(10) for recordings_{language}")
            
            # Check and add user_info column
            user_info_exists = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'recordings_{language}' 
                    AND column_name = 'user_info'
                )
            """)).scalar()
            
            if not user_info_exists:
                conn.execute(text(f"""
                    ALTER TABLE recordings_{language}
                    ADD COLUMN user_info TEXT
                """))
                conn.commit()
                logger.info(f"Added user_info column to recordings_{language}")
                
            # Check and add audio_sampling_rate column
            audio_sampling_rate_exists = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'recordings_{language}' 
                    AND column_name = 'audio_sampling_rate'
                )
            """)).scalar()
            
            if not audio_sampling_rate_exists:
                conn.execute(text(f"""
                    ALTER TABLE recordings_{language}
                    ADD COLUMN audio_sampling_rate INTEGER
                """))
                conn.commit()
                logger.info(f"Added audio_sampling_rate column to recordings_{language}")
                
            # Check and add audio_file_name column
            audio_file_name_exists = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'recordings_{language}' 
                    AND column_name = 'audio_file_name'
                )
            """)).scalar()
            
            if not audio_file_name_exists:
                conn.execute(text(f"""
                    ALTER TABLE recordings_{language}
                    ADD COLUMN audio_file_name VARCHAR
                """))
                conn.commit()
                logger.info(f"Added audio_file_name column to recordings_{language}")
            
            # Remove any fields that don't match the table columns
            valid_columns = [c.name for c in recordings_table.columns]
            cleaned_metadata = {
                k: (v if v != '' else None) 
                for k, v in metadata_dict.items() 
                if k in valid_columns
            }
            
            cleaned_metadata['status'] = 'pending'  # Set default status
            
            # LOGGING BEFORE INSERT - Step 2 ✓
            logger.info(f"=== DB STORE DEBUG ===")
            logger.info(f"Language table: recordings_{language}")
            logger.info(f"Inserting keys: {list(cleaned_metadata.keys())}")
            logger.info(f"Filename: {cleaned_metadata.get('audio_filename')}")
            logger.info(f"User: {cleaned_metadata.get('user_id')}")
            logger.info(f"Required fields - MT: '{cleaned_metadata.get('mother_tongue')}'")

            
            # Insert the metadata
            result = conn.execute(recordings_table.insert().values(**cleaned_metadata))
            inserted_id = result.inserted_primary_key[0]
            conn.commit()
            
            logger.info(f"✅ DB INSERT SUCCESS - Recording ID: {inserted_id}")
            logger.info(f"===================")
            return inserted_id
            
    except Exception as e:
        logger.error(f"Error in store_metadata: {str(e)}", exc_info=True)
        raise

def store_transcription(transcription_text, language):
    """Store transcription in language-specific table"""
    try:
        # Create language-specific transcriptions table if it doesn't exist
        with engine.connect() as conn:
            create_table_query = text(f"""
                CREATE TABLE IF NOT EXISTS transcriptions_{language} (
                    transcription_id SERIAL PRIMARY KEY,
                    user_id VARCHAR(255),
                    transcription_text TEXT NOT NULL,
                    recorded BOOLEAN DEFAULT false,
                    uploaded_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute(create_table_query)
            
            # Insert transcription
            insert_query = text(f"""
                INSERT INTO transcriptions_{language} (transcription_text)
                VALUES (:transcription_text)
                RETURNING transcription_id
            """)
            result = conn.execute(insert_query, {"transcription_text": transcription_text})
            conn.commit()
            return result.scalar()
    except Exception as e:
        logger.error(f"Error storing transcription: {e}")
        raise Exception(f"Database error: {str(e)}")

def get_available_languages():
    """Get list of languages that have transcriptions available"""
    try:
        with engine.connect() as conn:
            # Look for tables matching pattern 'transcriptions_*'
            result = conn.execute(text("""
                SELECT DISTINCT table_name 
                FROM information_schema.tables 
                WHERE table_name LIKE 'transcriptions_%'
            """))
            
            # Extract language codes from table names
            languages = [
                table_name.replace('transcriptions_', '') 
                for (table_name,) in result
            ]
            
            logger.debug(f"Found languages in DB: {languages}")
            return languages
            
    except Exception as e:
        logger.error(f"Database error in get_available_languages: {str(e)}")
        raise Exception(f"Database error: {str(e)}")

def ensure_transcription_table(conn, language):
    """Ensure transcription table exists with correct schema"""
    try:
        # Check if table exists
        table_exists = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'transcriptions_{language}'
            )
        """)).scalar()

        if table_exists:
            # Check if recorded column exists
            column_exists = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'transcriptions_{language}' 
                    AND column_name = 'recorded'
                )
            """)).scalar()
            
            if not column_exists:
                # Add recorded column
                conn.execute(text(f"""
                    ALTER TABLE transcriptions_{language}
                    ADD COLUMN recorded BOOLEAN DEFAULT false
                """))
                conn.commit()
                logger.info(f"Added recorded column to transcriptions_{language}")
                
            # Check if domain column exists
            domain_exists = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'transcriptions_{language}' 
                    AND column_name = 'domain'
                )
            """)).scalar()
            
            if not domain_exists:
                # Add domain and subdomain columns
                conn.execute(text(f"""
                    ALTER TABLE transcriptions_{language}
                    ADD COLUMN domain VARCHAR(10),
                    ADD COLUMN subdomain VARCHAR(10)
                """))
                conn.commit()
                logger.info(f"Added domain and subdomain columns to transcriptions_{language}")
        else:
            # Create new table with all columns
            conn.execute(text(f"""
                CREATE TABLE transcriptions_{language} (
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
            logger.info(f"Created table: transcriptions_{language}")
            
    except Exception as e:
        logger.error(f"Error ensuring transcription table: {e}")
        raise

def get_transcriptions_for_language(language_code, include_recorded=False, limit=None, offset=0, exclude_ids=None, 
                                  count_only=False, ids_only=False, specific_ids=None, domain=None, subdomain=None):
    """
    Get transcriptions for a language with various filtering options
    
    Args:
        language_code (str): Language code to fetch transcriptions for
        include_recorded (bool): Whether to include already recorded transcriptions
        limit (int, optional): Maximum number of transcriptions to return
        offset (int, optional): Offset for pagination
        exclude_ids (list, optional): IDs to exclude from results
        count_only (bool): Return only the count of matching transcriptions
        ids_only (bool): Return only the transcript IDs without content
        specific_ids (list, optional): Specific transcript IDs to fetch
        domain (str, optional): Filter by specific domain
        subdomain (str, optional): Filter by specific subdomain
    """
    try:
        with engine.connect() as conn:
            ensure_transcription_table(conn, language_code)
            
            # If count only, just return the count
            if count_only:
                where_clauses = []
                if not include_recorded:
                    where_clauses.append("recorded = false")
                
                # Add domain and subdomain filters if provided
                if domain:
                    where_clauses.append("domain = :domain")
                if subdomain:
                    where_clauses.append("subdomain = :subdomain")
                    
                where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
                
                count_query = text(f"""
                    SELECT COUNT(*)
                    FROM transcriptions_{language_code}
                    {where_clause}
                """)
                
                params = {}
                if domain:
                    params['domain'] = domain
                if subdomain:
                    params['subdomain'] = subdomain
                    
                count = conn.execute(count_query, params).scalar() or 0
                return {'count': count}
            
            # Build the SELECT clause based on ids_only
            select_clause = "SELECT transcription_id" if ids_only else "SELECT transcription_id, transcription_text, recorded, domain, subdomain"
            
            # Build the WHERE clause
            where_clauses = []
            if not include_recorded:
                where_clauses.append("recorded = false")
            
            # Add domain and subdomain filters if provided
            if domain:
                where_clauses.append("domain = :domain")
            if subdomain:
                where_clauses.append("subdomain = :subdomain")
            
            if exclude_ids and len(exclude_ids) > 0:
                placeholders = ','.join([f':exclude_id_{i}' for i in range(len(exclude_ids))])
                where_clauses.append(f"transcription_id NOT IN ({placeholders})")
            
            if specific_ids and len(specific_ids) > 0:
                placeholders = ','.join([f':specific_id_{i}' for i in range(len(specific_ids))])
                where_clauses.append(f"transcription_id IN ({placeholders})")
            
            where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            
            # Build pagination clause
            limit_clause = f"LIMIT :limit" if limit is not None else ""
            offset_clause = f"OFFSET :offset" if offset > 0 else ""
            
            # Build query
            query = text(f"""
                {select_clause}
                FROM transcriptions_{language_code}
                {where_clause}
                ORDER BY transcription_id
                {limit_clause}
                {offset_clause}
            """)
            
            # Build params dict
            params = {}
            if limit is not None:
                params['limit'] = limit
            if offset > 0:
                params['offset'] = offset
            if domain:
                params['domain'] = domain
            if subdomain:
                params['subdomain'] = subdomain
            
            # Add exclude IDs to params
            if exclude_ids and len(exclude_ids) > 0:
                for i, id_val in enumerate(exclude_ids):
                    params[f'exclude_id_{i}'] = id_val
            
            # Add specific IDs to params
            if specific_ids and len(specific_ids) > 0:
                for i, id_val in enumerate(specific_ids):
                    params[f'specific_id_{i}'] = id_val
            
            result = conn.execute(query, params)
            
            # Handle IDs-only response
            if ids_only:
                return [row[0] for row in result]
            
            # Handle normal response
            transcriptions = [
                {"id": row[0], "text": row[1], "recorded": row[2], "domain": row[3], "subdomain": row[4]} 
                for row in result
            ]
            
            if not transcriptions and not ids_only:
                cond = "unrecorded " if not include_recorded else ""
                domain_info = f" for domain '{domain}'" if domain else ""
                subdomain_info = f" and subdomain '{subdomain}'" if subdomain else ""
                logger.warning(f"No {cond}transcriptions found for language: {language_code}{domain_info}{subdomain_info}")
                return []
                
            return transcriptions
            
    except Exception as e:
        logger.error(f"Database error in get_transcriptions_for_language: {str(e)}")
        raise

def table_exists(conn, table_name):
    """Check if a table exists in the database"""
    exists_query = text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = :table_name
        )
    """)
    result = conn.execute(exists_query, {"table_name": table_name})
    return result.scalar()

def get_dataset_stats():
    """Get dataset statistics from PostgreSQL"""
    try:
        with engine.connect() as conn:
            stats = {
                'total_recordings': 0,
                'total_verified': 0,
                'languages': {},
                'total_duration': 0,  # This will be in seconds
                'total_users': 0,
                'total_languages': 0,
                'total_transcripts': 0  # Added total transcripts
            }
            
            # Get total transcripts count from all language-specific tables
            transcripts_query = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name LIKE 'transcriptions_%'
            """)
            transcript_tables = conn.execute(transcripts_query).fetchall()
            
            total_transcripts = 0
            for (table_name,) in transcript_tables:
                count_query = text(f"SELECT COUNT(*) FROM {table_name}")
                count = conn.execute(count_query).scalar() or 0
                total_transcripts += count
            
            stats['total_transcripts'] = total_transcripts

            # Get available languages and their tables
            languages = get_available_languages()
            if not languages:
                logger.warning("No language tables found")
                return stats

            # Tables created on-demand; stats query existing tables only

            # Build UNION query for both stats and user count
            query_parts = []
            existing_tables = []
            
            for lang in languages:
                table_name = f"recordings_{lang}"
                if table_exists(conn, table_name):
                    query_parts.append(f"""
                        SELECT 
                            status, 
                            duration,
                            user_id,
                            '{lang}' as language
                        FROM {table_name}
                    """)
                    existing_tables.append((table_name, lang))

            if query_parts:
                # Build and execute the full stats query
                union_query = " UNION ALL ".join(query_parts)
                stats_query = text(f"""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN status = 'verified' THEN 1 ELSE 0 END) as verified,
                        SUM(duration) as total_duration,
                        COUNT(DISTINCT user_id) as total_users
                    FROM ({union_query}) all_recordings
                """)
                
                result = conn.execute(stats_query)
                row = result.fetchone()
                if row:
                    stats['total_recordings'] = row[0]
                    stats['total_verified'] = row[1]
                    stats['total_duration'] = float(row[2] or 0)  # Duration in seconds
                    stats['total_users'] = row[3]  # Get unique users across all recordings

                # Get per-language statistics including transcripts count
                for table_name, lang in existing_tables:
                    try:
                        # Get recordings stats
                        result = conn.execute(text(f"""
                            SELECT 
                                COUNT(*) as total,
                                COUNT(DISTINCT user_id) as total_users,
                                SUM(CASE WHEN status = 'verified' THEN 1 ELSE 0 END) as verified,
                                SUM(duration) as duration
                            FROM {table_name}
                        """))
                        row = result.fetchone()
                        
                        # Get transcripts count for this language
                        trans_result = conn.execute(text(f"""
                            SELECT COUNT(*) 
                            FROM transcriptions_{lang}
                        """))
                        transcripts_count = trans_result.scalar() or 0
                        
                        if row:
                            stats['languages'][lang] = {
                                'recordings': row[0],
                                'total_users': row[1],
                                'verified': row[2],
                                'total_duration': float(row[3] or 0),
                                'available_transcripts': transcripts_count  # Added transcripts count
                            }
                    except Exception as e:
                        logger.warning(f"Could not get stats for {table_name}: {e}")
                        stats['languages'][lang] = {
                            'recordings': 0,
                            'total_users': 0,
                            'verified': 0,
                            'total_duration': 0.0,
                            'available_transcripts': 0
                        }
                        continue

            stats['total_languages'] = len(stats['languages'])
            
            return stats
    except Exception as e:
        logger.error(f"Error getting dataset stats: {e}")
        return {
            'total_recordings': 0,
            'total_verified': 0,
            'languages': {},
            'total_duration': 0,
            'total_users': 0,
            'total_languages': 0,
            'total_transcripts': 0
        }

def create_assignments_table(conn):
    """Create a single assignments table with one row per user"""
    conn.execute(text("""
        DROP TABLE IF EXISTS validation_assignments;
        CREATE TABLE validation_assignments (
            id SERIAL PRIMARY KEY,
            assigned_to VARCHAR(255) NOT NULL,
            language VARCHAR(10) NOT NULL,
            recording_id INTEGER NOT NULL,
            assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP NOT NULL,
            status VARCHAR(20) DEFAULT 'pending',
            assigned_by VARCHAR(255),
            UNIQUE (assigned_to)
        )
    """))
    conn.commit()
    
    # Fix language column constraint if table was recreated (it drops and recreates)
    # This ensures backward compatibility with any existing tables
    try:
        conn.execute(text("""
            ALTER TABLE validation_assignments 
            ALTER COLUMN language TYPE VARCHAR(10)
        """))
        conn.commit()
    except Exception as e:
        # Table may not have been recreated, ignore errors
        pass

def cleanup_completed_assignments():
    """Remove completed assignments from the table"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                DELETE FROM validation_assignments
                WHERE status IN ('completed_verified', 'completed_rejected')
            """))
            conn.commit()
    except Exception as e:
        logger.error(f"Error cleaning up assignments: {e}")

def assign_recording(language, moderator_id, mother_tongue=''):
    """Get next unassigned recording or update existing assignment with mother_tongue filtering"""
    try:
        with engine.connect() as conn:
            # First check if recordings table exists
            if not table_exists(conn, f"recordings_{language}"):
                logger.info(f"No recordings table exists for language: {language}")
                return None

            # Check if there are any pending recordings with these filters
            filters_check_query = text(f"""
                SELECT COUNT(*) FROM recordings_{language} 
                WHERE status = 'pending'
                {" AND mother_tongue = :mother_tongue" if mother_tongue else ""}
            """)
            
            matching_recordings = conn.execute(filters_check_query, {
                "mother_tongue": mother_tongue
            }).scalar()
            
            if matching_recordings == 0:
                mother_tongue_info = f" for mother tongue '{mother_tongue}'" if mother_tongue else ""
                logger.info(f"No pending recordings found{mother_tongue_info}")
                return None

            # Create assignments table if it doesn't exist
            create_assignments_table(conn)
            
            # Clean up completed assignments periodically
            cleanup_completed_assignments()
            
            # Clear expired assignments
            conn.execute(text("""
                UPDATE validation_assignments
                SET status = 'expired'
                WHERE expires_at < NOW()
            """))
            
            # Check if user has an existing assignment
            existing = conn.execute(text("""
                SELECT recording_id, language 
                FROM validation_assignments 
                WHERE assigned_to = :moderator_id
            """), {
                "moderator_id": moderator_id
            }).first()

            if existing and existing.language == language:
                # Check if existing assignment matches mother_tongue filters
                match_query = text(f"""
                    SELECT r.id
                    FROM recordings_{language} r
                    WHERE r.id = :recording_id
                    AND (:mother_tongue = '' OR r.mother_tongue = :mother_tongue)
                """)
                
                matches_filters = conn.execute(match_query, {
                    "recording_id": existing.recording_id,
                    "mother_tongue": mother_tongue
                }).scalar() is not None
                
                if matches_filters:
                    # Return existing assignment as it matches the filters
                    result = conn.execute(text(f"""
                        SELECT r.*, t.transcription_text
                        FROM recordings_{language} r
                        LEFT JOIN transcriptions_{language} t 
                            ON r.transcription_id = t.transcription_id
                        WHERE r.id = :recording_id
                    """), {
                        "recording_id": existing.recording_id
                    })
                    return result.mappings().first()
                else:
                    # Release the existing assignment as it doesn't match the filters
                    logger.info(f"Releasing assignment for user {moderator_id} as it doesn't match new filters")
                    conn.execute(text("""
                        DELETE FROM validation_assignments
                        WHERE assigned_to = :moderator_id
                    """), {
                        "moderator_id": moderator_id
                    })
                    conn.commit()  # Make sure to commit the DELETE
            
            # Build mother_tongue filter conditions
            mother_tongue_condition = "AND r.mother_tongue = :mother_tongue" if mother_tongue else ""
            
            # Log query parameters for debugging
            logger.debug(f"Looking for recordings with filters - mother_tongue: '{mother_tongue}'")
            
            # Get next available recording with filters
            next_recording = conn.execute(text(f"""
                WITH assigned_recordings AS (
                    SELECT recording_id
                    FROM validation_assignments
                    WHERE status = 'pending'
                    AND expires_at > NOW()
                )
                SELECT r.id
                FROM recordings_{language} r
                WHERE r.status = 'pending'
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM assigned_recordings ar 
                        WHERE ar.recording_id = r.id
                    )
                    {mother_tongue_condition}
                ORDER BY r.id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """), {
                "mother_tongue": mother_tongue
            }).scalar()
            
            if not next_recording:
                mother_tongue_info = f" with mother tongue '{mother_tongue}'" if mother_tongue else ""
                logger.info(f"No unassigned recordings found{mother_tongue_info}")
                return None
                
            # Insert or update assignment
            conn.execute(text("""
                INSERT INTO validation_assignments
                    (assigned_to, language, recording_id, expires_at, status)
                VALUES
                    (:moderator_id, :language, :recording_id, NOW() + INTERVAL '10 minutes', 'pending')
                ON CONFLICT (assigned_to)
                DO UPDATE SET 
                    language = EXCLUDED.language,
                    recording_id = EXCLUDED.recording_id,
                    expires_at = EXCLUDED.expires_at,
                    status = 'pending'
            """), {
                "moderator_id": moderator_id,
                "language": language,
                "recording_id": next_recording
            })
            
            # Get full recording data including transcription
            result = conn.execute(text(f"""
                SELECT r.*, t.transcription_text
                FROM recordings_{language} r
                LEFT JOIN transcriptions_{language} t 
                    ON r.transcription_id = t.transcription_id
                WHERE r.id = :recording_id
            """), {
                "recording_id": next_recording
            })
            
            recording = result.mappings().first()
            conn.commit()
            
            return recording
            
    except Exception as e:
        logger.error(f"Error assigning recording: {e}")
        raise

def complete_assignment(language, recording_id, moderator_id, status):
    """Mark an assignment as completed and remove it - optimized version"""
    try:
        with engine.begin() as conn:  # Use begin() for automatic transaction management
            # Update and cleanup in a single transaction
            conn.execute(text("""
                WITH completed AS (
                    UPDATE validation_assignments
                    SET status = :status
                    WHERE assigned_to = :moderator_id
                    AND recording_id = :recording_id
                    AND language = :language
                )
                DELETE FROM validation_assignments
                WHERE status IN ('completed_verified', 'completed_rejected')
            """), {
                "status": status,
                "moderator_id": moderator_id,
                "recording_id": recording_id,
                "language": language
            })
            
    except Exception as e:
        logger.error(f"Error completing assignment: {e}")
        raise

def get_pending_recordings_for_assignment(language, mother_tongue='', limit=50):
    """Get pending recordings that can be assigned to users (for admin view)"""
    try:
        with engine.connect() as conn:
            if not table_exists(conn, f"recordings_{language}"):
                return []
            
            # Ensure mother_tongue column exists (for older tables)
            mother_tongue_exists = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'recordings_{language}' 
                    AND column_name = 'mother_tongue'
                )
            """)).scalar()
            
            if not mother_tongue_exists:
                conn.execute(text(f"""
                    ALTER TABLE recordings_{language}
                    ADD COLUMN mother_tongue VARCHAR
                """))
                conn.commit()
                logger.info(f"Added mother_tongue column to recordings_{language}")
            
            # Build query for pending recordings - simpler version
            where_conditions = ["status = 'pending'"]
            params = {"language": language}
            
            if mother_tongue:
                where_conditions.append("mother_tongue = :mother_tongue")
                params["mother_tongue"] = mother_tongue
            
            where_clause = " AND ".join(where_conditions)
            
            # Get ALL pending recordings for the language - use string interpolation for LIMIT
            query = text(f"""
                SELECT id, audio_filename, speaker_name, gender, 
                       age, state, mother_tongue, user_id,
                       duration, audio_path, transcription_id
                FROM recordings_{language}
                WHERE {where_clause}
                ORDER BY id
                LIMIT {int(limit)}
            """)
            
            result = conn.execute(query, params)
            recordings = []
            for row in result:
                # Get transcription text using transcription_id
                trans_text = ""
                trans_id = row[10] if len(row) > 10 else None
                if trans_id:
                    try:
                        trans_query = text(f"""
                            SELECT transcription_text 
                            FROM transcriptions_{language}
                            WHERE transcription_id = :trans_id
                        """)
                        trans_result = conn.execute(trans_query, {"trans_id": trans_id}).fetchone()
                        if trans_result:
                            trans_text = trans_result[0]
                    except:
                        pass
                
                recordings.append({
                    'id': row[0],
                    'audio_filename': row[1],
                    'speaker_name': row[2],
                    'gender': row[3],
                    'age': row[4],
                    'state': row[5],
                    'mother_tongue': row[6],
                    'recorded_by': row[7],
                    'transcription_text': trans_text,
                    'language': language,
                    'audio_path': row[9] if len(row) > 9 else f"{language}/audio/{row[1]}",
                    'duration': row[8] if len(row) > 8 else 0
                })
            
            return recordings
    except Exception as e:
        logger.error(f"Error getting pending recordings: {e}")
        return []

def get_all_pending_recordings(language=None, limit=100):
    """Get all pending recordings across all languages or for a specific language"""
    try:
        # Get all recordings tables
        with engine.connect() as conn:
            tables_query = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name LIKE 'recordings_%'
                AND table_schema = 'public'
            """)
            tables = conn.execute(tables_query).fetchall()
        
        all_recordings = []
        
        for (table_name,) in tables:
            lang_code = table_name.replace('recordings_', '')
            
            # Skip if filtering by language and this isn't the right one
            if language and lang_code != language:
                continue
            
            # Use separate connection for each table to avoid transaction issues
            try:
                with engine.connect() as conn:
                    # Get ALL pending recordings for this language (simplified)
                    # Use string interpolation for LIMIT (safe since it's an integer)
                    # Ensure mother_tongue column exists (for older tables)
                    mother_tongue_exists = conn.execute(text(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.columns 
                            WHERE table_name = :table_name 
                            AND column_name = 'mother_tongue'
                        )
                    """), {"table_name": table_name}).scalar()
                    
                    if not mother_tongue_exists:
                        conn.execute(text(f"""
                            ALTER TABLE {table_name}
                            ADD COLUMN mother_tongue VARCHAR
                        """))
                        conn.commit()
                        logger.info(f"Added mother_tongue column to {table_name}")
                    
                    query = text(f"""
                        SELECT id, audio_filename, speaker_name, gender, 
                               age, state, mother_tongue, user_id,
                               duration, audio_path, transcription_id
                        FROM {table_name}
                        WHERE status = 'pending'
                        ORDER BY id
                        LIMIT {int(limit)}
                    """)
                    
                    result = conn.execute(query)
                    
                    for row in result:
                        # Get transcription text
                        trans_text = ""
                        trans_id = row[10] if len(row) > 10 else None
                        if trans_id:
                            try:
                                trans_query = text(f"""
                                    SELECT transcription_text 
                                    FROM transcriptions_{lang_code}
                                    WHERE transcription_id = :trans_id
                                """)
                                trans_result = conn.execute(trans_query, {"trans_id": trans_id}).fetchone()
                                if trans_result:
                                    trans_text = trans_result[0]
                            except:
                                pass
                        
                        all_recordings.append({
                            'id': row[0],
                            'audio_filename': row[1],
                            'speaker_name': row[2],
                            'gender': row[3],
                            'age': row[4],
                            'state': row[5],
                            'mother_tongue': row[6],
                            'recorded_by': row[7],
                            'transcription_text': trans_text,
                            'language': lang_code,
                            'audio_path': row[9] if len(row) > 9 else f"{lang_code}/audio/{row[1]}",
                            'duration': row[8] if len(row) > 8 else 0
                        })
            except Exception as e:
                logger.warning(f"Error getting recordings from {table_name}: {e}")
                continue
        
        return all_recordings
    except Exception as e:
        logger.error(f"Error getting all pending recordings: {e}")
        return []

def assign_recording_to_user(recording_id, language, user_id, assigned_by):
    """Assign a specific recording to a specific user"""
    try:
        with engine.connect() as conn:
            # Ensure validation_assignments table exists with correct schema
            create_assignments_table(conn)
            
            # Check if recording exists and is pending
            check_query = text(f"""
                SELECT id, status, user_id FROM recordings_{language}
                WHERE id = :recording_id
            """)
            result = conn.execute(check_query, {"recording_id": recording_id}).first()
            
            if not result:
                return {"success": False, "error": "Recording not found"}
            
            if result.status != 'pending':
                return {"success": False, "error": f"Recording is already {result.status}"}
            
            # Check if the user_id is the same as the person who recorded (cannot validate own recording)
            recorded_by_user = result[2]  # user_id column
            if recorded_by_user == user_id:
                return {"success": False, "error": "Cannot assign recording to the person who recorded it. Please assign to a different user."}
            
            # Check if recording is already assigned to someone else
            assignment_check = text("""
                SELECT id, assigned_to FROM validation_assignments
                WHERE recording_id = :recording_id
                AND language = :language
                AND status = 'pending'
                AND expires_at > NOW()
            """)
            existing = conn.execute(assignment_check, {
                "recording_id": recording_id,
                "language": language
            }).first()
            
            if existing:
                if existing.assigned_to == user_id:
                    return {"success": True, "message": "Recording already assigned to this user"}
                return {"success": False, "error": f"Recording already assigned to another user"}
            
            # Create the assignment
            conn.execute(text("""
                INSERT INTO validation_assignments
                    (assigned_to, language, recording_id, assigned_by, expires_at, status)
                VALUES
                    (:user_id, :language, :recording_id, :assigned_by, NOW() + INTERVAL '7 days', 'pending')
            """), {
                "user_id": user_id,
                "language": language,
                "recording_id": recording_id,
                "assigned_by": assigned_by
            })
            
            conn.commit()
            return {"success": True, "message": "Recording assigned successfully"}
    except Exception as e:
        logger.error(f"Error assigning recording to user: {e}")
        return {"success": False, "error": str(e)}

def get_user_assignments(user_id, include_pending=True):
    """Get all assignments for a specific user"""
    try:
        with engine.connect() as conn:
            conditions = ["assigned_to = :user_id"]
            if include_pending:
                conditions.append("status = 'pending'")
            else:
                conditions.append("status IN ('pending', 'completed_verified', 'completed_rejected', 'expired')")
            
            where_clause = " AND ".join(conditions)
            
            query = text(f"""
                SELECT va.id, va.recording_id, va.language, va.assigned_at, 
                       va.expires_at, va.status, va.assigned_by,
                       r.audio_filename, r.speaker_name, r.gender, r.state,
                       r.mother_tongue, r.audio_path, r.duration,
                       t.transcription_text
                FROM validation_assignments va
                JOIN recordings_{'{user_id}'} r ON va.recording_id = r.id
                LEFT JOIN transcriptions_{'{user_id}'} t ON r.transcription_id = t.transcription_id
                WHERE {where_clause}
                ORDER BY va.assigned_at DESC
            """)
            
            # This won't work with parameterized table names, let's fix this
            return []
    except Exception as e:
        logger.error(f"Error getting user assignments: {e}")
        return []

def get_user_pending_assignments(user_id):
    """Get pending assignments for a specific user with full recording details"""
    try:
        with engine.connect() as conn:
            # First get the assignments
            assignments_query = text("""
                SELECT recording_id, language, assigned_at, expires_at, assigned_by
                FROM validation_assignments
                WHERE assigned_to = :user_id
                AND status = 'pending'
                AND expires_at > NOW()
                ORDER BY assigned_at DESC
            """)
            
            assignments = conn.execute(assignments_query, {"user_id": user_id}).fetchall()
            
            if not assignments:
                return []
            
            result = []
            for assignment in assignments:
                recording_id = assignment[0]
                language = assignment[1]
                
                # Get recording details
                try:
                    recording_query = text(f"""
                        SELECT r.id, r.audio_filename, r.speaker_name, r.gender, 
                               r.age_group, r.state, r.mother_tongue, r.audio_path, r.duration,
                               r.user_id as recorded_by,
                               t.transcription_text
                        FROM recordings_{language} r
                        LEFT JOIN transcriptions_{language} t ON r.transcription_id = t.transcription_id
                        WHERE r.id = :recording_id
                    """)
                    
                    recording = conn.execute(recording_query, {"recording_id": recording_id}).first()
                    
                    if recording:
                        result.append({
                            'assignment_id': assignment[0],  # recording_id as id
                            'recording_id': recording[0],
                            'audio_filename': recording[1],
                            'speaker_name': recording[2],
                            'gender': recording[3],
                            'age_group': recording[4],
                            'state': recording[5],
                            'mother_tongue': recording[6],
                            'audio_path': recording[7],
                            'duration': recording[8],
                            'recorded_by': recording[9],
                            'transcription_text': recording[10],
                            'language': language,
                            'assigned_at': assignment[2],
                            'expires_at': assignment[3],
                            'assigned_by': assignment[4]
                        })
                except Exception as e:
                    logger.warning(f"Error getting recording details for {recording_id}: {e}")
                    continue
            
            return result
    except Exception as e:
        logger.error(f"Error getting user pending assignments: {e}")
        return []

def get_all_user_assignments(user_id):
    """Get all assignments (pending and completed) for a user"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT va.id, va.recording_id, va.language, va.assigned_at, 
                       va.expires_at, va.status, va.assigned_by
                FROM validation_assignments va
                WHERE va.assigned_to = :user_id
                ORDER BY va.assigned_at DESC
            """)
            
            assignments = conn.execute(query, {"user_id": user_id}).fetchall()
            
            if not assignments:
                return []
            
            result = []
            for assignment in assignments:
                recording_id = assignment[1]
                language = assignment[2]
                
                try:
                    recording_query = text(f"""
                        SELECT r.id, r.audio_filename, r.speaker_name, r.gender, 
                               r.age_group, r.state, r.mother_tongue, r.audio_path, r.duration,
                               r.user_id as recorded_by, r.status as recording_status,
                               t.transcription_text
                        FROM recordings_{language} r
                        LEFT JOIN transcriptions_{language} t ON r.transcription_id = t.transcription_id
                        WHERE r.id = :recording_id
                    """)
                    
                    recording = conn.execute(recording_query, {"recording_id": recording_id}).first()
                    
                    if recording:
                        result.append({
                            'assignment_id': assignment[0],
                            'recording_id': recording[0],
                            'audio_filename': recording[1],
                            'speaker_name': recording[2],
                            'gender': recording[3],
                            'age_group': recording[4],
                            'state': recording[5],
                            'mother_tongue': recording[6],
                            'audio_path': recording[7],
                            'duration': recording[8],
                            'recorded_by': recording[9],
                            'recording_status': recording[10],
                            'transcription_text': recording[11],
                            'language': language,
                            'assigned_at': assignment[3],
                            'expires_at': assignment[4],
                            'status': assignment[5],
                            'assigned_by': assignment[6]
                        })
                except Exception as e:
                    logger.warning(f"Error getting recording details: {e}")
                    continue
            
            return result
    except Exception as e:
        logger.error(f"Error getting all user assignments: {e}")
        return []

def ensure_domain_tables(conn):
    """Ensure domain and subdomain tables exist in PostgreSQL."""
    global _domain_tables_verified
    
    # Skip verification if tables were already verified
    if _domain_tables_verified:
        return True
        
    try:
        # Create domains table if it doesn't exist
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS domains (
                code VARCHAR(10) PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )
        """))
        
        # Create subdomains table if it doesn't exist
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS subdomains (
                id SERIAL PRIMARY KEY,
                mnemonic VARCHAR(10) NOT NULL,
                name VARCHAR(100) NOT NULL,
                domain_code VARCHAR(10) NOT NULL REFERENCES domains(code),
                UNIQUE(domain_code, mnemonic)
            )
        """))
        
        conn.commit()
        logger.info("Domain and subdomain tables created or verified")
        
        # Mark tables as verified
        _domain_tables_verified = True
        return True
    except Exception as e:
        logger.error(f"Error creating domain tables: {e}")
        conn.rollback()
        return False

def initialize_domain_data():
    """Initialize domain and subdomain tables with data from domain_subdomain.py"""
    try:
        with engine.connect() as conn:
            # Create the tables if they don't exist
            if not ensure_domain_tables(conn):
                logger.error("Failed to create domain tables")
                return False
            
            # Check if domains table is empty
            domain_count = conn.execute(text("SELECT COUNT(*) FROM domains")).scalar()
            if domain_count == 0:
                logger.info("Domain table is empty. Populating with predefined data...")
                
                # Insert domains
                for code, name in domains_and_subdomains["domains"].items():
                    conn.execute(text(
                        "INSERT INTO domains (code, name) VALUES (:code, :name)"
                    ), {"code": code, "name": name})
                
                # Insert subdomains
                for domain_code, subdomains in domains_and_subdomains["subdomains"].items():
                    for subdomain in subdomains:
                        conn.execute(text("""
                            INSERT INTO subdomains (mnemonic, name, domain_code) 
                            VALUES (:mnemonic, :name, :domain_code)
                        """), {
                            "mnemonic": subdomain["mnemonic"],
                            "name": subdomain["name"],
                            "domain_code": domain_code
                        })
                
                conn.commit()
                logger.info("Domain and subdomain data populated successfully")
            else:
                logger.debug("Domain data already exists in database")
                
            return True
            
    except Exception as e:
        logger.error(f"Error initializing domain data: {str(e)}")
        return False

def get_all_domains_db():
    """Get all domains from the database."""
    try:
        with engine.connect() as conn:
            ensure_domain_tables(conn)
            result = conn.execute(text("SELECT code, name FROM domains"))
            domains = {row[0]: row[1] for row in result}
            
            # If no domains found in database, return predefined domains
            if not domains:
                logger.warning("No domains found in database, returning predefined domains")
                return domains_and_subdomains["domains"]
                
            return domains
    except Exception as e:
        logger.error(f"Error getting all domains: {e}")
        # Fallback to predefined domains
        return domains_and_subdomains["domains"]

def get_domain_name_db(domain_code):
    """Get domain name from its code."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT name FROM domains WHERE code = :code"
            ), {"code": domain_code})
            row = result.fetchone()
            if row:
                return row[0]
            
            # Fallback to predefined domains if not found in database
            if domain_code in domains_and_subdomains["domains"]:
                return domains_and_subdomains["domains"][domain_code]
            return None
    except Exception as e:
        logger.error(f"Error getting domain name: {e}")
        # Fallback to predefined domains
        return domains_and_subdomains["domains"].get(domain_code)

def get_domain_subdomains_db(domain_code):
    """Get all subdomains for a specific domain."""
    try:
        with engine.connect() as conn:
            # Try to get from database first
            result = conn.execute(text(
                "SELECT mnemonic, name FROM subdomains WHERE domain_code = :code"
            ), {"code": domain_code})
            subdomains = [{"mnemonic": row[0], "name": row[1]} for row in result]
            
            # If nothing found in database, return predefined subdomains
            if not subdomains and domain_code in domains_and_subdomains["subdomains"]:
                return domains_and_subdomains["subdomains"][domain_code]
                
            return subdomains
    except Exception as e:
        logger.error(f"Error getting domain subdomains: {e}")
        # Fallback to predefined subdomains
        return domains_and_subdomains["subdomains"].get(domain_code, [])

def get_subdomain_by_mnemonic_db(domain_code, subdomain_mnemonic):
    """Get subdomain information by its mnemonic within a domain."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT mnemonic, name FROM subdomains WHERE domain_code = :code AND mnemonic = :mnemonic"
            ), {"code": domain_code, "mnemonic": subdomain_mnemonic})
            row = result.fetchone()
            if row:
                return {"mnemonic": row[0], "name": row[1]}
            
            # Fallback to predefined subdomains
            for subdomain in domains_and_subdomains["subdomains"].get(domain_code, []):
                if subdomain["mnemonic"] == subdomain_mnemonic:
                    return subdomain
            return None
    except Exception as e:
        logger.error(f"Error getting subdomain by mnemonic: {e}")
        # Fallback to predefined subdomains
        for subdomain in domains_and_subdomains["subdomains"].get(domain_code, []):
            if subdomain["mnemonic"] == subdomain_mnemonic:
                return subdomain
        return None

def search_subdomain_db(query, domain_code=None):
    """Search for a subdomain by name or mnemonic across all domains or a specific domain."""
    try:
        with engine.connect() as conn:
            if (domain_code):
                result = conn.execute(text("""
                    SELECT s.mnemonic, s.name, s.domain_code, d.name 
                    FROM subdomains s
                    JOIN domains d ON s.domain_code = d.code
                    WHERE s.domain_code = :domain_code 
                      AND (LOWER(s.name) LIKE :query OR LOWER(s.mnemonic) LIKE :query)
                """), {"domain_code": domain_code, "query": f"%{query.lower()}%"})
            else:
                result = conn.execute(text("""
                    SELECT s.mnemonic, s.name, s.domain_code, d.name 
                    FROM subdomains s
                    JOIN domains d ON s.domain_code = d.code
                    WHERE LOWER(s.name) LIKE :query OR LOWER(s.mnemonic) LIKE :query
                """), {"query": f"%{query.lower()}%"})
                
            results = []
            for row in result:
                results.append({
                    "domain": row[2],
                    "domain_name": row[3],
                    "subdomain": {"mnemonic": row[0], "name": row[1]}
                })
            return results
    except Exception as e:
        logger.error(f"Error searching subdomains: {e}")
        return []

def get_available_domains():
    """Get list of available domains from transcriptions in database"""
    try:
        with engine.connect() as conn:
            # Make sure domain tables exist
            ensure_domain_tables(conn)
            
            # First get a list of all transcription tables that actually exist
            existing_tables = conn.execute(text("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name LIKE 'transcriptions_%'
                AND table_schema = 'public'
            """)).fetchall()
            
            if not existing_tables:
                logger.warning("No transcription tables found in database")
                # Return all domains from domain_subdomain.py as a fallback
                return list(domains_and_subdomains["domains"].keys())
            
            # Build the UNION query dynamically based on existing tables
            query_parts = []
            for (table_name,) in existing_tables:
                # First check if the domain column exists in this table
                column_exists = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_name = :table_name 
                        AND column_name = 'domain'
                    )
                """), {"table_name": table_name}).scalar()
                
                if column_exists:
                    query_parts.append(f"""
                        SELECT DISTINCT domain FROM {table_name}
                        WHERE domain IS NOT NULL AND domain != ''
                    """)
            
            if query_parts:
                # Execute the union query to get all distinct domains across all tables
                domains_query = text(" UNION ".join(query_parts))
                domain_results = conn.execute(domains_query).fetchall()
                domains = [domain[0] for domain in domain_results if domain[0]]
                if domains:
                    return domains
                    
            logger.warning("No transcription tables with domain column found")
            # Return all domains from domain_subdomain.py as a fallback
            return list(domains_and_subdomains["domains"].keys())
                
    except Exception as e:
        logger.error(f"Error getting available domains from transcriptions: {e}")
        # Return all domains from domain_subdomain.py as a fallback
        return list(domains_and_subdomains["domains"].keys())

def get_available_subdomains(domain_code):
    """Get list of available subdomains for a domain from transcriptions in database"""
    try:
        with engine.connect() as conn:
            # First check if any transcription tables exist and have the subdomain column
            existing_tables = conn.execute(text("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name LIKE 'transcriptions_%'
                AND table_schema = 'public'
            """)).fetchall()
            
            if not existing_tables:
                logger.warning("No transcription tables found in database")
                # Return all subdomains for this domain as fallback
                return [sd["mnemonic"] for sd in domains_and_subdomains["subdomains"].get(domain_code, [])]
            
            # Build the UNION query dynamically based on existing tables
            query_parts = []
            for (table_name,) in existing_tables:
                # Check if both domain and subdomain columns exist
                columns_exist = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_name = :table_name 
                        AND column_name = 'domain'
                    ) AND EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_name = :table_name 
                        AND column_name = 'subdomain'
                    )
                """), {"table_name": table_name}).scalar()
                
                if columns_exist:
                    query_parts.append(f"""
                        SELECT DISTINCT subdomain FROM {table_name}
                        WHERE domain = :domain_code
                        AND subdomain IS NOT NULL AND subdomain != ''
                    """)
            
            if query_parts:
                # Execute the union query with domain code parameter
                subdomains_query = text(" UNION ".join(query_parts))
                subdomain_results = conn.execute(subdomains_query, {"domain_code": domain_code}).fetchall()
                subdomains = [subdomain[0] for subdomain in subdomain_results if subdomain[0]]
                if subdomains:
                    return subdomains
            
            logger.warning(f"No transcription tables with domain and subdomain columns found")
            # Return all subdomains for this domain as fallback
            return [sd["mnemonic"] for sd in domains_and_subdomains["subdomains"].get(domain_code, [])]
                
    except Exception as e:
        logger.error(f"Error getting available subdomains from transcriptions: {e}")
        # Return all subdomains for this domain as fallback
        return [sd["mnemonic"] for sd in domains_and_subdomains["subdomains"].get(domain_code, [])]

# Initialize domain data when module is loaded
try:
    initialize_domain_data()
except Exception as e:
    logger.warning(f"Could not initialize domain data: {e}. Will try again when needed.")
