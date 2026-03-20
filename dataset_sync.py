import os
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from huggingface_hub import HfApi
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from dotenv import load_dotenv
import soundfile as sf
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from sqlalchemy import text
from database_manager import engine
import filelock
import queue
from typing import Dict, Set, List, Optional, Iterator
import tempfile
import shutil
import gc
import socket
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dataset_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Define base directories
BASE_DIR = Path(os.getenv('DATASET_BASE_DIR', '/app/datasets'))  # Base directory for all data
SYNC_STATE_FILE = BASE_DIR / '.sync_state.json'  # Sync state file
STATS_FILE = BASE_DIR / 'stats.json'  # Stats file

class DatasetSynchronizer:
    def __init__(self):
        self.base_dir = BASE_DIR
        self.sync_state_file = SYNC_STATE_FILE  # Using correct sync state file
        self.hf_token = os.getenv('HF_TOKEN')
        self.repo_id = os.getenv('HF_REPO_ID')
        self.hf_api = HfApi(token=self.hf_token)
        self.sync_state = self._load_sync_state()
        self.stats_file = STATS_FILE
        self.max_workers = int(os.getenv('MAX_UPLOAD_WORKERS', 4))
        self.max_retries = int(os.getenv('MAX_UPLOAD_RETRIES', 3))
        self.verified_files = set()  # Track verified audio files
        self.verified_cache = {}  # Cache verification status
        self.lock_file = BASE_DIR / '.sync.lock'
        self.lock = filelock.FileLock(str(self.lock_file), timeout=0)  # Non-blocking
        self.file_queue = queue.Queue()
        self.uploaded_files: Set[str] = set()
        self.failed_files: Dict[str, int] = {}  # track retry counts
        self.chunk_size = int(os.getenv('UPLOAD_CHUNK_SIZE', 1024 * 1024))  # 1MB chunks
        self.memory_limit = int(os.getenv('SYNC_MEMORY_LIMIT_MB', 1024)) * 1024 * 1024  # Convert MB to bytes
        self.network_timeout = int(os.getenv('NETWORK_TIMEOUT', 30))
        self.batch_size = int(os.getenv('UPLOAD_BATCH_SIZE', 10))
        self.recovery_file = BASE_DIR / '.sync_recovery'

    def _load_sync_state(self):
        """Load the sync state from file or create new if doesn't exist"""
        if self.sync_state_file.exists():
            with open(self.sync_state_file, 'r', encoding='utf-8') as f:  # Added encoding
                return json.load(f)
        return {
            'files': {},  # Track file hashes
            'last_sync': None,  # Last successful sync timestamp
            'sync_count': 0  # Total number of successful syncs
        }

    def _save_sync_state(self):
        """Save the current sync state to file"""
        with open(self.sync_state_file, 'w', encoding='utf-8') as f:  # Added encoding
            json.dump(self.sync_state, f, indent=2, ensure_ascii=False)  # Added ensure_ascii=False

    def _calculate_file_hash(self, file_path: str) -> str:
        """Optimized file hashing using chunks"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(self.chunk_size), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _prepare_parquet_files(self):
        """Prepare Parquet files before synchronization"""
        try:
            from prepare_parquet import update_parquet_files
            update_parquet_files()
            logger.debug("Parquet files updated successfully")
            return True
        except Exception as e:
            logger.error(f"Error preparing Parquet files: {str(e)}")
            return False

    def _is_verified_audio(self, file_path):
        """Check if an audio file belongs to a verified recording"""
        try:
            # Get language and filename from path
            parts = file_path.parts
            lang_code = parts[-3]  # Assuming path structure: datasets/lang/audio/file.wav
            audio_filename = parts[-1]
            
            # Check cache first
            cache_key = f"{lang_code}:{audio_filename}"
            if cache_key in self.verified_cache:
                return self.verified_cache[cache_key]
            
            # First check if table exists
            with engine.connect() as conn:
                table_exists = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = :table_name
                    )
                """), {"table_name": f"recordings_{lang_code}"}).scalar()

                if not table_exists:
                    logger.debug(f"Table recordings_{lang_code} does not exist, skipping verification check")
                    self.verified_cache[cache_key] = False
                    return False

                # Only query if table exists
                result = conn.execute(text(f"""
                    SELECT EXISTS (
                        SELECT 1 
                        FROM recordings_{lang_code} 
                        WHERE audio_filename = :filename 
                        AND status = 'verified'
                    )
                """), {"filename": audio_filename}).scalar()
                
                # Cache the result
                self.verified_cache[cache_key] = bool(result)
                return self.verified_cache[cache_key]
                
        except Exception as e:
            logger.error(f"Error checking verification status for {file_path}: {str(e)}")
            return False

    def _get_modified_files(self):
        """Get list of new or modified files since last sync"""
        if not self._prepare_parquet_files():
            logger.error("Failed to prepare Parquet files, aborting sync")
            return []

        modified_files = []
        
        # Track skipped directories
        skipped_langs = set()
        
        # Ensure README.md exists and include it
        readme_file = self.base_dir / 'README.md'
        if not readme_file.exists():
            try:
                readme_content = "---\nconfigs:\n  - config_name: default\n    data_files:\n      - split: train\n        path: \"**/*.parquet\"\n---\n# Dhravani Speech Dataset\n\nThis dataset contains audio recordings and transcriptions.\n"
                with open(readme_file, 'w', encoding='utf-8') as f:
                    f.write(readme_content)
                logger.info("Created README.md with Hugging Face dataset configuration")
            except Exception as e:
                logger.error(f"Failed to create README.md: {e}")

        # Always include stats and README files
        root_files_to_sync = [self.stats_file, readme_file]
        for f in root_files_to_sync:
            if f.exists():
                current_hash = self._calculate_file_hash(f)
                stored_hash = self.sync_state['files'].get(str(f))
                
                if current_hash != stored_hash:
                    modified_files.append(f)
                    self.sync_state['files'][str(f)] = current_hash

        # Iterate through language directories
        for lang_dir in self.base_dir.iterdir():
            if not lang_dir.is_dir() or lang_dir.name.startswith('.'):
                continue

            lang_code = lang_dir.name
            
            # Check if recordings table exists for this language
            with engine.connect() as conn:
                table_exists = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = :table_name
                    )
                """), {"table_name": f"recordings_{lang_code}"}).scalar()

                if not table_exists:
                    skipped_langs.add(lang_code)
                    logger.info(f"Skipping language {lang_code} - no recordings table exists")
                    continue

            # Always include Parquet files
            parquet_file = lang_dir / f"{lang_dir.name}.parquet"
            if parquet_file.exists():
                current_hash = self._calculate_file_hash(parquet_file)
                stored_hash = self.sync_state['files'].get(str(parquet_file))
                
                if current_hash != stored_hash:
                    modified_files.append(parquet_file)
                    self.sync_state['files'][str(parquet_file)] = current_hash
                    logger.info(f"Added updated Parquet file to sync: {parquet_file}")
                
            # Check audio files - only include verified ones
            audio_dir = lang_dir / 'audio'
            if audio_dir.exists():
                for audio_file in audio_dir.glob('[a-z0-9]*_[0-9]*_[0-9]*.wav'):
                    # First check if file is verified
                    if not self._is_verified_audio(audio_file):
                        logger.debug(f"Skipping unverified audio: {audio_file}")
                        continue
                        
                    # Then check if it needs to be synced
                    current_hash = self._calculate_file_hash(audio_file)
                    stored_hash = self.sync_state['files'].get(str(audio_file))
                    
                    if current_hash != stored_hash:
                        modified_files.append(audio_file)
                        self.sync_state['files'][str(audio_file)] = current_hash
                        logger.info(f"Added verified audio file to sync: {audio_file}")

        if skipped_langs:
            logger.info(f"Skipped languages due to missing tables: {', '.join(skipped_langs)}")
            
        return modified_files

    @contextmanager
    def _memory_tracker(self):
        """Track memory usage during operations"""
        try:
            gc.collect()  # Force garbage collection before operation
            start_mem = self._get_memory_usage()
            yield
        finally:
            gc.collect()  # Clean up after operation
            end_mem = self._get_memory_usage()
            if end_mem - start_mem > self.memory_limit:
                logger.warning(f"Memory usage exceeded limit: {(end_mem - start_mem) / 1024 / 1024:.2f}MB")

    def _get_memory_usage(self) -> int:
        """Get current memory usage"""
        import psutil
        process = psutil.Process()
        return process.memory_info().rss

    def _batch_files(self, files: list) -> Iterator[list]:
        """Process files in batches to manage memory"""
        for i in range(0, len(files), self.batch_size):
            yield files[i:i + self.batch_size]

    def _save_recovery_state(self, failed_files: dict):
        """Save failed uploads for recovery"""
        try:
            with open(self.recovery_file, 'w') as f:
                json.dump(failed_files, f)
        except Exception as e:
            logger.error(f"Failed to save recovery state: {e}")

    def _load_recovery_state(self) -> dict:
        """Load failed uploads from previous sync"""
        try:
            if self.recovery_file.exists():
                with open(self.recovery_file) as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load recovery state: {e}")
        return {}

    def _upload_file_with_retry(self, file_path: str, retry_count: int = 0) -> bool:
        """Enhanced upload with network timeout and better error handling"""
        socket.setdefaulttimeout(self.network_timeout)  # Set network timeout
        
        try:
            with self._memory_tracker():
                relative_path = str(Path(file_path).relative_to(self.base_dir))
                relative_path = relative_path.replace('\\', '/')

                # Create a temporary file for chunked upload
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    temp_path = temp_file.name
                    shutil.copy2(file_path, temp_path)

                    logger.info(f"Uploading {relative_path} to {self.repo_id} (attempt {retry_count + 1})")
                    
                    self.hf_api.upload_file(
                        path_or_fileobj=temp_path,
                        path_in_repo=relative_path,
                        repo_id=self.repo_id,
                        repo_type="dataset"
                    )

                os.unlink(temp_path)
                self.uploaded_files.add(file_path)
                logger.info(f"Successfully uploaded {relative_path}")
                return True
                
        except socket.timeout:
            logger.error(f"Network timeout uploading {file_path}")
            if retry_count < self.max_retries:
                time.sleep(2 ** retry_count)  # Exponential backoff
                return self._upload_file_with_retry(file_path, retry_count + 1)
            return False
            
        except Exception as e:
            if retry_count < self.max_retries - 1:
                logger.warning(f"Upload failed for {file_path}, retrying... ({retry_count + 1}/{self.max_retries})")
                time.sleep(2 ** retry_count)  # Exponential backoff
                return self._upload_file_with_retry(file_path, retry_count + 1)
            else:
                logger.error(f"Error uploading {file_path} after {self.max_retries} attempts: {str(e)}")
                self.failed_files[file_path] = retry_count + 1
                return False
        finally:
            socket.setdefaulttimeout(None)  # Reset timeout

    def _parallel_upload(self, files: List[Path]) -> bool:
        """Upload multiple files in parallel with improved error handling"""
        successful = True
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self._upload_file_with_retry, str(file_path)): file_path 
                for file_path in files
            }
            
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    if not future.result():
                        successful = False
                        logger.error(f"Failed to upload: {file_path}")
                except Exception as e:
                    successful = False
                    logger.error(f"Unexpected error uploading {file_path}: {str(e)}")
                    
        return successful

    def is_syncing(self):
        """Check if a sync is in progress"""
        try:
            # Try to acquire lock without waiting
            with self.lock:
                return False
        except filelock.Timeout:
            return True

    def sync_dataset(self) -> bool:
        """Improved sync with recovery and resource management"""
        try:
            with self.lock:
                # Load any failed uploads from previous sync
                recovery_state = self._load_recovery_state()
                if recovery_state:
                    logger.info(f"Found {len(recovery_state)} failed uploads from previous sync")
                    
                modified_files = self._get_modified_files()
                modified_files.extend(recovery_state.keys())  # Add previously failed files
                
                if not modified_files:
                    return True

                # Process files in batches
                for batch in self._batch_files(modified_files):
                    with self._memory_tracker():
                        if not self._parallel_upload(batch):
                            self._save_recovery_state(self.failed_files)
                            return False

                # Update sync state and cleanup
                self._update_sync_state()
                if self.recovery_file.exists():
                    self.recovery_file.unlink()  # Remove recovery file after successful sync
                
                return len(self.failed_files) == 0

        except Exception as e:
            logger.error(f"Sync error: {e}")
            self._save_recovery_state(self.failed_files)
            return False
        finally:
            # Cleanup
            self.uploaded_files.clear()
            self.failed_files.clear()

    def _update_sync_state(self):
        """Update sync state with proper cleanup"""
        try:
            # Update only successfully uploaded files
            for file_path in self.uploaded_files:
                self.sync_state['files'][str(file_path)] = self._calculate_file_hash(str(file_path))

            self.sync_state['last_sync'] = datetime.now().isoformat()
            self.sync_state['sync_count'] += 1
            
            # Save state atomically
            temp_state_file = str(self.sync_state_file) + '.tmp'
            with open(temp_state_file, 'w') as f:
                json.dump(self.sync_state, f)
            os.replace(temp_state_file, self.sync_state_file)
            
        except Exception as e:
            logger.error(f"Failed to update sync state: {e}")
            raise

def sync_job():
    """Function to be called by the scheduler"""
    try:
        synchronizer = DatasetSynchronizer()
        synchronizer.sync_dataset()
    except Exception as e:
        logger.error(f"Error in sync job: {str(e)}")

def init_scheduler():
    """Initialize the scheduler with one-time sync after startup"""
    scheduler = BackgroundScheduler()
    
    # Add daily sync job
    sync_hour = int(os.getenv('SYNC_HOUR', '0'))
    sync_minute = int(os.getenv('SYNC_MINUTE', '0'))
    timezone = os.getenv('SYNC_TIMEZONE', 'UTC')
    
    # Configure daily sync job
    scheduler.add_job(
        sync_job,
        CronTrigger(
            hour=sync_hour,
            minute=sync_minute,
            timezone=pytz.timezone(timezone)
        ),
        id='daily_sync',
        name='Daily Dataset Sync'
    )
    
    # Add one-time Parquet update and sync job 1 minutes after startup
    initial_sync_time = datetime.now() + timedelta(minutes=1)
    scheduler.add_job(
        sync_job,
        'date',
        run_date=initial_sync_time,
        id='initial_sync',
        name='Initial Dataset Sync'
    )
    
    scheduler.start()
    logger.info(f"Scheduler initialized with daily sync and one-time initial sync at {initial_sync_time}")
    return scheduler

# Main execution
if __name__ == "__main__":
    try:
        # Initialize and start the scheduler
        scheduler = init_scheduler()
        
        # Keep the script running
        try:
            while True:
                pass
        except KeyboardInterrupt:
            scheduler.shutdown()
            logger.info("Scheduler shutdown complete")
            
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
        raise
