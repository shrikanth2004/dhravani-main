import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import wave
import struct
from io import BytesIO
from dotenv import load_dotenv
from huggingface_hub import HfApi
import logging
from database_manager import store_metadata, engine
from language_config import get_all_languages
from sqlalchemy import text
import numpy as np
import scipy.signal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Define base directories
BASE_DIR = Path('datasets')  # Base directory for all data

def should_save_locally():
    """Check if we should save files locally"""
    return os.getenv("SAVE_LOCALLY", "true").lower() == "true"

class AudioDatasetPreparator:
    def __init__(self, transcripts, user_id='anonymous'):
        self.transcripts = transcripts
        self.user_id = user_id
        self.speaker_name = ""
        self.gender = ""
        self.language = ""
        self.country = ""
        self.state = ""
        self.city = ""
        self.age_group = ""
        self.accent = ""
        self.domain = "GEN"  # Add default domain
        self.subdomain = "GEN"  # Add default subdomain
        
        # Initialize storage if needed
        if should_save_locally():
            self._initialize_storage()

    def _initialize_storage(self):
        """Initialize storage directories"""
        try:
            # Create base directory only
            BASE_DIR.mkdir(exist_ok=True)
            
            # Initialize recordings DataFrame with new columns
            self.recordings_df = pd.DataFrame(columns=[
                'user_id', 'audio_filename', 'transcription', 
                'speaker_name', 'speaker_id', 'audio_path',
                'sampling_rate', 'duration', 'language',
                'gender', 'country', 'state', 'city', 'verified',
                'username', 'timestamp', 'age_group', 'accent'
            ])
            
            logger.info("Storage initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing storage: {e}")
            raise

    def _get_language_df(self, language):
        """Get or create language-specific DataFrame"""
        if language not in self.language_dfs:
            # Create language directory structure
            lang_dir = BASE_DIR / language
            lang_dir.mkdir(exist_ok=True)
            # Create audio subdirectory
            audio_dir = lang_dir / 'audio'
            audio_dir.mkdir(exist_ok=True)
            
            parquet_path = lang_dir / f"{language}.parquet"
            
            if parquet_path.exists():
                self.language_dfs[language] = pd.read_parquet(parquet_path)
            else:
                self.language_dfs[language] = pd.DataFrame(columns=self.recordings_df.columns)
        
        return self.language_dfs[language]

    def add_metadata(self, recording_data):
        """Only handle local file operations if needed"""
        if not should_save_locally():
            return

    def save_audio(self, pcm_data, sample_rate, bits_per_sample, channels, filename, already_processed=False):
        TARGET_SR = 16000
        """Save audio file in language-specific audio folder using built-in wave module with processing"""
        if not should_save_locally():
            return None

        try:
            logger.info(f"Saving audio: lang={self.language}, filename={filename}, sr={sample_rate}, bits={bits_per_sample}, channels={channels}")
            # Get language directory and create audio subdirectory
            lang_dir = BASE_DIR / self.language
            audio_dir = lang_dir / 'audio'
            audio_dir.mkdir(parents=True, exist_ok=True)
            
            # Full path for the output WAV file
            filepath = audio_dir / filename
            
            logger.info(f"Creating WAV at {filepath}")
            
            # Create WAV file        
            with wave.open(str(filepath), 'wb') as wav_file:

                # Set WAV file parameters
                wav_file.setnchannels(channels)
                wav_file.setsampwidth(bits_per_sample // 8)
                wav_file.setframerate(sample_rate)
                
                # Write PCM data directly
                wav_file.writeframes(pcm_data)
            
            logger.info(f"Saved audio file: {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"Error saving audio file: {e}")
            return None