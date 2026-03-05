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

    def save_audio(self, pcm_data, sample_rate, filename, bits_per_sample=16, channels=1, already_processed=False):
        """Save audio file in language-specific audio folder using built-in wave module with processing"""
        if not should_save_locally():
            return None

        try:
            # Get language directory and create audio subdirectory
            lang_dir = BASE_DIR / self.language
            audio_dir = lang_dir / 'audio'
            audio_dir.mkdir(parents=True, exist_ok=True)
            
            # Full path for the output WAV file
            filepath = audio_dir / filename
            
            # Apply server-side processing if not already done client-side
            if not already_processed and isinstance(pcm_data, (bytes, bytearray)):
                # Convert PCM bytes to int16 array for processing
                bytes_per_sample = bits_per_sample // 8
                num_samples = len(pcm_data) // bytes_per_sample
                
                if bits_per_sample == 16:
                    # Convert bytes to int16 array
                    import array
                    int16_data = array.array('h')
                    int16_data.frombytes(pcm_data)
                    
                    # Parameters for audio processing
                    samples_per_second = sample_rate * channels
                    fade_in_samples = min(int(samples_per_second * 0.3), int(num_samples * 0.1))  # 300ms fade in
                    end_trim_samples = min(int(samples_per_second * 0.15), int(num_samples * 0.05))  # 150ms end trim
                    fade_out_samples = min(int(samples_per_second * 0.15), int(num_samples * 0.04))  # 150ms fade out
                    
                    # Step 1: Apply fade-in to the beginning (before trimming)
                    for i in range(fade_in_samples):
                        fade_ratio = i / fade_in_samples
                        # Cubic ease-in curve for smooth fade
                        smooth_fade = fade_ratio * fade_ratio * fade_ratio
                        int16_data[i] = int(int16_data[i] * smooth_fade)
                    
                    # Step 2: Calculate the length after removing the end trim
                    trimmed_length = max(0, num_samples - end_trim_samples)
                    
                    # Step 3: Apply fade-out at the end (before the trim point)
                    fade_out_start = trimmed_length - fade_out_samples
                    for i in range(fade_out_samples):
                        if fade_out_start + i >= trimmed_length:
                            break
                        fade_ratio = 1 - (i / fade_out_samples)
                        # Cubic ease-out curve for smooth fade
                        smooth_fade = fade_ratio * fade_ratio * fade_ratio
                        int16_data[fade_out_start + i] = int(int16_data[fade_out_start + i] * smooth_fade)
                    
                    # Step 4: Create new PCM data with end trimming
                    pcm_data = int16_data[:trimmed_length].tobytes()
                    logger.info(f"Server-side processing: applied 300ms fade-in, 150ms end trim and 150ms fade-out")
            
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