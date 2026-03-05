import logging
import random
from database_manager import get_transcriptions_for_language

logger = logging.getLogger(__name__)

class LazyTranscriptLoader:
    """Lazy loading class for transcripts to avoid loading all at once"""
    
    def __init__(self, language, batch_size=50, randomize=False, domain=None, subdomain=None):
        """
        Initialize the lazy loader
        
        Args:
            language (str): Language code for transcripts
            batch_size (int): Number of transcripts to load per batch
            randomize (bool): Whether to randomize the order of transcripts
            domain (str): Optional domain filter
            subdomain (str): Optional subdomain filter
        """
        self.language = language
        self.batch_size = batch_size
        self.randomize = randomize
        self.domain = domain
        self.subdomain = subdomain
        
        # Internal state
        self._loaded_transcripts = []  # Currently loaded batch
        self._current_index = 0        # Index within the batch
        self._total_count = 0          # Total available transcripts
        self._loaded_count = 0         # How many we've loaded so far
        self._exclude_ids = []         # IDs we've already seen
        
        # Initialize by loading the count
        try:
            result = get_transcriptions_for_language(
                language_code=self.language,
                include_recorded=False,
                count_only=True,
                domain=self.domain,
                subdomain=self.subdomain
            )
            self._total_count = result.get('count', 0)
            
            # Load first batch if we have transcripts
            if self._total_count > 0:
                self._load_next_batch()
        except Exception as e:
            logger.error(f"Error initializing LazyTranscriptLoader: {e}")
            # Don't propagate the error - just start with empty state
            self._total_count = 0
            self._loaded_transcripts = []
    
    def _load_next_batch(self):
        """Load the next batch of transcripts"""
        try:
            transcripts = get_transcriptions_for_language(
                language_code=self.language,
                include_recorded=False,
                limit=self.batch_size,
                exclude_ids=self._exclude_ids,
                domain=self.domain,
                subdomain=self.subdomain
            )
            
            # Update loaded count
            self._loaded_count += len(transcripts)
            
            # Add new IDs to exclude list for future loads
            for t in transcripts:
                self._exclude_ids.append(t['id'])
            
            # Randomize if needed
            if self.randomize:
                random.shuffle(transcripts)
            
            # Replace current batch and reset index
            self._loaded_transcripts = transcripts
            self._current_index = 0
            
            return len(transcripts) > 0
        except Exception as e:
            logger.error(f"Error loading transcript batch: {e}")
            return False
    
    def get_current(self):
        """Get the current transcript"""
        if not self._loaded_transcripts:
            return None
        
        if self._current_index >= len(self._loaded_transcripts):
            # Load next batch if we've exhausted the current one
            if not self._load_next_batch():
                # If we're at the end, adjust the index to the last valid item
                if len(self._loaded_transcripts) > 0:
                    self._current_index = len(self._loaded_transcripts) - 1
                else:
                    return None
        
        if self._loaded_transcripts and self._current_index < len(self._loaded_transcripts):
            return self._loaded_transcripts[self._current_index]
            
        return None
    
    def move_next(self):
        """Move to the next transcript and return it"""
        if not self._loaded_transcripts:
            return None
            
        self._current_index += 1
        
        if self._current_index >= len(self._loaded_transcripts):
            # Load next batch if we've exhausted the current one
            if not self._load_next_batch():
                # If we've reached the end, adjust the index back to the last valid item
                if len(self._loaded_transcripts) > 0:
                    self._current_index = len(self._loaded_transcripts) - 1
                    
                    # Return None to indicate we've reached the end but
                    # keep the internal state valid for Previous navigation
                    return None
                else:
                    return None
        
        return self.get_current()
    
    def move_prev(self):
        """Move to the previous transcript if possible"""
        if not self._loaded_transcripts:
            return None
        
        # Ensure we have a valid current index
        if self._current_index >= len(self._loaded_transcripts):
            self._current_index = len(self._loaded_transcripts) - 1
        
        # If we're at the first item, return None to indicate boundary
        if self._current_index <= 0:
            return None
        
        # Move back one position and return that item
        self._current_index -= 1
        return self.get_current()
    
    def get_progress(self):
        """Get the current progress information"""
        total = self._total_count
        current = min(self._loaded_count - len(self._loaded_transcripts) + self._current_index + 1, total) if total > 0 else 0
        
        return {
            'current': current,
            'total': total,
            'loaded': self._loaded_count
        }
