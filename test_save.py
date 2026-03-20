import sys
import os
from prepare_dataset import AudioDatasetPreparator, should_save_locally
from pathlib import Path

def test_save():
    print("Should save locally:", should_save_locally())
    print("BASE_DIR:", Path('datasets').absolute())
    
    preparator = AudioDatasetPreparator([], user_id='test')
    preparator.language = 'kn'
    
    pcm_data = b'\x00\x00' * 100
    sample_rate = 48000
    bits_per_sample = 16
    channels = 1
    filename = "test_audio.wav"
    
    try:
        path = preparator.save_audio(pcm_data, sample_rate, bits_per_sample, channels, filename)
        print("Saved audio correctly to:", path)
    except Exception as e:
        print("Exception:", e)

if __name__ == "__main__":
    test_save()
