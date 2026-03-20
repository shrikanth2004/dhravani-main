"""Multi-language transcriber using Whisper models"""

# Language code to Whisper model mapping
# Using models that support more languages for Indian languages
WHISPER_MODELS = {
    'en': {'model': 'openai/whisper-small', 'lang': 'english'},
    'as': {'model': 'openai/whisper-medium', 'lang': 'assamese'},
    'bn': {'model': 'openai/whisper-small', 'lang': 'bengali'},
    'bdo': {'model': 'openai/whisper-medium', 'lang': 'bodo'},
    'doi': {'model': 'openai/whisper-medium', 'lang': 'dogri'},
    'gu': {'model': 'openai/whisper-small', 'lang': 'gujarati'},
    'hi': {'model': 'openai/whisper-small', 'lang': 'hindi'},
    'kn': {'model': 'vasista22/whisper-kannada-small', 'lang': 'kannada'},
    'ks': {'model': 'openai/whisper-medium', 'lang': 'kashmiri'},
    'kok': {'model': 'openai/whisper-medium', 'lang': 'hindi'},  # Fallback to Hindi
    'mai': {'model': 'openai/whisper-medium', 'lang': 'maithili'},
    'ml': {'model': 'openai/whisper-small', 'lang': 'malayalam'},
    'mr': {'model': 'openai/whisper-small', 'lang': 'marathi'},
    'mni': {'model': 'openai/whisper-medium', 'lang': 'manipuri'},
    'ne': {'model': 'openai/whisper-small', 'lang': 'nepali'},
    'ol': {'model': 'openai/whisper-medium', 'lang': 'oriya'},
    'or': {'model': 'openai/whisper-small', 'lang': 'odia'},
    'pa': {'model': 'openai/whisper-small', 'lang': 'punjabi'},
    'sa': {'model': 'openai/whisper-medium', 'lang': 'sanskrit'},
    'sd': {'model': 'openai/whisper-medium', 'lang': 'sindhi'},
    'ta': {'model': 'openai/whisper-small', 'lang': 'tamil'},
    'te': {'model': 'openai/whisper-small', 'lang': 'telugu'},
    'ur': {'model': 'openai/whisper-small', 'lang': 'urdu'},
}

# Cache for loaded models to avoid reloading
_model_cache = {}
_pipe_cache = {}


def get_transcriber(language_code):
    """Get or create a transcriber pipeline for the specified language"""
    if language_code in _pipe_cache:
        return _pipe_cache[language_code]
    
    if language_code not in WHISPER_MODELS:
        raise ValueError(f"Unsupported language code: {language_code}")
    
    import torch
    from transformers import pipeline, WhisperProcessor, WhisperForConditionalGeneration, logging
    import warnings
    
    warnings.filterwarnings("ignore")
    logging.set_verbosity_error()
    
    config = WHISPER_MODELS[language_code]
    model_path = config['model']
    lang = config['lang']
    
    try:
        processor = WhisperProcessor.from_pretrained(model_path)
        model = WhisperForConditionalGeneration.from_pretrained(model_path)
        
        # Set forced decoder IDs for the language
        forced_decoder_ids = processor.get_decoder_prompt_ids(language=lang, task="transcribe")
        model.config.forced_decoder_ids = forced_decoder_ids
        model.config.suppress_tokens = []
        
        # Use generate_kwargs to set language
        whisper_pipe = pipeline(
            "automatic-speech-recognition",
            model=model,
            tokenizer=processor.tokenizer,
            feature_extractor=processor.feature_extractor,
            chunk_length_s=30,
            return_timestamps=False,
            device=0 if torch.cuda.is_available() else -1,
            generate_kwargs={"language": lang, "task": "transcribe"}
        )
        
        _pipe_cache[language_code] = whisper_pipe
        return whisper_pipe
        
    except Exception as e:
        raise RuntimeError(f"Failed to load transcriber for {language_code}: {e}")


def transcribe_audio(file_path, language_code):
    """Transcribe audio file for the given language code"""
    from pydub import AudioSegment
    import warnings
    
    warnings.filterwarnings("ignore")
    
    if language_code not in WHISPER_MODELS:
        raise ValueError(f"Unsupported language code: {language_code}")
    
    # Clear cache for this language to ensure fresh model is loaded with correct settings
    if language_code in _pipe_cache:
        del _pipe_cache[language_code]
    
    try:
        # Get the transcriber pipeline
        whisper_pipe = get_transcriber(language_code)
        
        # Convert audio to wav if needed
        audio = AudioSegment.from_file(file_path)
        wav_path = file_path.rsplit(".", 1)[0] + ".wav"
        audio.export(wav_path, format="wav")
        
        # Transcribe
        result = whisper_pipe(wav_path)
        return result["text"]
        
    except Exception as e:
        raise RuntimeError(f"Transcription failed for {language_code}: {e}")


# Convenience functions for individual languages
def transcribe_english(file_path):
    return transcribe_audio(file_path, 'en')

def transcribe_kannada(file_path):
    return transcribe_audio(file_path, 'kn')

def transcribe_hindi(file_path):
    return transcribe_audio(file_path, 'hi')

def transcribe_bengali(file_path):
    return transcribe_audio(file_path, 'bn')

def transcribe_tamil(file_path):
    return transcribe_audio(file_path, 'ta')

def transcribe_telugu(file_path):
    return transcribe_audio(file_path, 'te')

def transcribe_malayalam(file_path):
    return transcribe_audio(file_path, 'ml')

def transcribe_marathi(file_path):
    return transcribe_audio(file_path, 'mr')

def transcribe_gujarati(file_path):
    return transcribe_audio(file_path, 'gu')

def transcribe_punjabi(file_path):
    return transcribe_audio(file_path, 'pa')
