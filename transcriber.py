import torch
from transformers import pipeline, WhisperProcessor, WhisperForConditionalGeneration, logging
from pydub import AudioSegment
import os
import warnings

warnings.filterwarnings("ignore")
logging.set_verbosity_error()

model_path = "/home/atreyagnayak/Dhravani-main/models/whisper-small-kannada" 
processor = WhisperProcessor.from_pretrained(model_path)
model = WhisperForConditionalGeneration.from_pretrained(model_path)


forced_decoder_ids = processor.get_decoder_prompt_ids(language="kannada", task="transcribe")
model.config.forced_decoder_ids = forced_decoder_ids
model.config.suppress_tokens = []  

whisper_pipe = pipeline(
    "automatic-speech-recognition",
    model=model,
    tokenizer=processor.tokenizer,
    feature_extractor=processor.feature_extractor,
    processor=processor,
    chunk_length_s=30,
    return_timestamps=False,
    device=0 if torch.cuda.is_available() else -1
)

def transcribe_file(file_path):
    try:
        audio = AudioSegment.from_file(file_path)
        wav_path = file_path.rsplit(".", 1)[0] + ".wav"
        audio.export(wav_path, format="wav")

        result = whisper_pipe(wav_path)
        return result["text"]
    except Exception as e:
        raise RuntimeError(f"Whisper transcription failed: {e}")
