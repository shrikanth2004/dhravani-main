---
title: Dhravani
colorFrom: blue
colorTo: blue
sdk: docker
pinned: true
short_description: Speech Corpus Creation Tool
thumbnail: >-
  https://cdn-uploads.huggingface.co/production/uploads/663de91ee8b79b3f9e74461e/sQ_AHOKdSuOrfe7o8Jo31.png
---

Check out the configuration reference at https://huggingface.co/docs/hub/spaces-config-reference

# Dataset Preparation Interface for Fine-tuning Whisper

A web-based interface for preparing audio datasets to fine-tune OpenAI's Whisper model. This tool helps in recording, managing, and organizing voice recordings with their corresponding transcriptions, with support for cloud storage and authentication.

## Features

- 🔐 User authentication via Pocketbase
- ☁️ Cloud storage support (Hugging Face Datasets)
- 🌐 Multi-language support with native names
- 🎤 Modern Material Design recording interface
- 📝 CSV transcript file support
- 🎯 Session-based recording workflow
- 🔄 Advanced recording controls
- ⌨️ Keyboard shortcuts for efficiency
- 📊 Progress tracking and navigation
- 💾 Local and cloud metadata management
- 🎨 Responsive, mobile-friendly UI

## Getting Started

1. Create a transcript CSV file with your content:

```csv
transcript
"First sentence to record"
"Second sentence to record"
# For multi-language support:
transcript_en,transcript_es
"English sentence","Spanish sentence"
```

2. Start the Flask application:

```bash
python app.py
```

3. Access the interface:

```
http://localhost:5000
```

## Usage

1. **Authentication**

   - Sign in using your Google account
2. **Session Setup**

   - Upload your transcript CSV
   - Select language and recording location
   - Enter speaker details
   - Click "Start Session"
3. **Recording**

   - Use on-screen controls or keyboard shortcuts:
     - `R`: Start recording / Stop recording
     - `Space`: Play recording
     - `Enter`: Save recording
     - `Backspace`: Re-record
     - `←`: Previous transcript
     - `→`: Skip current
   - Navigate using row numbers
   - Adjust transcript font size as needed

## Data Storage

Recordings are stored in language-specific directories:

- **Storage**:

  ```
  datasets/
  ├── en/
  │   ├── audio/
  │   │   ├── {user_prefix}_{YYYYMMDD_HHMMSS}.wav
  │   │   └── ...
  │   └── en.parquet         # English recordings metadata
  ├── es/
  │   ├── audio/
  │   │   ├── {user_prefix}_{YYYYMMDD_HHMMSS}.wav
  │   │   └── ...
  │   └── es.parquet         # Spanish recordings metadata
  │
  ```

## Technical Details

### Audio Recording

- Browser Recording Format: 48kHz mono WebM
- Storage Format: 16bit mono WAV
- Maximum Duration: 30 seconds
- Audio Processing: WebM -> WAV conversion with sample rate adjustment
- Channels: 1 (mono)

### Data Management

- Metadata Organization:
  - stats.json: Global recording statistics
  - {language_code}.parquet: Language-specific metadata files
- File Naming: `{user_id_prefix}_{YYYYMMDD_HHMMSS}.{format}`
- Unicode Handling: NFC normalization for text

### Authentication

- Provider: Pocketbase with Google OAuth
- Session Management: Server-side Flask sessions

### Languages

- Support: 74 languages with native names
- Codes: ISO 639-1 standard
- CSV Format:
  - Single language: `transcript` column
  - Multi-language: `transcript_${lang_code}` columns

### Upload Management

- Queue System: Background worker thread
- Status Tracking: Real-time upload status polling
- Error Handling: Automatic retries with timeout
- Progress Updates: Toast notifications
- Temporary Storage: ./temp folder for conversions

### Frontend Features

- Keyboard Shortcuts: Recording and navigation
- Real-time Status: Progress tracking and notifications

### Security

- Authentication Required: All routes except static/login
- File Validation: MIME type and extension checking
- Secure Context: HTTPS recommended

### Performance

- Upload Queue: Asynchronous processing
- Audio Conversion: Server-side processing
- Session Caching: Browser storage optimization
- Progress Tracking: Real-time websocket updates

## Browser Support

- Chrome (recommended)
- Brave
- Edge
- Safari

## Known Limitations

- Requires microphone permissions
- Internet connection needed
- Maximum recording duration: 30 seconds
- File size limits based on storage backend

