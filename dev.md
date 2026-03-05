# Developer Documentation

## Overview

The Dataset Preparation Tool is designed to facilitate the creation and management of audio datasets for machine learning applications. It provides functionalities for recording, transcribing, validating, and synchronizing audio data. This documentation provides a comprehensive guide for developers who wish to understand, modify, or contribute to the project.

## Installation

### Prerequisites

-   Python 3.11+
-   Docker (optional, for containerized deployment)
-   `make` (optional, for simplified commands)
-   PostgreSQL database
-   PocketBase instance
-   Hugging Face account and token

### Steps

1.  **Clone the repository:**

    ```bash
    git clone <repository_url>
    cd dataset-preparation-tool
    ```

2.  **Set up the environment:**

    *   **Using venv (recommended):**

        ```bash
        python -m venv venv
        source venv/bin/activate  # On Windows: venv\Scripts\activate
        pip install -r requirements.txt
        ```

    *   **Using Docker:**

        1.  Build the Docker image:

            ```bash
            docker build -t dataset-preparation .
            ```

        2.  Run the Docker container:

            ```bash
            docker run -p 7860:7860 -v dataset_volume:/app/datasets dataset-preparation
            ```

            **Note:**  The `-v dataset_volume:/app/datasets` flag mounts a Docker volume to persist the datasets.  Ensure the volume is properly configured.

3.  **Configure environment variables:**

    Create a `.env` file in the root directory and define the following variables:

    ```
    FLASK_SECRET_KEY=<your_secret_key>
    POSTGRES_URL=<your_postgres_connection_string>
    POCKETBASE_URL=<your_pocketbase_url>
    HF_TOKEN=<your_huggingface_token>
    HF_REPO_ID=<your_huggingface_repo_id>
    SAVE_LOCALLY=true # or false
    SUPER_ADMIN_PASSWORD=<your_super_admin_password>
    SUPER_USER_EMAILS=<comma_separated_emails_of_super_users> # Optional
    ENABLE_AUTH=true # or false
    JWT_SECRET_KEY=<your_jwt_secret_key> # Optional, defaults to FLASK_SECRET_KEY
    TRANSCRIPT_BATCH_SIZE=50 # Optional, defaults to 50
    SYNC_MEMORY_LIMIT_MB=1024 # Optional, defaults to 1024
    NETWORK_TIMEOUT=30 # Optional, defaults to 30
    UPLOAD_CHUNK_SIZE=1048576 # Optional, defaults to 1048576 (1MB)
    UPLOAD_BATCH_SIZE=10 # Optional, defaults to 10
    SYNC_HOUR=0 # Optional, defaults to 0
    SYNC_MINUTE=0 # Optional, defaults to 0
    SYNC_TIMEZONE=UTC # Optional, defaults to UTC
    MAX_UPLOAD_WORKERS=4 # Optional, defaults to 4
    MAX_UPLOAD_RETRIES=3 # Optional, defaults to 3
    FLASK_PORT=7860 # Optional, defaults to 5000
    DATASET_BASE_DIR=/app/datasets # Optional, defaults to /app/datasets
    TEMP_FOLDER=./temp # Optional, defaults to ./temp
    ```

    **Important:** Replace the placeholder values with your actual configuration.

## Usage

### Running the application

*   **Using venv:**

    ```bash
    flask run --host=0.0.0.0 --port=7860
    ```

*   **Using Docker:** (See Installation step 2.2)

### Key Functionalities

1.  **Data Recording:**  Users can record audio data through the web interface.  Ensure microphone access is enabled in the browser. The audio is saved as a WAV file after processing (fade-in, trim, fade-out).
2.  **Transcription:**  Administrators can upload transcriptions in `.txt` or `.csv` format.  `.csv` files should have one transcription per row. These transcriptions are stored in language-specific tables in the PostgreSQL database.
3.  **Validation:**  Moderators can validate audio recordings and transcriptions.  The validation interface allows filtering by language and status (pending, verified, rejected).
4.  **Synchronization:**  The system automatically synchronizes the dataset with a Hugging Face repository.  Manual synchronization can be triggered via the admin interface.  This process involves calculating file hashes, preparing Parquet files, and uploading verified audio files.
5.  **User Management:**  Administrators can manage user roles (user, moderator, admin) via the admin interface. Super admins can manage admins. User roles are managed through PocketBase.

### Accessing the application

Open your web browser and navigate to `http://localhost:7860` (or the appropriate address if running in Docker).

## Architecture

The application follows a modular architecture with the following key components:

*   **Flask:**  Handles routing, request processing, and rendering templates.
*   **PostgreSQL:** Stores audio metadata and transcriptions. Language-specific tables are used for both recordings and transcriptions.
*   **PocketBase:** Manages user authentication and authorization.
*   **Hugging Face Hub:**  Stores the audio dataset and metadata.
*   **APScheduler:** Schedules dataset synchronization tasks.

## API Reference

### Authentication Endpoints

These endpoints are managed by the `auth_middleware.py` and rely on PocketBase for authentication.

*   `/auth/callback` (POST): Authentication callback from PocketBase. Receives a `token` and `user` object. Stores user data in session and sets access/refresh tokens as cookies.
*   `/login` (GET): Renders the login page.
*   `/logout` (GET): Logs out the user and clears the session and authentication cookies.
*   `/token/refresh` (GET): Refreshes the access token using the refresh token.

### Data Recording Endpoints

These endpoints are defined in `app.py` and handle audio recording and transcription submission.

*   `/start_session` (POST): Starts a new recording session. Requires `language`, `speakerName` (if auth disabled), and other metadata. CSRF protected. Initializes an `AudioDatasetPreparator` instance.
*   `/next_transcript` (GET): Retrieves the next transcription in the queue using the `LazyTranscriptLoader`.
*   `/prev_transcript` (GET): Retrieves the previous transcription using the `LazyTranscriptLoader`.
*   `/skip_transcript` (GET): Skips the current transcription and retrieves the next one using the `LazyTranscriptLoader`.
*   `/save_recording` (POST): Saves the audio recording and metadata. Requires an `audio` file. CSRF protected. The audio is processed (fade-in, trim, fade-out) before saving. Metadata is stored in the PostgreSQL database.
*   `/languages` (GET): Retrieves a list of supported languages from `language_config.py`.

### Validation Endpoints

These endpoints are defined in `validation_route.py` and handle audio validation by moderators.

*   `/validation/` (GET): Renders the validation interface (moderator access required).
*   `/validation/api/recordings` (GET): Retrieves a list of recordings for validation (moderator access required). Supports pagination and filtering by language and status.
*   `/validation/api/verify/<recording_id>` (POST): Verifies or rejects a recording (moderator access required). CSRF protected. Updates the recording status in the PostgreSQL database.
*   `/validation/api/audio/<filename>` (GET): Serves the audio file.
*   `/validation/api/delete/<recording_id>` (DELETE): Deletes a recording (moderator access required). CSRF protected.
*   `/validation/api/next_recording` (GET): Gets the next recording for validation (moderator access required). Uses the `assign_recording` function to assign a recording to the moderator.

### Admin Endpoints

These endpoints are defined in `admin_routes.py` and handle administrative tasks.

*   `/admin/` (GET): Renders the admin interface (admin access required).
*   `/admin/submit` (POST): Submits transcriptions from a file or text input (admin access required). Stores transcriptions in language-specific tables in the PostgreSQL database.
*   `/admin/users/moderators` (GET): Retrieves a list of moderators (admin access required).
*   `/admin/users/search` (GET): Searches for a user by email (admin access required).
*   `/admin/users/<user_id>/role` (POST): Updates a user's role (admin access required).
*   `/admin/sync/status` (GET): Checks the status of the dataset synchronization (admin access required).
*   `/admin/sync` (POST): Triggers dataset synchronization (admin access required).

### Super Admin Endpoints

These endpoints are defined in `super_admin.py` and handle super-administrative tasks. They require both admin access and super admin password verification.

*   `/admin/super/` (GET): Renders the super admin interface (admin access and super admin password required).
*   `/admin/super/verify` (POST): Verifies the super admin password.
*   `/admin/super/admins` (GET): Retrieves a list of admins (super admin access required).
*   `/admin/super/users/search` (GET): Searches for a user by email (super admin access required).
*   `/admin/super/users/<user_id>/role` (POST): Updates a user's role (super admin access required).

### Data Models

*   **User:**

    ```json
    {
        "id": "string",
        "email": "string",
        "name": "string",
        "role": "user" | "moderator" | "admin",
        "is_moderator": boolean,
        "gender": "M" | "F" | "O" | null,
        "age_group": "Teenagers" | "Adults" | "Elderly" | null,
        "country": "string" | null,
        "state_province": "string" | null,
        "city": "string" | null,
        "accent": "Rural" | "Urban" | null,
        "language": "string" | null
    }
    ```

*   **Recording:**

    ```json
    {
        "id": integer,
        "user_id": "string",
        "audio_filename": "string",
        "transcription_id": integer,
        "speaker_name": "string",
        "speaker_id": "string",
        "audio_path": "string",
        "sampling_rate": integer,
        "duration": float,
        "language": "string",
        "gender": "string",
        "country": "string",
        "state": "string",
        "city": "string",
        "status": "pending" | "verified" | "rejected",
        "verified_by": "string" | null,
        "username": "string",
        "age_group": "string",
        "accent": "string",
        "transcription": "string"
    }
    ```

## Key Classes and Functions

*   **`AudioDatasetPreparator` (prepare_dataset.py):**  Handles local storage and processing of audio files.  Includes functions for saving audio, adding metadata, and initializing storage directories.
*   **`LazyTranscriptLoader` (lazy_loader.py):** Lazily loads transcriptions in batches from the database to reduce memory usage. Implements randomization and caching.
*   **`DatasetSynchronizer` (dataset_sync.py):**  Synchronizes the dataset with a Hugging Face repository.  Includes functions for calculating file hashes, preparing Parquet files, and uploading files.
*   **`update_parquet_files` (prepare_parquet.py):** Extracts verified records from the PostgreSQL database and updates Parquet files for each language.
*   **`store_metadata` (database_manager.py):** Stores recording metadata in the appropriate language table in the PostgreSQL database.
*   **`assign_recording` (database_manager.py):** Assigns a recording to a moderator for validation.
*   **`verify_password_secure` (super_admin.py):** Securely verifies the super admin password using constant-time comparison.
*   **`set_security_headers` (security_middleware.py):** Sets security headers for all responses to protect against common web vulnerabilities.
*   **`csrf_protect` (security_middleware.py):** Decorator to protect routes against CSRF attacks.

## Database Schema

The application uses a PostgreSQL database with the following schema:

*   **`recordings_{language}`:** Stores metadata for audio recordings. Each language has its own table.
    *   `id` (SERIAL PRIMARY KEY)
    *   `user_id` (VARCHAR)
    *   `audio_filename` (VARCHAR)
    *   `transcription_id` (INTEGER, foreign key to `transcriptions_{language}.transcription_id`)
    *   `speaker_name` (VARCHAR)
    *   `speaker_id` (VARCHAR)
    *   `audio_path` (VARCHAR)
    *   `sampling_rate` (INTEGER)
    *   `duration` (FLOAT)
    *   `language` (VARCHAR(2))
    *   `gender` (VARCHAR(10))
    *   `country` (VARCHAR)
    *   `state` (VARCHAR)
    *   `city` (VARCHAR)
    *   `status` (VARCHAR(20), default 'pending')
    *   `verified_by` (VARCHAR, nullable=True)
    *   `username` (VARCHAR)
    *   `age_group` (VARCHAR)
    *   `accent` (VARCHAR)
*   **`transcriptions_{language}`:** Stores transcriptions. Each language has its own table.
    *   `transcription_id` (SERIAL PRIMARY KEY)
    *   `user_id` (VARCHAR(255))
    *   `transcription_text` (TEXT NOT NULL)
    *   `recorded` (BOOLEAN, default FALSE)
    *   `uploaded_at` (TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP)
*   **`validation_assignments`:** Stores assignments of recordings to moderators for validation.
    *   `id` (SERIAL PRIMARY KEY)
    *   `assigned_to` (VARCHAR(255) NOT NULL)
    *   `language` (VARCHAR(2) NOT NULL)
    *   `recording_id` (INTEGER NOT NULL)
    *   `assigned_at` (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
    *   `expires_at` (TIMESTAMP NOT NULL)
    *   `status` (VARCHAR(20) DEFAULT 'pending')

## Security Considerations

*   **CSRF Protection:**  Routes that modify data (e.g., `/save_recording`, `/admin/submit`, `/validation/api/verify/<recording_id>`) are protected against Cross-Site Request Forgery (CSRF) attacks using the `@csrf_protect` decorator.  Ensure that CSRF tokens are included in all POST, PUT, and DELETE requests.  The CSRF token is included in the `X-CSRF-Token` header.
*   **Authentication and Authorization:** User authentication and authorization are handled by PocketBase.  The `@login_required`, `@admin_required`, and `@super_admin_required` decorators are used to restrict access to specific routes based on user roles.
*   **Rate Limiting:** The super admin password verification process is rate-limited to prevent brute-force attacks.
*   **Input Validation:**  The `AudioMetadataSchema` in `input_validation.py` is used to validate audio metadata.
*   **Security Headers:** The `set_security_headers` function sets security headers to protect against common web vulnerabilities.
*   **Password Security:** The `verify_password_secure` function uses constant-time comparison to prevent timing attacks when verifying the super admin password.

## Best Practices

1.  **Secure Coding:**
    *   Always validate user inputs to prevent injection attacks.
    *   Use parameterized queries to prevent SQL injection.
    *   Implement proper authentication and authorization mechanisms.
    *   Keep dependencies up-to-date to patch security vulnerabilities.

2.  **Performance Optimization:**
    *   Use caching to reduce database load.
    *   Optimize database queries for faster retrieval.
    *   Use asynchronous tasks for long-running operations.
    *   Implement pagination for large datasets.

3.  **Code Style:**
    *   Follow PEP 8 guidelines for Python code.
    *   Write clear and concise code with meaningful variable names.
    *   Add comments to explain complex logic.
    *   Use logging for debugging and monitoring.

4.  **Configuration Management:**
    *   Use environment variables for configuration.
    *   Avoid hardcoding sensitive information in the code.
    *   Use a configuration file for non-sensitive settings.

5.  **Error Handling:**
    *   Implement proper error handling to prevent crashes.
    *   Log errors for debugging and monitoring.
    *   Provide informative error messages to the user.

## Contributing Guidelines

1.  **Fork the repository:**

    ```bash
    git clone --fork <your_fork_url>
    cd dataset-preparation-tool
    ```

2.  **Create a new branch:**

    ```bash
    git checkout -b feature/your-feature-name
    ```

3.  **Make changes:**

    *   Follow the code style guidelines.
    *   Write unit tests for your changes.
    *   Add comments to explain your code.

4.  **Test your changes:**

    ```bash
    pytest
    ```

5.  **Commit your changes:**

    ```bash
    git commit -m "feat: Add your feature"
    ```

6.  **Push your changes:**

    ```bash
    git push origin feature/your-feature-name
    ```

7.  **Create a pull request:**

    *   Submit a pull request to the `main` branch.
    *   Provide a clear and concise description of your changes.
    *   Address any feedback from the reviewers.

### Code of Conduct

Please adhere to the project's code of conduct. Be respectful and inclusive in your interactions with other contributors.

### Reporting Issues

If you encounter any issues, please report them on the project's issue tracker. Provide as much detail as possible, including steps to reproduce the issue.

## Environment Variables

A comprehensive list of environment variables used by the application:

*   `FLASK_SECRET_KEY`: Secret key for Flask application. Used for session management and CSRF protection.
*   `POSTGRES_URL`: Connection string for the PostgreSQL database.
*   `POCKETBASE_URL`: URL of the PocketBase instance.
*   `HF_TOKEN`: Hugging Face API token.
*   `HF_REPO_ID`: Hugging Face repository ID.
*   `SAVE_LOCALLY`: Boolean value indicating whether to save audio files locally.
*   `SUPER_ADMIN_PASSWORD`: Password for the super admin user.
*   `SUPER_USER_EMAILS`: Comma-separated list of email addresses for super users who cannot have their roles modified.
*   `ENABLE_AUTH`: Boolean value indicating whether authentication is enabled.
*   `JWT_SECRET_KEY`: Secret key for JWT encoding/decoding. Defaults to `FLASK_SECRET_KEY` if not set.
*   `TRANSCRIPT_BATCH_SIZE`: Number of transcripts to load in each batch by the `LazyTranscriptLoader`.
*   `SYNC_MEMORY_LIMIT_MB`: Memory limit (in MB) for dataset synchronization.
*   `NETWORK_TIMEOUT`: Network timeout (in seconds) for Hugging Face API requests.
*   `UPLOAD_CHUNK_SIZE`: Chunk size (in bytes) for uploading files to Hugging Face Hub.
*   `UPLOAD_BATCH_SIZE`: Number of files to upload in parallel during dataset synchronization.
*   `SYNC_HOUR`: Hour of the day (in UTC) to run the daily dataset synchronization job.
*   `SYNC_MINUTE`: Minute of the hour to run the daily dataset synchronization job.
*   `SYNC_TIMEZONE`: Timezone for the daily dataset synchronization job.
*   `MAX_UPLOAD_WORKERS`: Maximum number of worker threads to use for uploading files.
*   `MAX_UPLOAD_RETRIES`: Maximum number of retries for failed file uploads.
*   `FLASK_PORT`: Port on which the Flask application will listen.
*   `DATASET_BASE_DIR`: Base directory for all datasets. Defaults to `/app/datasets`.
*   `TEMP_FOLDER`: Temporary folder for storing audio files during processing. Defaults to `./temp`.

## Modules

### auth_middleware.py

This module handles authentication and authorization using PocketBase and JWTs.

*   `init_auth(app)`: Initializes authentication middleware. Sets up JWT secret key, initializes PocketBase client, and registers a `before_request` handler to validate access tokens.
*   `create_access_token(user_data, expires_delta=timedelta(minutes=60))`: Creates a new access token.
*   `create_refresh_token(user_data, expires_delta=timedelta(days=30))`: Creates a new refresh token.
*   `validate_token(token)`: Validates a JWT token.

### database_manager.py

This module manages database connections and operations.

*   `get_language_table(language)`: Gets or creates language-specific recordings table.
*   `store_metadata(metadata_dict)`: Stores recording metadata in the appropriate language table.
*   `store_transcription(transcription_text, language)`: Stores a transcription in the language-specific transcriptions table.
*   `get_available_languages()`: Gets a list of languages that have transcriptions available.
*   `ensure_transcription_table(conn, language)`: Ensures that the transcriptions table exists with the correct schema.
*   `get_transcriptions_for_language(language_code, include_recorded=False, limit=None, offset=0, exclude_ids=None, count_only=False, ids_only=False, specific_ids=None)`: Gets transcriptions for a language with various filtering options.
*   `table_exists(conn, table_name)`: Checks if a table exists in the database.
*   `get_dataset_stats()`: Gets dataset statistics from the PostgreSQL database.
*   `create_assignments_table(conn)`: Creates the `validation_assignments` table.
*   `cleanup_completed_assignments()`: Removes completed assignments from the `validation_assignments` table.
*   `assign_recording(language, moderator_id)`: Assigns a recording to a moderator for validation.
*   `complete_assignment(language, recording_id, moderator_id, status)`: Marks an assignment as completed.

### dataset_sync.py

This module handles the synchronization of the dataset with the Hugging Face Hub.

*   `DatasetSynchronizer`: Class that manages the dataset synchronization process.
    *   `sync_dataset()`: Synchronizes the dataset with the Hugging Face Hub.
    *   `is_syncing()`: Checks if a sync is in progress.
    *   `_get_modified_files()`: Gets a list of new or modified files since the last sync.
    *   `_upload_file_with_retry(file_path, retry_count=0)`: Uploads a file to the Hugging Face Hub with retry logic.
    *   `_parallel_upload(files)`: Uploads multiple files in parallel.
    *   `_update_sync_state()`: Updates the sync state file.
*   `sync_job()`: Function that is called by the scheduler to run the synchronization process.
*   `init_scheduler()`: Initializes the APScheduler to run the synchronization job.

### input_validation.py

This module defines validation schemas for input data.

*   `AudioMetadataSchema`: Marshmallow schema for validating audio metadata.
*   `validate_audio_metadata(data)`: Validates audio metadata using the `AudioMetadataSchema`.

### language_config.py

This module defines the supported languages for the application.

*   `LANGUAGES`: Dictionary containing the language codes, names, and native names.
*   `get_language_name(code)`: Gets the English name of a language from its code.
*   `get_native_name(code)`: Gets the native name of a language from its code.
*   `get_language_code(name)`: Gets the language code from its English name.
*   `get_all_languages()`: Gets a list of all supported languages.

### lazy_loader.py

This module implements the `LazyTranscriptLoader` class, which lazily loads transcriptions in batches from the database.

*   `LazyTranscriptLoader`: Class that manages the lazy loading of transcriptions.
    *   `get_current()`: Gets the transcript at the current index.
    *   `move_next()`: Moves to the next transcript.
    *   `move_prev()`: Moves to the previous transcript.
    *   `get_progress()`: Gets the current progress information.

### prepare_dataset.py

This module handles the preparation of the audio dataset.

*   `AudioDatasetPreparator`: Class that manages the preparation of the audio dataset.
    *   `save_audio(pcm_data, sample_rate, filename, bits_per_sample=16, channels=1, already_processed=False)`: Saves an audio file in the language-specific audio folder.
    *   `add_metadata(recording_data)`: Adds metadata to the recording.
*   `should_save_locally()`: Checks if the application should save files locally.

### prepare_parquet.py

This module handles the creation and updating of Parquet files.

*   `update_parquet_files()`: Extracts verified records from the PostgreSQL database and updates the Parquet files for each language.

### security_middleware.py

This module implements security middleware for the application.

*   `generate_csrf_token()`: Generates a new CSRF token.
*   `validate_csrf_token(token)`: Validates a CSRF token.
*   `csrf_protect(func)`: Decorator to protect routes against CSRF attacks.
*   `set_security_headers(response)`: Sets security headers for all responses.

### super_admin.py

This module handles super admin functionality.

*   `verify_password_secure(provided_password)`: Securely verifies the super admin password.
*   `super_admin_required(f)`: Decorator to protect routes that require super admin access.
*   `clean_expired_verifications()`: Cleans up expired verification sessions.
*   `init_cleanup(app)`: Initializes the verification cleanup system.

### upload_manager.py

This module manages the queuing and processing of file uploads.

*   `UploadManager`: Class that manages the queuing and processing of file uploads.
    *   `queue_upload(task_id, push_func, *args)`: Adds an upload task to the queue.
    *   `check_status(task_id)`: Checks if a specific upload is complete.
    *   `get_pending_count()`: Gets the number of pending uploads.

### validation_route.py

This module defines the routes for the validation interface.

*   `assign_recording(language, moderator_id)`: Assigns a recording to a moderator for validation.
*   `complete_assignment(language, recording_id, moderator_id, status)`: Marks an assignment as completed.

## JavaScript Code (recorder.js)

This section details the JavaScript code (`recorder.js`) responsible for handling audio recording, playback, and UI interactions within the web application.

### Core Functionalities

1.  **Audio Recording:**
    *   Utilizes the `MediaRecorder` API to capture audio from the user's microphone.
    *   Configures audio parameters such as sample rate (48kHz), channel count (mono), and applies noise suppression and echo cancellation.
    *   Provides visual feedback during recording using a recording indicator.
    *   Implements a maximum recording duration (30 seconds) to prevent excessively long recordings.
    *   Processes raw PCM data to apply fade-in, trim, and fade-out effects for improved audio quality.
2.  **Audio Playback:**
    *   Allows users to play back recorded audio using the `<audio>` element.
    *   Provides controls for starting and stopping playback.
    *   Handles conversion of raw PCM data to a playable audio format.
3.  **UI Interactions:**
    *   Manages the state of UI elements (buttons, progress indicators) based on the recording and playback status.
    *   Provides keyboard shortcuts for common actions (recording, playback, saving, skipping).
    *   Displays toast notifications to provide feedback to the user.
    *   Handles session management, including storing and retrieving session data from `sessionStorage`.
    *   Dynamically adjusts the font size of the transcript text for improved readability.
    *   Manages the visibility of the settings panel on mobile devices.

### Key Variables

*   `mediaRecorder`: The `MediaRecorder` object used for capturing audio.
*   `audioChunks`: An array to store the captured audio data chunks.
*   `audioBlob`: A `Blob` object representing the complete audio recording.
*   `sessionData`: An object to store session-related data.
*   `SESSION_STORAGE_KEY`: A constant defining the key used to store session data in `sessionStorage`.
*   `CURRENT_ROW_KEY`: A constant defining the key used to store the current row number in `sessionStorage`.
*   `NAVIGATION_BUTTONS`: An array of button IDs used for navigation.
*   `MIN_FONT_SIZE`, `MAX_FONT_SIZE`, `FONT_SIZE_STEP`: Constants defining the font size range and step for transcript text.
*   `pendingUploads`: A `Map` to track pending file uploads.
*   `MAX_RECORDING_DURATION`: Maximum recording duration in milliseconds.
*   `isAuthenticated`: A boolean indicating whether the user is authenticated.
*   `audioPlayer`: A reference to the audio player object.
*   `isSaving`: A boolean indicating whether the application is currently saving a recording.
*   `audioContext`: The `AudioContext` object used for audio processing and playback.
*   `scriptProcessor`: The `ScriptProcessorNode` object used for capturing raw PCM data.
*   `audioInput`: The `MediaStreamSource` object used as the input to the audio processing graph.
*   `rawPCMData`: An array to store raw PCM data chunks.
*   `isRecordingPCM`: A boolean indicating whether raw PCM data is currently being recorded.

### Core Functions

*   `setupAudioContext()`: Creates and configures the `AudioContext` object.
*   `stopPCMRecording()`: Stops the PCM recording process and creates an audio blob.
*   `processPCMData()`: Processes the raw PCM data to apply fade-in, trim, and fade-out effects.
*   `convertFloat32ToInt16(float32Array)`: Converts a `Float32Array` to an `Int16Array`.
*   `updateButtonStates(state)`: Updates the disabled state of UI buttons based on the current application state.
*   `showToast(message, type = 'info')`: Displays a toast notification with the specified message and type.
*   `updateTranscriptDisplay(data)`: Updates the transcript text and progress display.
*   `loadNextTranscript()`: Loads the next transcript from the server.
*   `increaseFontSize()`: Increases the font size of the transcript text.
*   `decreaseFontSize()`: Decreases the font size of the transcript text.
*   `clearSession()`: Clears the session data and resets the UI.

### Event Listeners

*   `DOMContentLoaded`: Initializes the UI, loads session data, and sets up event listeners.
*   `recordBtn.addEventListener('click', ...)`: Handles the start/stop recording action.
*   `playBtn.addEventListener('click', ...)`: Handles the play/stop playback action.
*   `saveBtn.addEventListener('click', ...)`: Handles the save recording action.
*   `rerecordBtn.addEventListener('click', ...)`: Handles the rerecord action.
*   `prevBtn.addEventListener('click', ...)`: Handles the previous transcript navigation.
*   `skipBtn.addEventListener('click', ...)`: Handles the skip transcript navigation.
*   `keydown`: Handles keyboard shortcuts.

## HTML Templates

This section describes the HTML templates used in the application.

### base.html (Not explicitly provided, but assumed for common elements)

This template likely provides the base HTML structure for all pages, including:

*   `<!DOCTYPE html>` declaration
*   `<html lang="en">` tag
*   `<head>` section with common meta tags, CSS links, and JavaScript includes
*   `<body>` section with common header and footer elements
*   `container-fluid` div for layout

### index.html

This template renders the main dataset preparation interface.

*   **Key Elements:**
    *   Navigation bar with links to "Validate" (if moderator), "Admin Panel" (if admin), and "Logout".
    *   Recording interface with transcript display, recording controls, and progress indicator.
    *   Settings panel with form for configuring session parameters.
*   **Dynamic Content:**
    *   User role-based access to "Validate" and "Admin Panel" links.
    *   Transcript text loaded dynamically from the server.
    *   Progress indicator updated dynamically during recording.
    *   Form fields pre-filled with user data from the session.
*   **JavaScript Integration:**
    *   Includes `recorder.js` for handling audio recording and UI interactions.
    *   Uses `country-states.js` for populating country and state dropdowns.
    *   Uses Bootstrap for UI components and styling.
*   **Security:**
    *   Includes a CSRF token meta tag for protecting against CSRF attacks.

### login.html

This template renders the login page.

*   **Key Elements:**
    *   Google Sign-In button.
    *   Error message display.
    *   Footer with links to "Terms of Use / Privacy Policy" and "Contact".
*   **Dynamic Content:**
    *   PocketBase URL configured via `config.POCKETBASE_URL`.
*   **JavaScript Integration:**
    *   Uses the PocketBase JavaScript SDK for handling authentication.
*   **Security:**
    *   Relies on PocketBase for secure authentication.

### validate.html

This template renders the audio validation interface for moderators.

*   **Key Elements:**
    *   Navigation bar with a "Back" link.
    *   Filters panel for selecting language and status.
    *   Single recording view with audio player, transcript display, and validation actions.
*   **Dynamic Content:**
    *   List of languages loaded dynamically from the server.
    *   Audio recordings and transcriptions loaded dynamically from the server.
    *   User role-based access to the validation interface.
*   **JavaScript Integration:**
    *   Uses JavaScript to handle filtering, loading recordings, and submitting validation decisions.
    *   Uses Bootstrap for UI components and styling.

### admin.html

This template renders the admin dashboard.

*   **Key Elements:**
    *   Navigation bar with links to "Super Admin", "Back to Recording", and "Logout".
    *   Statistics cards displaying key dataset metrics.
    *   Language statistics table.
    *   User management section for adding/removing moderators.
    *   Form for uploading transcriptions.
    *   Dataset synchronization section for triggering dataset synchronization.
*   **Dynamic Content:**
    *   Dataset statistics loaded dynamically from the server.
    *   List of languages loaded dynamically from the server.
    *   List of moderators loaded dynamically from the server.
*   **JavaScript Integration:**
    *   Uses JavaScript to handle form submissions, user management, and dataset synchronization.
    *   Uses Bootstrap for UI components and styling.

### super_admin.html

This template renders the super admin interface.

*   **Key Elements:**
    *   Password entry section for verifying super admin credentials.
    *   Main content section with user management tools.
*   **Dynamic Content:**
    *   List of admins loaded dynamically from the server.
    *   Search results for users.
*   **JavaScript Integration:**
    *   Uses JavaScript to handle password verification, user searching, and role updating.
    *   Uses Bootstrap for UI components and styling.
*   **Security:**
    *   Requires super admin password verification to access the main content section.

### privacy.html

This template renders the Terms of Use & Privacy Policy page.

*   **Key Elements:**
    *   Terms of Use and Privacy Policy content.
    *   Footer with links to "TERMS OF USE / PRIVACY POLICY" and "CONTACT".
*   **Static Content:**
    *   The content is mostly static, but can be updated as needed.

### error.html

This template renders a generic error page.

*   **Key Elements:**
    *   Error code and message.
    *   Link to return to the home page.
*   **Dynamic Content:**
    *   Error code and message passed from the server.

### download.html

This template renders the audio dataset download interface.

*   **Key Elements:**
    *   Form for selecting download parameters (language, demographics, duration, etc.).
    *   Statistics display showing dataset characteristics.
*   **Dynamic Content:**
    *   List of languages loaded dynamically from the server.
    *   Dataset statistics loaded dynamically from the server.
*   **JavaScript Integration:**
    *   Uses JavaScript to populate country and state dropdowns.
    *   Uses JavaScript to update statistics based on selected filters.

