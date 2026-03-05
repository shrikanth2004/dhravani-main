import os
import ctypes
import platform
os.environ['TRANSFORMERS_OFFLINE'] = '1'
os.environ['HF_HUB_OFFLINE'] = '1'

if platform.system() == "Windows":
    try:
        dll_path = r"C:\Users\HP\OneDrive\Downloads\Dhravani-updated-main\venv\lib\site-packages\torch\lib\c10.dll"
        ctypes.CDLL(dll_path)
    except Exception:
        pass


from flask import Flask, render_template, request, jsonify, send_file, send_from_directory, redirect, url_for, session, abort, make_response, Response
from flask_login import login_required
from prepare_dataset import AudioDatasetPreparator, should_save_locally
import json
import pandas as pd
import os
from pathlib import Path
import soundfile as sf
import numpy as np
from datetime import datetime
from werkzeug.utils import secure_filename
import uuid
import logging
from traceback import format_exc
from functools import wraps
from dotenv import load_dotenv
from auth_middleware import init_auth, create_access_token, create_refresh_token, validate_token
from language_config import get_all_languages, get_language_name, get_language_code
from security_middleware import set_security_headers, generate_csrf_token, csrf_protect
from werkzeug.security import safe_join
import secrets
from datetime import datetime, timedelta
from collections import defaultdict
import time
from dataset_sync import init_scheduler
from pocketbase import PocketBase
from validation_route import validation
from admin_routes import admin_bp
from database_manager import (
    get_transcriptions_for_language,
    get_available_languages,
    store_metadata,
    engine,
    get_available_domains,
    get_available_subdomains,
    get_all_domains_db as get_all_domains,
    get_domain_subdomains_db as get_domain_subdomains,
    get_domain_name_db as get_domain_name,
    get_subdomain_by_mnemonic_db as get_subdomain_by_mnemonic
)
from sqlalchemy import text
import wave
import struct
from io import BytesIO
from lazy_loader import LazyTranscriptLoader
from super_admin import super_admin_bp
from flask_compress import Compress

from transcriber.english_transcriber import transcribe_english as transcribe_english
from transcriber.kannada_transcriber import transcribe_kannada as transcribe_kannada




TEMP_FOLDER = os.getenv("TEMP_FOLDER", "./temp")
os.makedirs(TEMP_FOLDER, exist_ok=True) 

# Configure logging early - MOVED UP FROM LINE 67
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Define TEMP_FOLDER as environment variable
os.environ['TEMP_FOLDER'] = './temp'

ALLOWED_EXTENSIONS = {'csv'}
TEMP_FOLDER = os.environ['TEMP_FOLDER']
MAX_AUDIO_DURATION = 30  # seconds

# Create the Flask app
app = Flask(__name__)
app.config['TEMP_FOLDER'] = TEMP_FOLDER

# Initialize Flask-Compress BEFORE any routes
compress = Compress()
compress.init_app(app)

# Configure compression settings for better performance
app.config['COMPRESS_MIMETYPES'] = [
    'text/html', 
    'text/css', 
    'text/javascript', 
    'application/javascript',
    'application/json',
    'application/xml',
    'text/xml',
    'text/plain'
]
app.config['COMPRESS_LEVEL'] = 6  # Higher level = better compression but more CPU
app.config['COMPRESS_MIN_SIZE'] = 500  # Only compress files larger than 500 bytes
# app.config['POCKETBASE_URL'] = "http://127.0.0.1:8090"

# Use environment variable for secret key, falling back to generated one only if needed
app.secret_key = os.getenv('FLASK_SECRET_KEY')
if not app.secret_key:
    # Generate a key only if not provided in environment
    app.secret_key = secrets.token_hex(32)
    logger.warning("No FLASK_SECRET_KEY found in environment. Using generated key - sessions will not persist across restarts!")

# Update these app configuration settings
app.config.update(
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    PERMANENT_SESSION_LIFETIME=timedelta(days=30),  # Increase session lifetime to 30 days
    MAX_CONTENT_LENGTH=16 * 1024 * 1024  # 16MB max file size
)

# Add this function after app configuration but before any routes
def set_auth_cookies(response, access_token, refresh_token):
    """Set secure cookies for authentication"""
    # Set secure HTTP-only cookies
    max_age = 30 * 24 * 60 * 60  # 30 days in seconds
    response.set_cookie(
        'refresh_token',
        refresh_token,
        max_age=max_age,
        httponly=True, 
        secure=True,
        samesite='Lax',
        path='/'
    )
    # Access token with shorter lifetime (1 hour)
    response.set_cookie(
        'access_token',
        access_token,
        max_age=3600,  # 1 hour
        httponly=True, 
        secure=True,
        samesite='Lax',
        path='/'
    )
    return response

# Add middleware before route definitions
@app.after_request
def after_request(response):
    return set_security_headers(response)

# Global variables to maintain state
active_sessions = {}
session_timestamps = {}
SESSION_TIMEOUT = 3600  # 1 hour timeout

# Initialize PocketBase and register blueprints early
pb = None
if os.getenv('ENABLE_AUTH', 'true').lower() == 'true':
    try:
        pb_url = os.getenv('POCKETBASE_URL')
        if not pb_url:
            logger.warning("PocketBase URL not found in environment variables")
        else:
            pb = PocketBase(pb_url)
            logger.info("PocketBase client initialized successfully")
            
            # Initialize scheduler first and attach to app
            scheduler = init_scheduler()
            app.scheduler = scheduler
            logger.info("Scheduler initialized and attached to app")
            
            # Initialize auth
            init_auth(app)
            # Register the blueprints
            validation.pb = pb
            app.register_blueprint(validation)
            app.register_blueprint(admin_bp)
            app.register_blueprint(super_admin_bp)
            
            # Initialize super admin cleanup system
            from super_admin import init_cleanup
            init_cleanup(app)
            
            logger.info("Registered admin, validation, and super admin blueprints")
    except Exception as e:
        logger.error(f"Failed to initialize PocketBase client: {str(e)}")

def cleanup_expired_sessions():
    """Remove expired sessions"""
    try:
        current_time = time.time()
        expired = []
        
        for session_id, timestamp in session_timestamps.items():
            if current_time - timestamp > SESSION_TIMEOUT:
                expired.append(session_id)
        
        for session_id in expired:
            # Safely remove expired sessions
            active_sessions.pop(session_id, None)
            session_timestamps.pop(session_id, None)
            logger.info(f"Cleaned up expired session: {session_id}")
            
    except Exception as e:
        logger.error(f"Error cleaning up sessions: {str(e)}")

def get_user_session():
    """Get or create user session"""
    cleanup_expired_sessions()
    
    session_id = session.get('session_id')
    if not session_id or session_id not in active_sessions:
        session_id = secrets.token_urlsafe(32)
        session['session_id'] = session_id
        active_sessions[session_id] = {
            'preparator': None,
            'transcripts': None,
            'current_index': 0
        }
    
    # Update timestamp
    session_timestamps[session_id] = time.time()
    return active_sessions[session_id]


# Modify the login_required decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not os.getenv('ENABLE_AUTH', 'true').lower() == 'true':
            return f(*args, **kwargs)
            
        if not session.get('user'):
            return jsonify({'error': 'Authentication required', 'code': 'AUTH_REQUIRED'}), 401
            
        return f(*args, **kwargs)
    return decorated_function

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.before_request
def before_request():
    # Add CSRF token to all responses
    if request.endpoint not in ['static', 'favicon']:
        token = generate_csrf_token()
        logger.debug(f"Generated/Retrieved CSRF token for {request.endpoint}: {token[:10]}...")

@app.route('/auth/callback', methods=['POST'])
def auth_callback():
    try:
        data = request.get_json()
        token = data.get('token')
        user = data.get('user')
        
        if not token or not user:
            logger.error("No token or user data provided")
            return jsonify({'error': 'Invalid auth data'}), 400

        # Make session permanent so it survives browser restarts
        session.permanent = True
        
        # Store all necessary user data in session to avoid future API calls
        session['user'] = {
            'id': user.get('id'),
            'email': user.get('email', ''),
            'name': user.get('name', ''),
            'token': token,  # Store token in session for auth restoration
            'is_moderator': user.get('role', '').lower() in ['moderator', 'admin'],
            'role': user.get('role', '').lower(),
            'gender': user.get('gender', ''),
            'age_group': user.get('age_group', ''),
            'country': user.get('country', ''),
            'state_province': user.get('state_province', ''),
            'city': user.get('city', ''),
            'accent': user.get('accent', ''),
            'language': user.get('language', '')
        }

        # Only save token in PocketBase auth store
        app.pb.auth_store.save(token, None)
        
        # Create tokens
        access_token = create_access_token(session['user'])
        refresh_token = create_refresh_token(session['user'])
        
        # Store tokens in session for middleware access
        session['access_token'] = access_token
        session['refresh_token'] = refresh_token
        
        response = jsonify({'status': 'success'})
        return set_auth_cookies(response, access_token, refresh_token)

    except Exception as e:
        logger.error(f"Auth error: {str(e)}")
        return jsonify({'error': 'Authentication failed'}), 500

@app.route('/login')
def login():
    return render_template('login.html', 
                         config={
                             'POCKETBASE_URL': os.getenv('POCKETBASE_URL', 'http://127.0.0.1:8090')
                         })

@app.route('/')
@login_required
def index():
    enable_auth = os.getenv('ENABLE_AUTH', 'true').lower() == 'true'
    if enable_auth and not session.get('user'):
        return redirect(url_for('login'))
    session['last_visited'] = time.time()
    return render_template('dashboard.html', 
                         enable_auth=enable_auth,
                         session=session)

@app.route('/record')
@login_required
def recorder():
    return render_template('index.html', enable_auth=True, session=session)


@app.route("/transcriber", methods=["GET", "POST"])
@login_required
def transcriber_page():
    if request.method == "POST":
        file = request.files.get("audio")
        transcriber_choice = request.form.get("transcriber", "kannada").lower()

        if not file:
            return jsonify({"error": "No audio file uploaded"}), 400

        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config["TEMP_FOLDER"], filename)
        file.save(file_path)

        try:
            if transcriber_choice == "english":
                transcript = transcribe_english(file_path)
            else:
                transcript = transcribe_kannada(file_path)

            os.remove(file_path)

            return Response(
                json.dumps({"transcription": transcript}, ensure_ascii=False),
                content_type="application/json; charset=utf-8"
            )

        except Exception as e:
            logger.error(f"Transcription failed: {str(e)}", exc_info=True)
            return jsonify({"error": f"Failed to transcribe: {str(e)}"}), 500

    return render_template("transcriber.html", enable_auth=True, session=session)



@app.route('/logout')
def logout():
    session.clear()
    response = make_response(redirect(url_for('login')))
    # Clear all cookies
    response.set_cookie('refresh_token', '', expires=0)
    response.set_cookie('access_token', '', expires=0)
    return response

def update_user_profile(user_id, data):
    """Update user profile in PocketBase"""
    try:
        # Check if PocketBase client is authenticated
        if not app.pb.auth_store.token:
            logger.warning("PocketBase client not authenticated, can't update profile")
            return False
            
        # Filter out None or empty string values
        update_data = {k: v for k, v in data.items() if v}
        if update_data:
            try:
                app.pb.collection('users').update(user_id, update_data)
                logger.info(f"Updated user profile for {user_id}")
                return True
            except Exception as e:
                if hasattr(e, 'status') and e.status == 404:
                    logger.warning(f"User not found in PocketBase (id: {user_id})")
                    # Force user to login again by clearing session
                    session.clear()
                    return False
                raise  # Re-raise other errors
        return True
    except Exception as e:
        logger.warning(f"Failed to update user profile: {e}")
        return False

@app.route('/start_session', methods=['POST'])
@csrf_protect
@login_required
def start_session():
    try:
        user_session = get_user_session()
        
        # Get form data
        language = request.form.get('language')
        if not language:
            return jsonify({'error': 'Language is required'}), 400
            
        if not get_language_name(language):
            return jsonify({'error': f'Invalid language code: {language}'}), 400

        # Get domain and subdomain from form, ensure they're not None/empty
        domain = request.form.get('domain', '').strip()
        subdomain = request.form.get('subdomain', '').strip()
        
        if not domain:
            return jsonify({'error': 'Domain is required'}), 400
        
        if not subdomain:
            return jsonify({'error': 'Subdomain is required'}), 400
        
        # Validate domain and subdomain exist in database
        available_domains = get_available_domains()
        if not available_domains:
            return jsonify({'error': 'No domains available'}), 400
        
        if domain not in available_domains:
            return jsonify({'error': f'Invalid domain: {domain}'}), 400
            
        available_subdomains = get_available_subdomains(domain)
        if not available_subdomains:
            return jsonify({'error': 'No subdomains available for selected domain'}), 400
        
        if subdomain not in available_subdomains:
            return jsonify({'error': f'Invalid subdomain: {subdomain}'}), 400

        # Create lazy loader for transcripts instead of loading all at once
        try:
            # Get batch size from environment or use default
            batch_size = int(os.getenv('TRANSCRIPT_BATCH_SIZE', '50'))
            
            # Initialize the lazy loader with domain and subdomain filters
            transcript_loader = LazyTranscriptLoader(
                language=language,
                batch_size=batch_size,
                randomize=True,  # Keep the randomization
                domain=domain,    # Add domain filter
                subdomain=subdomain  # Add subdomain filter
            )
            
            # Check if we have any transcripts with the selected domain/subdomain
            progress = transcript_loader.get_progress()
            if progress['total'] == 0:
                return jsonify({'error': f'No available transcripts for the selected domain ({domain}) and subdomain ({subdomain})'}), 400
            
            logger.debug(f"Lazy loader initialized with {progress['loaded']} transcripts loaded, {progress['total']} total for domain {domain}, subdomain {subdomain}")
            
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            return jsonify({'error': 'Error fetching transcriptions'}), 500

        # Get speaker name and handle user identification
        enable_auth = os.getenv('ENABLE_AUTH', 'true').lower() == 'true'
        if enable_auth and session.get('user'):
            user_id = session['user']['id']
            try:
                # Update user profile in PocketBase with the form data
                profile_data = {
                    'gender': request.form.get('gender'),
                    'age_group': request.form.get('age_group'),
                    'country': request.form.get('country'),
                    'state_province': request.form.get('state'),
                    'city': request.form.get('city'),
                    'accent': request.form.get('accent'),
                    'language': request.form.get('language'),
                    'domain': domain,
                    'subdomain': subdomain
                }
                
                # Try to update user profile, if it fails due to auth issues, return an error
                profile_updated = update_user_profile(user_id, profile_data)
                if not profile_updated:
                    return jsonify({
                        'error': 'Failed to update user profile. Please try logging in again.',
                        'code': 'PROFILE_UPDATE_ERROR'
                    }), 401
                
                speaker_name = session['user'].get('name', '').strip()
                if not speaker_name:
                    speaker_name = session['user'].get('email', '').split('@')[0].strip()
            except Exception as e:
                logger.error(f"Error updating user profile: {str(e)}")
                return jsonify({'error': 'Failed to update user profile'}), 500
        else:
            speaker_name = request.form.get('speakerName', '').strip()
            if not speaker_name:
                return jsonify({'error': 'Speaker name is required'}), 400
            user_id = 'anonymous'

        # Initialize the AudioDatasetPreparator with empty transcripts list
        # We'll get transcripts one by one from the lazy loader
        user_session['preparator'] = AudioDatasetPreparator(
            [],  # Empty initial list
            user_id=user_id
        )
        
        # Set session parameters
        user_session['preparator'].speaker_name = speaker_name
        user_session['preparator'].gender = request.form.get('gender')
        user_session['preparator'].language = language
        user_session['preparator'].country = request.form.get('country')
        user_session['preparator'].state = request.form.get('state')
        user_session['preparator'].city = request.form.get('city')
        user_session['preparator'].age_group = request.form.get('age_group')
        user_session['preparator'].accent = request.form.get('accent')
        user_session['preparator'].domain = domain
        user_session['preparator'].subdomain = subdomain
        
        # Store transcript loader in session instead of all transcripts
        user_session['transcript_loader'] = transcript_loader
        user_session['current_index'] = 0  # Keep this for compatibility
        user_session['transcript_order'] = 'random'  # Indicate that transcripts are randomized
        
        # Get the first transcript to ensure it's loaded
        first_transcript = transcript_loader.get_current()
        if not first_transcript:
            return jsonify({'error': 'Failed to load first transcript'}), 500

        # After successful save to PocketBase, update the Flask session
        if 'user' in session:
            # Create a copy to avoid modifying the session directly
            user_data = dict(session['user'])
            
            # Update user session data with form values
            user_data.update({
                'language': language,
                'gender': request.form.get('gender'),
                'country': request.form.get('country'),
                'state_province': request.form.get('state'),
                'city': request.form.get('city'),
                'age_group': request.form.get('age_group'),
                'accent': request.form.get('accent'),
                'domain': domain,
                'subdomain': subdomain
            })
            
            # Save updated values back to session
            session['user'] = user_data
            
            # Force session data to be saved
            session.modified = True
            
        return jsonify({
            'status': 'success',
            'total': progress['total'],
            'language_name': get_language_name(language),
            'speaker_name': speaker_name,
            'transcript_order': 'random',
            'domain': domain,
            'subdomain': subdomain
        })
        
    except Exception as e:
        logger.error(f"Error starting session: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/next_transcript')
@login_required
def next_transcript():
    user_session = get_user_session()
    transcript_loader = user_session.get('transcript_loader')
    
    if not transcript_loader:
        return jsonify({'error': 'No active session'}), 400
    
    # Get current transcript
    current = transcript_loader.get_current()
    if not current:
        return jsonify({'finished': True})
    
    # Get progress information
    progress = transcript_loader.get_progress()
    
    return jsonify({
        'finished': False,
        'transcript': current['text'],
        'current': progress['current'],
        'total': progress['total'],
        'previously_recorded': current.get('recorded', False),
        'loaded': progress['loaded'],
        'randomized': True  # Add this flag to indicate randomized order
    })

# Add this helper function
def validate_session_state(user_session):
    """Validate session state and transcripts"""
    if not user_session:
        return False, 'No active session found'
        
    if not user_session.get('transcripts'):
        return False, 'No transcripts loaded in session'
        
    if not isinstance(user_session.get('current_index'), int):
        user_session['current_index'] = 0
        
    return True, None

@app.route('/prev_transcript')
@login_required
def prev_transcript():
    try:
        user_session = get_user_session()
        transcript_loader = user_session.get('transcript_loader')
        
        if not transcript_loader:
            return jsonify({'error': 'No active session found', 'code': 'NO_SESSION'}), 400

        # Get current transcript before moving (for boundary case)
        current = transcript_loader.get_current()
        if not current:
            return jsonify({'error': 'No current transcript available', 'code': 'NO_TRANSCRIPT'}), 400
            
        # Get progress information before moving
        current_progress = transcript_loader.get_progress()
            
        # Try to move to previous transcript
        prev_transcript = transcript_loader.move_prev()
        
        # If we're at the beginning, return the current transcript with error message
        if prev_transcript is None:
            return jsonify({
                'error': 'Already at first transcript',
                'code': 'BOUNDARY_ERROR',
                'transcript': current['text'],
                'current': current_progress['current'],
                'total': current_progress['total'],
                'previously_recorded': current.get('recorded', False)
            }), 200  # Return 200 since this is an expected condition
        
        # Return the previous transcript (which is now the current one)
        progress = transcript_loader.get_progress()
        
        return jsonify({
            'transcript': prev_transcript['text'],
            'current': progress['current'],
            'total': progress['total'],
            'previously_recorded': prev_transcript.get('recorded', False)
        })
            
    except Exception as e:
        logger.error(f"Error in prev_transcript: {str(e)}")
        return jsonify({
            'error': 'Failed to navigate to previous transcript',
            'code': 'NAVIGATION_ERROR',
            'details': str(e)
        }), 500

@app.route('/skip_transcript')
@login_required
def skip_transcript():
    try:
        user_session = get_user_session()
        transcript_loader = user_session.get('transcript_loader')
        
        if not transcript_loader:
            return jsonify({'error': 'No active session found', 'code': 'NO_SESSION'}), 400

        # Get current transcript before moving (for boundary case)
        current = transcript_loader.get_current()
        if not current:
            return jsonify({'error': 'No current transcript available', 'code': 'NO_TRANSCRIPT'}), 400
            
        # Get progress information before moving
        current_progress = transcript_loader.get_progress()
        
        # Try to move to next transcript
        next_transcript = transcript_loader.move_next()
        
        # If we're at the end, return the current transcript with error message
        if next_transcript is None:
            return jsonify({
                'error': 'Already at last transcript',
                'code': 'BOUNDARY_ERROR',
                'transcript': current['text'],
                'current': current_progress['current'],
                'total': current_progress['total'],
                'previously_recorded': current.get('recorded', False),
                'at_end': True  # Add a specific flag to indicate we're at the end
            }), 200  # Return 200 since this is an expected condition
        
        # Return the next transcript (which is now the current one)
        progress = transcript_loader.get_progress()
        
        return jsonify({
            'transcript': next_transcript['text'],
            'current': progress['current'],
            'total': progress['total'],
            'previously_recorded': next_transcript.get('recorded', False)
        })
            
    except Exception as e:
        logger.error(f"Error in skip_transcript: {str(e)}")
        return jsonify({
            'error': 'Failed to skip to next transcript',
            'code': 'NAVIGATION_ERROR',
            'details': str(e)
        }), 500

@app.route('/save_recording', methods=['POST'])
@csrf_protect
@login_required
def save_recording():
    try:
        user_session = get_user_session()
        if 'audio' not in request.files:
            return jsonify({'error': 'No audio file'}), 400

        audio_file = request.files['audio']
        
        # Ensure a session preparator is set
        if not user_session.get('preparator'):
            return jsonify({'error': 'No active session found. Please start a session first.'}), 400

        transcript_loader = user_session.get('transcript_loader')
        if not transcript_loader:
            return jsonify({'error': 'No transcripts found in session'}), 400
            
        # Get the current transcript
        current_transcript = transcript_loader.get_current()
        if not current_transcript:
            return jsonify({'error': 'Current transcript not found'}), 400
        
        # Handle user identification based on auth status
        enable_auth = os.getenv('ENABLE_AUTH', 'true').lower() == 'true'
        if enable_auth and session.get('user'):
            user_id = session['user']['id']
            username = session['user'].get('email', '').split('@')[0]
        else:
            user_id = 'anonymous'
            username = user_session['preparator'].speaker_name

        # Generate unique filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        user_id_prefix = user_id[:int(len(user_id) * 0.6)]
        filename = f"{user_id_prefix}_{timestamp}.wav"

        # Get PCM data parameters
        sample_rate = int(request.form.get('sampleRate', 48000))
        bits_per_sample = int(request.form.get('bitsPerSample', 16))
        channels = int(request.form.get('channels', 1))
        already_processed = request.form.get('trimmed', 'false').lower() == 'true'  # Reuse 'trimmed' field but treat as processed
        
        # Read PCM data from file
        pcm_data = audio_file.read()
        
        # Calculate duration based on PCM data length, accounting for processing
        bytes_per_sample = bits_per_sample // 8
        original_duration = len(pcm_data) / (sample_rate * channels * bytes_per_sample)
        
        # If client reported processing, use the original duration
        # Otherwise, account for server-side processing (trim at end)
        if already_processed:
            duration = original_duration
        else:
            # Subtract 150ms for end trimming if the audio is long enough
            trim_duration = 0.15 if original_duration > 0.5 else 0  # only trim if > 500ms
            duration = original_duration - trim_duration
            
        # Ensure duration is not negative
        duration = max(duration, 0)

        # Save locally if enabled
        local_path = None
        if should_save_locally():
            local_path = user_session['preparator'].save_audio(
                pcm_data, 
                sample_rate, 
                filename,
                bits_per_sample,
                channels,
                already_processed  # Pass the parameter correctly
            )

        language = user_session['preparator'].language
        
        # Get transcription text from the current transcript object
        transcription = str(current_transcript['text']).strip()
        
        try:
            import unicodedata
            transcription = unicodedata.normalize('NFC', transcription)
        except Exception as e:
            logger.warning(f"Unicode normalization failed: {e}")

        # Get domain and subdomain from the preparator object where they were stored during session setup
        domain = user_session['preparator'].domain
        subdomain = user_session['preparator'].subdomain

        # Create metadata without transcription field
        metadata = {
            'user_id': user_id,
            'audio_filename': filename,
            'transcription_id': current_transcript.get('id'),
            'speaker_name': user_session['preparator'].speaker_name.strip(),
            'speaker_id': f"spk_{user_id}",
            'audio_path': f"{language}/audio/{filename}",
            'sampling_rate': sample_rate,
            'duration': duration,
            'language': language,
            'gender': user_session['preparator'].gender,
            'country': user_session['preparator'].country.strip(),
            'state': user_session['preparator'].state.strip(),
            'city': user_session['preparator'].city.strip(),
            'verified': False,
            'username': username.strip(),
            'age_group': user_session['preparator'].age_group,
            'accent': user_session['preparator'].accent,
            'domain': domain,
            'subdomain': subdomain
        }

        # Store metadata in PostgreSQL
        try:
            store_metadata(metadata)
            logger.info(f"Stored metadata for recording: {filename}")
        except Exception as db_error:
            logger.error(f"Database error storing metadata: {str(db_error)}")
            return jsonify({'error': f'Database error: {str(db_error)}'}), 500

        # After storing metadata, mark the transcription as recorded
        try:
            with engine.connect() as conn:
                update_query = text(f"""
                    UPDATE transcriptions_{language}
                    SET recorded = true
                    WHERE transcription_id = :transcription_id
                """)
                conn.execute(update_query, {"transcription_id": current_transcript.get('id')})
                conn.commit()
        except Exception as db_error:
            logger.error(f"Error marking transcription as recorded: {str(db_error)}")
            # Continue execution - the recording is still saved

        storage_locations = []

        if should_save_locally():
            user_session['preparator'].add_metadata(metadata)
            storage_locations.append('local')

        response_data = {
            'status': 'success',
            'metadata': metadata,
            'storage': storage_locations
        }

        # Handle next transcript - now using the lazy loader
        next_transcript = transcript_loader.move_next()
        if next_transcript:
            progress = transcript_loader.get_progress()
            response_data['next_transcript'] = {
                'text': next_transcript['text'],
                'current': progress['current'],
                'total': progress['total'],
                'previously_recorded': next_transcript.get('recorded', False)
            }
        else:
            # We're at the end
            response_data['session_complete'] = True

        return jsonify(response_data)

    except Exception as e:
        logger.error(f"Save recording error: {str(e)}")
        logger.error(format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                             'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.errorhandler(500)
def handle_500_error(e):
    logger.error(f"Internal server error: {str(e)}")
    logger.error(format_exc())
    return jsonify({
        'error': 'Internal server error',
        'details': str(e)
    }), 500

@app.route('/languages')
def get_languages():
    """Get list of supported languages that have available transcriptions"""
    try:
        # Get languages that have transcriptions in the database
        available_languages = set(get_available_languages())  # Convert to set for faster lookup
        logger.debug(f"Available languages from DB: {available_languages}")
        
        # Import languages from config here to ensure it's available
        from language_config import LANGUAGES
        
        # Filter the language config to only show available languages
        languages = [
            {'code': code, **lang} 
            for code, lang in LANGUAGES.items() 
            if code in available_languages
        ]
        
        logger.debug(f"Filtered languages: {languages}")
        
        if not languages:
            logger.warning("No languages found with available transcriptions")
            return jsonify({
                'status': 'warning',
                'message': 'No languages available',
                'languages': []
            })

        return jsonify({
            'status': 'success',
            'languages': languages
        })
    except Exception as e:
        logger.error(f"Error getting languages: {str(e)}")
        return jsonify({
            'error': 'Failed to fetch languages',
            'details': str(e)
        }), 500

@app.route('/domains')
def get_domain_list():
    """Get list of available domains"""
    try:
        # Get domains actually in use from database
        available_domain_codes = get_available_domains()
        
        # Get all defined domains from database
        all_domains = get_all_domains()
        
        # Filter domains to only those in use
        filtered_domains = {code: name for code, name in all_domains.items() if code in available_domain_codes}
        
        return jsonify({
            'status': 'success',
            'domains': filtered_domains
        })
    except Exception as e:
        logger.error(f"Error getting domains: {str(e)}")
        # Return empty domains as fallback instead of GEN
        return jsonify({
            'status': 'error',
            'message': 'Failed to load domains',
            'domains': {}
        })

@app.route('/domains/<domain_code>/subdomains')
def get_subdomain_list(domain_code):
    """Get list of subdomains for a domain"""
    try:
        # Get subdomains actually in use from database
        available_subdomain_codes = get_available_subdomains(domain_code)
        
        # Get all defined subdomains for this domain from database
        all_subdomains = get_domain_subdomains(domain_code)
        
        # Filter subdomains to only those in use
        filtered_subdomains = [s for s in all_subdomains if s['mnemonic'] in available_subdomain_codes]
        
        return jsonify({
            'status': 'success',
            'subdomains': filtered_subdomains
        })
    except Exception as e:
        logger.error(f"Error getting subdomains: {str(e)}")
        # Return empty subdomains as fallback instead of GEN
        return jsonify({
            'status': 'error',
            'message': 'Failed to load subdomains',
            'subdomains': []
        })

@app.route('/privacy')
def privacy():
    return render_template('privacy.html')

@app.route('/docs')
def docs():
    return render_template('docs.html')

@app.route('/refresh_session', methods=['POST'])
@login_required
def refresh_session_route():
    # Update or set any session parameters here
    session['refreshed_at'] = time.time()
    return jsonify({'message': 'Session refreshed'})

@app.route('/token/refresh')
def token_refresh():
    """Refresh the access token using the refresh token"""
    next_url = request.args.get('next', '/')
    refresh_token = request.cookies.get('refresh_token') or session.get('refresh_token')
    
    if not refresh_token:
        # No refresh token - must login again
        return redirect(url_for('login'))
    
    # Validate refresh token
    payload, error = validate_token(refresh_token)
    if error or payload.get('token_type') != 'refresh':
        # Invalid or wrong token type
        session.clear()
        response = make_response(redirect(url_for('login')))
        response.set_cookie('refresh_token', '', expires=0)
        response.set_cookie('access_token', '', expires=0)
        return response
    
    # Get user
    try:
        user_id = payload.get('user_id')
        user_data = session.get('user', {})
        
        # If user data is missing from session, try to fetch from PocketBase
        if not user_data or user_data.get('id') != user_id:
            user = app.pb.collection('users').get_one(user_id)
            user_data = {
                'id': user_id,
                'email': getattr(user, 'email', ''),
                'name': getattr(user, 'name', ''),
                'role': getattr(user, 'role', 'user').lower(),
                'is_moderator': getattr(user, 'role', '').lower() in ['moderator', 'admin'],
            }
            session['user'] = user_data
        
        # Create new access token
        access_token = create_access_token(user_data)
        session['access_token'] = access_token
        
        # Return to requested page with new token
        response = make_response(redirect(next_url))
        response.set_cookie(
            'access_token',
            access_token,
            max_age=3600,  # 1 hour
            httponly=True, 
            secure=True,
            samesite='Lax',
            path='/'
        )
        return response
        
    except Exception as e:
        logger.error(f"Token refresh error: {str(e)}")
        session.clear()
        return redirect(url_for('login'))

if __name__ == "__main__":
    try:
        # Don't initialize scheduler again, as it's already attached to the app
        # Just use the port from environment
        port = int(os.getenv('FLASK_PORT', 5000))
        from waitress import serve
        serve(app, host="0.0.0.0", port=port)
        
    except Exception as e:
        logger.error(f"Startup error: {e}")
        raise
    finally:
        if hasattr(app, 'scheduler'):
            app.scheduler.shutdown()
            logger.info("Scheduler shutdown successfully")

# Update file handling
def safe_filename(filename):
    """Generate safe filename"""
    return secure_filename(filename.replace(' ', '_'))

# Update save_audio function to use safe path joining
def save_audio(audio_data, path):
    try:
        safe_path = safe_join(app.config['TEMP_FOLDER'], path)
        if not safe_path:
            raise ValueError("Invalid path")
        # ...rest of save logic...
    except Exception as e:
        logger.error(f"Error saving audio: {e}")
        raise
