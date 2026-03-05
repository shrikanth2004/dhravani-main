from flask import Blueprint, render_template, jsonify, send_from_directory, abort, session, request, redirect, url_for, current_app
from pathlib import Path
import pandas as pd
import logging
from functools import wraps
from language_config import get_all_languages
from filelock import FileLock  # Remove if not needed
import threading
from database_manager import (
    engine, 
    assign_recording,
    complete_assignment,
    table_exists  # Add this import
)
from sqlalchemy import text
import os
import shutil


logger = logging.getLogger(__name__)

# Update Blueprint with explicit url_prefix
validation = Blueprint('validation', __name__, url_prefix='/validation')

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('user'):
            return jsonify({'error': 'Authentication required', 'code': 'AUTH_REQUIRED'}), 401
        return f(*args, **kwargs)
    return decorated_function

@validation.route('/')
@login_required
def validate():
    try:
        # Get user from session
        user = session.get('user', {})
        
        # Update user role from PocketBase only when accessing validation interface
        try:
            pb_user = current_app.pb.collection('users').get_one(user['id'])
            # Update session with latest role
            user['role'] = getattr(pb_user, 'role', user.get('role', ''))
            user['is_moderator'] = user['role'] in ['moderator', 'admin']
            session['user'] = user
        except Exception as e:
            logger.error(f"Error updating user role: {e}")
            # Continue with existing session data if PocketBase update fails
            pass

        # Check moderator access with updated session data
        if not user.get('is_moderator', False):
            return render_template('error.html', 
                                error_code=403,
                                error_message="You don't have permission to access this page"), 403

        languages = get_all_languages()
        return render_template('validate.html', languages=languages)
        
    except Exception as e:
        logger.error(f"Error in validate route: {e}")
        return render_template('error.html',
                             error_code=500, 
                             error_message="An internal server error occurred"), 500

def ensure_language_tables(conn, language):
    """Ensure both recordings and transcriptions tables exist for the language"""
    # Check if tables exist
    tables_query = text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = :table_name
        )
    """)
    
    # Check recordings table
    recordings_exists = conn.execute(tables_query, 
        {"table_name": f"recordings_{language}"}).scalar()
    
    # Check transcriptions table
    transcriptions_exists = conn.execute(tables_query, 
        {"table_name": f"transcriptions_{language}"}).scalar()
    
    # Create missing tables
    if not recordings_exists or not transcriptions_exists:
        logger.info(f"Creating missing tables for language: {language}")
        
        if not recordings_exists:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS recordings_{language} (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR,
                    audio_filename VARCHAR,
                    transcription_id INTEGER,
                    speaker_name VARCHAR,
                    speaker_id VARCHAR,
                    audio_path VARCHAR,
                    sampling_rate INTEGER,
                    duration FLOAT,
                    language VARCHAR(2),
                    gender VARCHAR(10),
                    country VARCHAR,
                    state VARCHAR,
                    city VARCHAR,
                    status VARCHAR(20) DEFAULT 'pending',
                    verified_by VARCHAR,
                    username VARCHAR,
                    age_group VARCHAR,
                    accent VARCHAR,
                    domain VARCHAR(10),
                    subdomain VARCHAR(10)
                )
            """))
            
        if not transcriptions_exists:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS transcriptions_{language} (
                    transcription_id SERIAL PRIMARY KEY,
                    user_id VARCHAR(255),
                    transcription_text TEXT NOT NULL,
                    uploaded_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """))
        
        conn.commit()
    else:
        # Check if domain and subdomain columns exist in recordings table
        domain_exists = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = 'recordings_{language}' 
                AND column_name = 'domain'
            )
        """)).scalar()
        
        if not domain_exists:
            # Add domain and subdomain columns if they don't exist
            conn.execute(text(f"""
                ALTER TABLE recordings_{language}
                ADD COLUMN domain VARCHAR(10),
                ADD COLUMN subdomain VARCHAR(10)
            """))
            conn.commit()
            logger.info(f"Added domain and subdomain columns to recordings_{language}")

@validation.route('/api/recordings', methods=['GET'])
@login_required
def get_recordings():
    if not session.get('user', {}).get('is_moderator', False):
        return jsonify({'error': 'Unauthorized'}), 403
        
    page = request.args.get('page', 1, type=int)
    language = request.args.get('language', '')
    status = request.args.get('status', 'all')
    domain = request.args.get('domain', '')
    subdomain = request.args.get('subdomain', '')
    
    try:
        offset = (page - 1) * 10
        
        query = """
            SELECT r.*, COALESCE(t.transcription_text, '') as transcription
            FROM recordings_{} r
            LEFT JOIN transcriptions_{} t ON r.transcription_id = t.transcription_id
        """
        
        count_query = """
            SELECT COUNT(*) FROM recordings_{} WHERE {}
        """
        
        where_conditions = []
        count_conditions = []
        
        if status == 'verified':
            where_conditions.append("r.status = 'verified'")
            count_conditions.append("status = 'verified'")
        elif status == 'pending':
            where_conditions.append("r.status = 'pending'")
            count_conditions.append("status = 'pending'")
        elif status == 'rejected':
            where_conditions.append("r.status = 'rejected'")
            count_conditions.append("status = 'rejected'")
        
        if domain:
            where_conditions.append("r.domain = :domain")
            count_conditions.append("domain = :domain")
        
        if subdomain:
            where_conditions.append("r.subdomain = :subdomain")
            count_conditions.append("subdomain = :subdomain")
            
        where_clause = f" WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        count_where = f"{' AND '.join(count_conditions)}" if count_conditions else "true"
            
        query += where_clause + " ORDER BY r.id LIMIT 10 OFFSET :offset"
        
        with engine.connect() as conn:
            if language:
                ensure_language_tables(conn, language)
                
                full_query = text(query.format(language, language))
                params = {"offset": offset}
                
                if domain:
                    params["domain"] = domain
                if subdomain:
                    params["subdomain"] = subdomain
                
                result = conn.execute(full_query, params)
                
                count_params = {}
                if domain:
                    count_params["domain"] = domain
                if subdomain:
                    count_params["subdomain"] = subdomain
                
                total_result = conn.execute(text(
                    count_query.format(language, count_where)
                ), count_params)
                
                total = total_result.scalar()
            else:
                all_recordings = []
                total = 0
                
                tables_query = text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_name LIKE 'recordings_%'
                """)
                tables = conn.execute(tables_query).fetchall()
                
                for (table_name,) in tables:
                    language_code = table_name.replace('recordings_', '')
                    ensure_language_tables(conn, language_code)
                    language_query = text(query.format(language_code, language_code))
                    
                    params = {"offset": offset}
                    if domain:
                        params["domain"] = domain
                    if subdomain:
                        params["subdomain"] = subdomain
                        
                    recordings = conn.execute(language_query, params).fetchall()
                    
                    count_params = {}
                    if domain:
                        count_params["domain"] = domain
                    if subdomain:
                        count_params["subdomain"] = subdomain
                        
                    count_result = conn.execute(text(
                        count_query.format(language_code, count_where)
                    ), count_params)
                    
                    total += count_result.scalar() or 0
                
                all_recordings.sort(key=lambda x: x.id)
                result = all_recordings[offset:offset+10]
        
        recordings = []
        for row in result:
            recording_data = dict(row._mapping)
            if 'audio_path' not in recording_data or not recording_data['audio_path']:
                recording_data['audio_path'] = f"{recording_data.get('language', 'unknown')}/audio/{recording_data.get('audio_filename', '')}"
            recordings.append(recording_data)
        
        return jsonify({
            'recordings': recordings,
            'total': total,
            'domain': domain,
            'subdomain': subdomain
        })
        
    except Exception as e:
        logger.error(f"Error getting recordings: {str(e)}")
        return jsonify({'error': str(e)}), 500

@validation.route('/api/verify/<path:recording_id>', methods=['POST'])
@login_required
def verify_recording(recording_id):
    if not session.get('user', {}).get('is_moderator', False):
        return jsonify({'error': 'Unauthorized'}), 403

    try:
        data = request.get_json()
        verify = data.get('verify', False)
        
        parts = recording_id.split('/')
        if len(parts) < 3:
            return jsonify({'error': 'Invalid recording ID format'}), 400
            
        language = parts[0]
        filename = parts[-1]
        

        with engine.begin() as conn:
            result = conn.execute(text(f"""
                UPDATE recordings_{language}
                SET status = :status,
                    verified_by = :verified_by
                WHERE audio_filename = :filename
                RETURNING id
            """), {
                "status": 'verified' if verify else 'rejected',
                "verified_by": session['user']['id'],
                "filename": filename
            })
            
            rec_id = result.scalar()
            if not rec_id:
                return jsonify({'error': 'Recording not found'}), 404

            conn.execute(text("""
                UPDATE validation_assignments
                SET status = :status
                WHERE assigned_to = :user_id
                AND recording_id = :rec_id
                AND language = :language
            """), {
                "status": 'completed_verified' if verify else 'completed_rejected',
                "user_id": session['user']['id'],
                "rec_id": rec_id,
                "language": language
            })

        return jsonify({'status': 'success'})
        
    except Exception as e:
        logger.error(f"Error verifying recording: {str(e)}")
        return jsonify({'error': str(e)}), 500

@validation.route('/api/audio/<path:filename>')
@login_required
def serve_audio(filename):
    try:
        audio_path = Path('datasets') / filename
        if not audio_path.exists():
            raise FileNotFoundError(f"Audio file not found: {filename}")
        directory = str(audio_path.parent)
        file_name = audio_path.name
        return send_from_directory(directory, file_name, as_attachment=False)
    except Exception as e:
        logger.error(f"Error serving audio: {e}")
        abort(404)

@validation.route('/api/delete/<path:recording_id>', methods=['DELETE'])
@login_required
def delete_recording(recording_id):
    if not session.get('user', {}).get('is_moderator', False):
        return jsonify({'error': 'Unauthorized'}), 403
        
    try:
        parts = recording_id.split('/')
        if len(parts) < 3:
            return jsonify({'error': 'Invalid recording ID format'}), 400
            
        language = parts[0]
        filename = parts[-1]

        with engine.connect() as conn:
            verify_query = text(f"""
                SELECT status FROM recordings_{language}
                WHERE audio_filename = :filename
            """)
            result = conn.execute(verify_query, {"filename": filename}).first()
            
            if not result:
                return jsonify({'error': 'Recording not found'}), 404
                
            if result.status == 'verified':
                return jsonify({
                    'error': 'Cannot delete verified recording. Please unverify first.'
                }), 400

        with engine.begin() as conn:
            query = text(f"""
                SELECT * FROM recordings_{language}
                WHERE audio_filename = :filename
            """)
            recording = conn.execute(query, {"filename": filename}).first()
            
            if not recording:
                return jsonify({'error': 'Recording not found'}), 404

            delete_query = text(f"""
                DELETE FROM recordings_{language}
                WHERE audio_filename = :filename
            """)
            conn.execute(delete_query, {"filename": filename})

            audio_path = Path('datasets') / recording_id
            if audio_path.exists():
                try:
                    os.remove(audio_path)
                    audio_dir = audio_path.parent
                    if not any(audio_dir.iterdir()):
                        shutil.rmtree(audio_dir)
                except Exception as e:
                    logger.error(f"Error deleting audio file: {e}")

        return jsonify({'status': 'success', 'message': 'Recording deleted successfully'})
        
    except Exception as e:
        logger.error(f"Error deleting recording: {str(e)}")
        return jsonify({'error': str(e)}), 500

@validation.route('/api/next_recording', methods=['GET'])
@login_required
def get_next_recording():
    if not session.get('user', {}).get('is_moderator', False):
        return jsonify({'error': 'Unauthorized'}), 403
        
    language = request.args.get('language', '')
    domain = request.args.get('domain', '')
    subdomain = request.args.get('subdomain', '')
    
    if not language:
        return jsonify({'error': 'Language is required'}), 400
        
    try:
        with engine.connect() as conn:
            if not table_exists(conn, f"recordings_{language}"):
                return jsonify({
                    'status': 'no_recordings',
                    'message': 'No recordings available for validation'
                })

        # This now uses the imported assign_recording from database_manager.py
        # which properly handles domain/subdomain filtering
        recording = assign_recording(language, session['user']['id'], domain, subdomain)
        
        if not recording:
            return jsonify({
                'status': 'no_recordings',
                'message': 'No recordings available for validation'
            })
            
        return jsonify({
            'status': 'success',
            'recording': dict(recording)
        })
        
    except Exception as e:
        logger.error(f"Error getting next recording: {str(e)}")
        return jsonify({
            'status': 'no_recordings',
            'message': 'No recordings available for validation'
        })
