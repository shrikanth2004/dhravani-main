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
    table_exists,
    get_pending_recordings_for_assignment,
    get_all_pending_recordings,
    assign_recording_to_user,
    get_user_pending_assignments,
    get_all_user_assignments
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
        
        # Check if user has valid ID
        if not user.get('id'):
            logger.error("User session missing ID")
            return render_template('error.html', 
                                error_code=403,
                                error_message="Invalid session. Please login again."), 403
        
        # Update user role from PocketBase only when accessing validation interface
        try:
            if hasattr(current_app, 'pb') and current_app.pb:
                pb_user = current_app.pb.collection('users').get_one(user['id'])
                # Update session with latest role
                user['role'] = getattr(pb_user, 'role', user.get('role', ''))
                user['is_moderator'] = user['role'] in ['moderator', 'admin']
                session['user'] = user
        except Exception as e:
            logger.error(f"Error updating user role: {e}")
            # Continue with existing session data if PocketBase update fails
            pass

        # Check if user has access:
        # - Admin/Moderator: Full access to validate any recording
        # - Regular user: Can access but will only see assigned recordings
        is_moderator = user.get('is_moderator', False)

        languages = get_all_languages()
        return render_template('validate_new.html', languages=languages)
        
    except Exception as e:
        logger.error(f"Error in validate route: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return render_template('error.html',
                             error_code=500, 
                             error_message=f"An internal server error occurred: {str(e)}"), 500

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
                    mother_tongue VARCHAR
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
        # Check if mother_tongue column exist in recordings table
        mother_tongue_exists = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = 'recordings_{language}' 
                AND column_name = 'mother_tongue'
            )
        """)).scalar()
        
        if not mother_tongue_exists:
            # Add mother_tongue column if they don't exist
            conn.execute(text(f"""
                ALTER TABLE recordings_{language}
                ADD COLUMN mother_tongue VARCHAR
            """))
            conn.commit()
            logger.info(f"Added mother_tongue column to recordings_{language}")

@validation.route('/api/recordings', methods=['GET'])
@login_required
def get_recordings():
    user = session.get('user', {})
    is_moderator = user.get('is_moderator', False)
    user_id = user.get('id')
    
    # For regular users, only return their assigned recordings
    # For moderators/admins, return all recordings
    page = request.args.get('page', 1, type=int)
    language = request.args.get('language', '')
    status = request.args.get('status', 'all')
    mother_tongue = request.args.get('mother_tongue', '')
    
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
        
        if mother_tongue:
            where_conditions.append("r.mother_tongue = :mother_tongue")
            count_conditions.append("mother_tongue = :mother_tongue")
            
        where_clause = f" WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        count_where = f"{' AND '.join(count_conditions)}" if count_conditions else "true"
            
        query += where_clause + " ORDER BY r.id LIMIT 10 OFFSET :offset"
        
        with engine.connect() as conn:
            if language:
                ensure_language_tables(conn, language)
                
                full_query = text(query.format(language, language))
                params = {"offset": offset}
                
                if mother_tongue:
                    params["mother_tongue"] = mother_tongue
                
                result = conn.execute(full_query, params)
                
                count_params = {}
                if mother_tongue:
                    count_params["mother_tongue"] = mother_tongue
                
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
                    if mother_tongue:
                        params["mother_tongue"] = mother_tongue
                        
                    recordings = conn.execute(language_query, params).fetchall()
                    
                    count_params = {}
                    if mother_tongue:
                        count_params["mother_tongue"] = mother_tongue
                        
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
            'mother_tongue': mother_tongue
        })
        
    except Exception as e:
        logger.error(f"Error getting recordings: {str(e)}")
        return jsonify({'error': str(e)}), 500

@validation.route('/api/verify/<path:recording_id>', methods=['POST'])
@login_required
def verify_recording(recording_id):
    user = session.get('user', {})
    is_moderator = user.get('is_moderator', False)
    user_id = user.get('id')

    # Allow both moderators and regular users with assigned recordings
    # Regular users can only verify recordings assigned to them
    if not is_moderator:
        # Verify the recording is assigned to this user
        assignments = get_user_pending_assignments(user_id)
        if not assignments or len(assignments) == 0:
            return jsonify({'error': 'Unauthorized - No recordings assigned to you'}), 403

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
    user = session.get('user', {})
    is_moderator = user.get('is_moderator', False)
    user_id = user.get('id')
    
    language = request.args.get('language', '')
    mother_tongue = request.args.get('mother_tongue', '')

    if not language:
        return jsonify({'error': 'Language is required'}), 400

    # Check if user has any assignments for this language
    user_id = session['user']['id']
    assignments = get_user_pending_assignments(user_id)
    
    # FOR NON-MODERATORS: Only show assigned recordings
    if not is_moderator:
        # First check if user has any assignments at all
        if not assignments or len(assignments) == 0:
            return jsonify({'error': 'No recordings assigned to you. Please contact admin for assignment.'}), 403
            
        # Filter assignments by language
        lang_assignments = [a for a in assignments if a.get('language') == language]
        
        if not lang_assignments:
            return jsonify({
                'status': 'no_recordings',
                'message': f'No recordings assigned to you for language: {language}'
            }), 200
            
        # Return the first assigned recording
        recording = lang_assignments[0]
        
        return jsonify({
            'status': 'success',
            'recording': recording,
            'is_assigned': True
        })
    
    # For moderators/admins, show all pending recordings
    try:
        with engine.connect() as conn:
            if not table_exists(conn, f"recordings_{language}"):
                return jsonify({
                    'status': 'no_recordings',
                    'message': 'No recordings available for validation'
                })

        # First check if user has assigned recordings for this language
        user_id = session['user']['id']
        user_assignments = get_user_pending_assignments(user_id)
        
        # Filter assignments for the requested language
        assigned_recording = None
        for assignment in user_assignments:
            if assignment['language'] == language:
                assigned_recording = assignment
                break
        
        if assigned_recording:
            # Return the assigned recording first
            return jsonify({
                'status': 'success',
                'recording': assigned_recording,
                'is_assigned': True,
                'assigned_by': assigned_recording.get('assigned_by')
            })
        
        # If no assigned recording, fall back to the existing assignment logic
        recording = assign_recording(language, session['user']['id'], mother_tongue)
        
        if not recording:
            return jsonify({
                'status': 'no_recordings',
                'message': 'No recordings available for validation'
            })
            
        return jsonify({
            'status': 'success',
            'recording': dict(recording),
            'is_assigned': False
        })
        
    except Exception as e:
        logger.error(f"Error getting next recording: {str(e)}")
        return jsonify({
            'status': 'no_recordings',
            'message': 'No recordings available for validation'
        })

@validation.route('/api/pending_recordings', methods=['GET'])
@login_required
def get_pending_recordings_api():
    """Get pending recordings that can be assigned to users (admin only)"""
    user = session.get('user', {})
    
    # Debug: Log user info
    logger.info(f"Pending recordings API - User: {user.get('id')}, Role: {user.get('role')}")
    
    # Check if user is admin
    user_role = user.get('role', '')
    if user_role != 'admin':
        logger.warning(f"Non-admin user {user.get('id')} with role '{user_role}' tried to access pending recordings API")
        return jsonify({'error': 'Admin access required. Current role: ' + str(user_role)}), 403
    
    language = request.args.get('language', '')
    mother_tongue = request.args.get('mother_tongue', '')
    limit = request.args.get('limit', 50, type=int)
    
    logger.info(f"Loading pending recordings - Language: '{language}', Limit: {limit}")
    
    try:
        if language:
            recordings = get_pending_recordings_for_assignment(language, mother_tongue, limit)
            logger.info(f"Found {len(recordings)} recordings for language: {language}")
        else:
            # Pass None instead of empty string to get all recordings
            recordings = get_all_pending_recordings(None, limit)
            logger.info(f"Found {len(recordings)} recordings across all languages")
        
        return jsonify({
            'status': 'success',
            'recordings': recordings,
            'count': len(recordings)
        })
    except Exception as e:
        import traceback
        logger.error(f"Error getting pending recordings: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@validation.route('/api/assign', methods=['POST'])
@login_required
def assign_recording_api():
    """Assign a recording to a specific user (admin only)"""
    user = session.get('user', {})
    
    # Check if user is admin
    if user.get('role') != 'admin':
        return jsonify({'error': 'Admin access required'}), 403
    
    try:
        data = request.get_json()
        recording_id = data.get('recording_id')
        language = data.get('language')
        user_id = data.get('user_id')  # The user to assign to
        
        if not recording_id or not language or not user_id:
            return jsonify({'error': 'recording_id, language, and user_id are required'}), 400
        
        # Assign the recording
        result = assign_recording_to_user(
            recording_id=recording_id,
            language=language,
            user_id=user_id,
            assigned_by=user['id']
        )
        
        if result.get('success'):
            return jsonify({
                'status': 'success',
                'message': result.get('message')
            })
        else:
            return jsonify({
                'status': 'error',
                'error': result.get('error')
            }), 400
            
    except Exception as e:
        logger.error(f"Error assigning recording: {str(e)}")
        return jsonify({'error': str(e)}), 500

@validation.route('/api/users', methods=['GET'])
@login_required
def get_users_api():
    """Get list of all users who can be assigned recordings for validation"""
    user = session.get('user', {})
    
    # Check if user is admin
    if user.get('role') != 'admin':
        logger.warning(f"Non-admin user {user.get('id')} tried to access users API")
        return jsonify({'error': 'Admin access required'}), 403
    
    try:
        # Use the existing PocketBase client from the app (already authenticated)
        pb = current_app.pb
        
        if not pb:
            logger.error("PocketBase client not initialized")
            return jsonify({'error': 'PocketBase not initialized'}), 500
        
        users = []
        
        # Use get_list with fields parameter to get username, name, email, role
        # Include username as it's commonly used in PocketBase
        try:
            result = pb.collection('users').get_list(
                query_params={
                    'per_page': 200,
                    'sort': '-created',
                    'fields': 'id,username,name,email,role'
                }
            )
            
            logger.info(f"Users API: Got {result.total_items} users")
            
            for item in result.items:
                # Get username - prefer username, then name, then email
                display_name = getattr(item, 'username', '') or getattr(item, 'name', '') or getattr(item, 'email', '')
                users.append({
                    'id': item.id,
                    'username': display_name,
                    'email': getattr(item, 'email', ''),
                    'name': getattr(item, 'name', ''),
                    'role': getattr(item, 'role', 'user')
                })
                
        except Exception as pb_error:
            logger.error(f"PocketBase error fetching users: {pb_error}")
            return jsonify({'error': f'Failed to fetch users: {str(pb_error)}'}), 500
        
        logger.info(f"Final user count: {len(users)}")
        
        return jsonify({
            'status': 'success',
            'users': users
        })
    except Exception as e:
        logger.error(f"Error in get_users_api: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@validation.route('/api/my_assignments', methods=['GET'])
@login_required
def get_my_assignments_api():
    """Get current user's assigned recordings"""
    user = session.get('user', {})
    user_id = user.get('id')
    
    try:
        # Get pending assignments
        pending = get_user_pending_assignments(user_id)
        
        # Get all assignments (including completed)
        all_assignments = get_all_user_assignments(user_id)
        
        return jsonify({
            'status': 'success',
            'pending_assignments': pending,
            'all_assignments': all_assignments,
            'pending_count': len(pending)
        })
    except Exception as e:
        logger.error(f"Error getting assignments: {str(e)}")
        return jsonify({'error': str(e)}), 500

@validation.route('/api/unassign', methods=['POST'])
@login_required
def unassign_recording_api():
    """Unassign a recording from a user (admin only)"""
    user = session.get('user', {})
    
    # Check if user is admin
    if user.get('role') != 'admin':
        return jsonify({'error': 'Admin access required'}), 403
    
    try:
        data = request.get_json()
        recording_id = data.get('recording_id')
        language = data.get('language')
        
        if not recording_id or not language:
            return jsonify({'error': 'recording_id and language are required'}), 400
        
        with engine.connect() as conn:
            conn.execute(text("""
                DELETE FROM validation_assignments
                WHERE recording_id = :recording_id
                AND language = :language
                AND status = 'pending'
            """), {
                "recording_id": recording_id,
                "language": language
            })
            conn.commit()
        
        return jsonify({
            'status': 'success',
            'message': 'Recording unassigned successfully'
        })
    except Exception as e:
        logger.error(f"Error unassigning recording: {str(e)}")
        return jsonify({'error': str(e)}), 500
