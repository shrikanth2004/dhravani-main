from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify, current_app
from sqlalchemy import text
from database_manager import engine, get_dataset_stats, get_all_domains_db as get_all_domains, get_domain_subdomains_db as get_domain_subdomains
from dataset_sync import DatasetSynchronizer
import logging
from functools import wraps
from language_config import get_all_languages
import csv
from io import StringIO
from datetime import datetime
import os

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')
logger = logging.getLogger(__name__)

def admin_required(f):
    """Decorator to check for admin role"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('user'):
            logger.warning("No user in session, unauthorized")
            return redirect(url_for('index'))

        # Always verify with PocketBase for all admin routes
        pb = current_app.pb
        user_id = session['user'].get('id')
        try:
            pb_user = pb.collection('users').get_one(user_id)
            role = getattr(pb_user, 'role', '')
            # Update session with latest role
            session['user']['role'] = role
            # Mark session as modified to ensure changes are saved
            session.modified = True
            
            if role != 'admin':
                logger.warning(f"User {user_id} is not admin")
                return redirect(url_for('index'))
        except Exception as e:
            logger.error(f"Error verifying user role from PocketBase: {e}")
            return redirect(url_for('index'))
            
        return f(*args, **kwargs)
    return decorated_function

@admin_bp.route('/')
@admin_required
def admin_interface():
    try:
        # Get PocketBase client
        pb = current_app.pb
        
        # Get fresh user data directly from PocketBase
        user_id = session['user'].get('id')
        pb_user = pb.collection('users').get_one(user_id)
        
        languages = get_all_languages()
        stats = get_dataset_stats()
        
        # Add debug logging
        logger.debug(f"Language stats: {stats.get('languages', {})}")
        logger.debug(f"Total languages: {stats.get('total_languages', 0)}")
        
        verification_rate = (stats['total_verified'] / stats['total_recordings'] * 100) if stats['total_recordings'] > 0 else 0
        
        # Get domains from domain_subdomain.py
        domains = get_all_domains()
        
        # Pass the fresh user data from PocketBase instead of session data
        return render_template('admin.html', 
                             languages=languages,
                             stats=stats,
                             verification_rate=round(verification_rate, 1),
                             pb_user=pb_user,
                             domains=domains)
    except Exception as e:
        logger.error(f"Error in admin interface: {e}")
        empty_stats = {
            'total_recordings': 0,
            'total_verified': 0,
            'languages': {},
            'total_duration': 0,
            'total_users': 0,
            'total_languages': 0,
            'total_transcripts': 0
        }
        return render_template('admin.html', 
                             languages=get_all_languages(),
                             stats=empty_stats,
                             verification_rate=0,
                             domains=get_all_domains())



@admin_bp.route('/submit', methods=['POST'])
@admin_required
def submit_transcription():
    # Get PocketBase client and fetch fresh user data
    pb = current_app.pb
    user_id = session['user'].get('id')
    pb_user = pb.collection('users').get_one(user_id)
    
    language = request.form.get('language')
    transcription_text = request.form.get('transcription_text')
    file = request.files.get('fileInput')

    if not language:
        return jsonify({'error': 'Language is required'}), 400

    try:
        # Save or update the language CSV in the transcript folder
        transcript_dir = os.path.join(current_app.root_path, 'transcript')
        os.makedirs(transcript_dir, exist_ok=True)
        csv_path = os.path.join(transcript_dir, f"{language}_transcript.csv")
        
        # We always want to save the content (whether from file or text area) to the CSV
        content_to_save = ""
        if file and file.filename:
            content = file.read().decode('utf-8')
            content_to_save = content
            # Reset the file pointer if we need to parse it below (or just use content)
        elif transcription_text:
            content_to_save = transcription_text

        if content_to_save:
            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                f.write(content_to_save)
            logger.info(f"Saved transcript CSV to {csv_path}")

        # Create language-specific table if it doesn't exist
        with engine.connect() as conn:
            from database_manager import ensure_transcription_table
            ensure_transcription_table(conn, language)

            if content_to_save:
                # We can reuse the content string instead of file / text area branching
                lines = content_to_save.splitlines()
                # If it's a CSV, parse it with csv module, else just lines
                if (file and file.filename and file.filename.endswith('.csv')):
                    csv_reader = csv.reader(StringIO(content_to_save))
                    for row in csv_reader:
                        if row:  # Skip empty rows
                            query = text(f"""
                                INSERT INTO transcriptions_{language} 
                                (user_id, transcription_text, recorded)
                                VALUES (:user_id, :transcription_text, false)
                            """)
                            conn.execute(query, {
                                "user_id": user_id,
                                "transcription_text": row[0].strip()
                            })
                else:  # Treat as .txt or direct text input
                    for line in lines:
                        if line.strip():  # Skip empty lines
                            query = text(f"""
                                INSERT INTO transcriptions_{language} 
                                (user_id, transcription_text, recorded)
                                VALUES (:user_id, :transcription_text, false)
                            """)
                            conn.execute(query, {
                                "user_id": user_id,
                                "transcription_text": line.strip()
                            })
            else:
                return jsonify({'error': 'No content provided'}), 400

            conn.commit()
            return jsonify({'status': 'success'})

    except Exception as e:
        logger.error(f"Error inserting transcriptions: {e}")
        return jsonify({'error': str(e)}), 500

@admin_bp.route('/users/moderators')
@admin_required
def get_moderators():
    try:
        pb = current_app.pb
        moderators = pb.collection('users').get_list(
            query_params={
                'sort': '-created',
                'filter': 'role = "moderator"',
                'fields': 'id,email,name,role'
            }
        )
        return jsonify({
            'status': 'success',
            'users': [
                {
                    'id': item.id,
                    'email': getattr(item, 'email', ''),
                    'name': getattr(item, 'name', ''),
                    'role': 'moderator'
                }
                for item in moderators.items
            ]
        })
    except Exception as e:
        logger.error(f"Error fetching moderators: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@admin_bp.route('/users/search')
@admin_required
def search_user():
    try:
        email_list = request.args.get('email', '').strip()
        if not email_list:
            return jsonify({'error': 'Email is required'}), 400

        # Split emails by comma and clean them
        emails = [e.strip() for e in email_list.split(',') if e.strip()]
        if not emails:
            return jsonify({'error': 'No valid emails provided'}), 400

        pb = current_app.pb
        # Build filter query for multiple emails
        email_filters = ' || '.join([f'email ~ "{email.lower()}"' for email in emails])
        users = pb.collection('users').get_list(
            query_params={
                'filter': f'({email_filters})',
                'fields': 'id,email,name,role'
            }
        )
        
        return jsonify({
            'status': 'success',
            'users': [
                {
                    'id': item.id,
                    'email': getattr(item, 'email', ''),
                    'name': getattr(item, 'name', ''),
                    'role': getattr(item, 'role', 'user')
                }
                for item in users.items
            ]
        })
    except Exception as e:
        logger.error(f"Error searching users: {e}")
        return jsonify({'error': str(e)}), 500

@admin_bp.route('/users/<user_id>/role', methods=['POST'])
@admin_required
def update_user_role(user_id):
    try:
        logger.debug(f"Updating role for user {user_id}")
        
        # Get current admin user from PocketBase
        pb = current_app.pb
        admin_user = pb.collection('users').get_one(session['user']['id'])
        
        if user_id == admin_user.id:
            return jsonify({
                'error': 'Cannot modify your own role',
                'code': 'SELF_MODIFY_DENIED'
            }), 403

        data = request.get_json()
        if not data or 'role' not in data:
            return jsonify({'error': 'Role is required'}), 400
            
        new_role = data['role']
        if new_role not in ['user', 'moderator']:
            return jsonify({'error': 'Invalid role. Must be either "user" or "moderator"'}), 400

        try:
            # First check if target user exists and is not admin
            user = pb.collection('users').get_one(user_id)
            if getattr(user, 'role', '') == 'admin':
                return jsonify({'error': 'Cannot modify admin user roles'}), 403
                
            # Update the role
            pb.collection('users').update(user_id, {'role': new_role})
            logger.info(f"Successfully updated role for user {user_id} to {new_role}")
            return jsonify({'status': 'success'})
            
        except Exception as e:
            logger.error(f"Error updating role in PocketBase: {str(e)}")
            return jsonify({'error': 'User not found or database error'}), 404

    except Exception as e:
        logger.error(f"Error updating user role: {str(e)}")
        return jsonify({'error': str(e)}), 500

@admin_bp.route('/sync/status', methods=['GET'])
@admin_required
def sync_status():
    """Check if sync is in progress"""
    try:
        synchronizer = DatasetSynchronizer()
        return jsonify({
            'is_syncing': synchronizer.is_syncing(),
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error checking sync status: {e}")
        return jsonify({'error': str(e)}), 500

@admin_bp.route('/sync', methods=['POST'])
@admin_required
def trigger_sync():
    """Trigger dataset synchronization"""
    try:
        synchronizer = DatasetSynchronizer()
        
        # Check if sync is already running
        if synchronizer.is_syncing():
            return jsonify({
                'error': 'A sync operation is already in progress',
                'code': 'SYNC_IN_PROGRESS',
                'status': 'error'
            }), 409  # HTTP 409 Conflict
            
        # Start sync
        synchronizer.sync_dataset()
        
        return jsonify({
            'status': 'success',
            'message': 'Dataset synchronization completed successfully',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error during manual sync: {e}", exc_info=True)
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500
