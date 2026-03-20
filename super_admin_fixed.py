from flask import Blueprint, render_template, request, jsonify, current_app, session, redirect, url_for
import os
import logging
from functools import wraps
from datetime import datetime, timedelta
from collections import defaultdict
import time
import hashlib
import secrets
import random

super_admin_bp = Blueprint('super_admin', __name__, url_prefix='/admin/super')
logger = logging.getLogger(__name__)

MAX_PASSWORD_ATTEMPTS = 5
RATELIMIT_WINDOW = 300
VERIFICATION_TIMEOUT = 1800

password_attempts = defaultdict(list)
successful_verifications = {}

def is_rate_limited(ip):
    now = time.time()
    window_start = now - RATELIMIT_WINDOW
    password_attempts[ip] = [t for t in password_attempts[ip] if t > window_start]
    return len(password_attempts[ip]) >= MAX_PASSWORD_ATTEMPTS

def record_password_attempt(ip):
    password_attempts[ip].append(time.time())

def verify_password_secure(provided_password):
    actual_password = os.getenv('SUPER_ADMIN_PASSWORD')
    if not actual_password:
        logger.error("SUPER_ADMIN_PASSWORD not set in environment")
        return False
    return secrets.compare_digest(provided_password, actual_password)

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not os.getenv('ENABLE_AUTH', 'true').lower() == 'true':
            return f(*args, **kwargs)
        if not session.get('user'):
            logger.warning("No user in session, unauthorized")
            return redirect(url_for('login'))
        if session['user'].get('role') != 'admin':
            logger.warning(f"Non-admin user attempted to access super admin: {session['user'].get('email')}")
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

def super_admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('user') or session['user'].get('role') != 'admin':
            logger.warning("Non-admin tried to use super admin functions")
            return jsonify({'error': 'Unauthorized access'}), 403
        client_ip = request.remote_addr
        user_id = session['user'].get('id')
        verification_data = successful_verifications.get(user_id)
        if not verification_data or time.time() - verification_data['timestamp'] > VERIFICATION_TIMEOUT:
            provided_password = request.headers.get('X-Super-Admin-Password')
            if not provided_password:
                return jsonify({'error': 'Super admin password required', 'code': 'PASSWORD_REQUIRED'}), 401
            if is_rate_limited(client_ip):
                logger.warning(f"Rate limit exceeded for super admin password attempts from {client_ip}")
                return jsonify({'error': 'Too many password attempts. Try again later.', 'code': 'RATE_LIMITED'}), 429
            if not verify_password_secure(provided_password):
                record_password_attempt(client_ip)
                logger.warning(f"Invalid super admin password attempt from {client_ip}")
                return jsonify({'error': 'Invalid super admin password'}), 403
            session_token = secrets.token_hex(32)
            successful_verifications[user_id] = {
                'timestamp': time.time(),
                'token': session_token,
                'ip': client_ip
            }
        logger.info(f"Super admin action by {session['user'].get('email')} ({user_id})")
        return f(*args, **kwargs)
    return decorated_function

@super_admin_bp.route('/')
@admin_required
def super_admin_interface():
    return render_template('super_admin.html')

@super_admin_bp.route('/verify', methods=['POST'])
@admin_required
def verify_password():
    data = request.get_json()
    password = data.get('password')
    if not password:
        return jsonify({'error': 'Password required'}), 400
    client_ip = request.remote_addr
    if is_rate_limited(client_ip):
        logger.warning(f"Rate limit exceeded for super admin password attempts from {client_ip}")
        return jsonify({'error': 'Too many password attempts. Try again later.', 'code': 'RATE_LIMITED'}), 429
    if verify_password_secure(password):
        user_id = session['user'].get('id')
        session_token = secrets.token_hex(32)
        successful_verifications[user_id] = {
            'timestamp': time.time(),
            'token': session_token,
            'ip': client_ip
        }
        logger.info(f"Super admin access granted to {session['user'].get('email')} ({user_id})")
        return jsonify({'status': 'success', 'expires_in': VERIFICATION_TIMEOUT})
    record_password_attempt(client_ip)
    logger.warning(f"Invalid super admin password from {session['user'].get('email')} ({client_ip})")
    return jsonify({'error': 'Invalid password'}), 403

@super_admin_bp.route('/admins')
@super_admin_required
def get_admins():
    try:
        pb = current_app.pb
        admins = pb.collection('users').get_list(
            query_params={
                'sort': '-created',
                'filter': 'role = "admin"',
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
                    'role': 'admin'
                } for item in admins.items
            ]
        })
    except Exception as e:
        logger.error(f"Error fetching admins: {e}")
        return jsonify({'error': str(e)}), 500

@super_admin_bp.route('/users/search')
@super_admin_required
def search_user():
    try:
        email_list = request.args.get('email', '').strip()
        logger.info(f"SUPER_ADMIN_SEARCH: Incoming='{email_list}'")
        if not email_list:
            return jsonify({'error': 'Email required'}), 400
        emails = [e.strip().lower() for e in email_list.split(',') if e.strip()]
        logger.info(f"SUPER_ADMIN_SEARCH: Emails={emails}")
        if not emails:
            return jsonify({'error': 'No valid emails'}), 400
        pb = current_app.pb
        # Simple substring search (works in PocketBase)
        email_filter = ' || '.join([f'email ~ "{email}"' for email in emails])
        logger.info(f"SUPER_ADMIN_SEARCH: Filter='{email_filter}'")
        # DEBUG sample
        sample = pb.collection('users').get_list(query_params={'perPage': '5', 'fields': 'email,id'})
        logger.info(f"SUPER_ADMIN_SEARCH DEBUG: Sample emails={[getattr(u, 'email', 'N/A') for u in sample.items]}")
        users = pb.collection('users').get_list(
            query_params={
                'filter': email_filter,
                'fields': 'id,email,name,role',
                'perPage': '50'
            }
        )
        logger.info(f"SUPER_ADMIN_SEARCH Result: {len(users.items)} users, total={getattr(users, 'total_items', 0)}")
        return jsonify({
            'status': 'success',
            'users': [
                {
                    'id': item.id,
                    'email': getattr(item, 'email', ''),
                    'name': getattr(item, 'name', ''),
                    'role': getattr(item, 'role', 'user')
                } for item in users.items
            ]
        })
    except Exception as e:
        logger.error(f"SUPER_ADMIN_SEARCH ERROR: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@super_admin_bp.route('/users/<user_id>/role', methods=['POST'])
@super_admin_required
def update_user_role(user_id):
    try:
        pb = current_app.pb
        data = request.get_json()
        new_role = data.get('role')
        if new_role not in ['user', 'moderator', 'admin']:
            return jsonify({'error': 'Invalid role'}), 400
        if user_id == session['user']['id']:
            return jsonify({'error': 'Cannot modify own role'}), 403
        user = pb.collection('users').get_one(user_id)
        current_role = getattr(user, 'role', '')
        pb.collection('users').update(user_id, {'role': new_role})
        logger.info(f"Role changed {getattr(user, 'email', user_id)} {current_role} → {new_role}")
        return jsonify({'status': 'success', 'message': f"Role changed to {new_role}"})
    except Exception as e:
        logger.error(f"Error updating role: {e}")
        return jsonify({'error': str(e)}), 500

def clean_expired_verifications():
    now = time.time()
    expired = [uid for uid, data in successful_verifications.items() if now - data['timestamp'] > VERIFICATION_TIMEOUT]
    for uid in expired:
        successful_verifications.pop(uid, None)

def init_cleanup(app):
    try:
        if hasattr(app, 'scheduler'):
            app.scheduler.add_job(clean_expired_verifications, 'interval', minutes=60, id='clean_super_admin_verifications', replace_existing=True)
            logger.info("Scheduled cleanup")
        else:
            logger.warning("No scheduler, skipping cleanup")
    except Exception as e:
        logger.error(f"Cleanup setup failed: {e}")
