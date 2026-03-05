from flask import Blueprint, render_template, request, jsonify, current_app, session, redirect, url_for
import os
import logging
from functools import wraps
from datetime import datetime, timedelta
from collections import defaultdict
import time
import hashlib
import secrets
import random  # Add this import for random.random()

super_admin_bp = Blueprint('super_admin', __name__, url_prefix='/admin/super')
logger = logging.getLogger(__name__)

# Rate limiting implementation
MAX_PASSWORD_ATTEMPTS = 5
RATELIMIT_WINDOW = 300  # 5 minutes
VERIFICATION_TIMEOUT = 1800  # 30 minutes

password_attempts = defaultdict(list)
successful_verifications = {}

def is_rate_limited(ip):
    """Check if IP has exceeded password attempt rate limit"""
    now = time.time()
    window_start = now - RATELIMIT_WINDOW
    
    # Clear old attempts outside the window
    password_attempts[ip] = [t for t in password_attempts[ip] if t > window_start]
    
    # Check if attempts exceed the limit
    return len(password_attempts[ip]) >= MAX_PASSWORD_ATTEMPTS

def record_password_attempt(ip):
    """Record a password attempt for rate limiting"""
    password_attempts[ip].append(time.time())

def verify_password_secure(provided_password):
    """Securely verify the super admin password"""
    # Get the actual super admin password from environment
    actual_password = os.getenv('SUPER_ADMIN_PASSWORD')
    if not actual_password:
        logger.error("SUPER_ADMIN_PASSWORD not set in environment")
        return False
        
    # Use constant-time comparison to prevent timing attacks
    return secrets.compare_digest(provided_password, actual_password)

def admin_required(f):
    """Verify user is an admin before proceeding"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check if auth is disabled
        if not os.getenv('ENABLE_AUTH', 'true').lower() == 'true':
            return f(*args, **kwargs)
            
        if not session.get('user'):
            logger.warning("No user in session, unauthorized")
            return redirect(url_for('login'))
            
        # Verify admin role
        if session['user'].get('role') != 'admin':
            logger.warning(f"Non-admin user attempted to access super admin: {session['user'].get('email')}")
            return redirect(url_for('index'))
            
        return f(*args, **kwargs)
    return decorated_function

def super_admin_required(f):
    """Verify super admin password and admin role"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # First verify admin role
        if not session.get('user') or session['user'].get('role') != 'admin':
            logger.warning("Non-admin tried to use super admin functions")
            return jsonify({'error': 'Unauthorized access'}), 403
            
        # Get client IP for rate limiting
        client_ip = request.remote_addr
        
        # Check for valid verification session
        user_id = session['user'].get('id')
        verification_data = successful_verifications.get(user_id)
        
        if not verification_data or time.time() - verification_data['timestamp'] > VERIFICATION_TIMEOUT:
            # Verification expired or doesn't exist, require password
            provided_password = request.headers.get('X-Super-Admin-Password')
            
            if not provided_password:
                return jsonify({'error': 'Super admin password required', 'code': 'PASSWORD_REQUIRED'}), 401
                
            # Check rate limiting
            if is_rate_limited(client_ip):
                logger.warning(f"Rate limit exceeded for super admin password attempts from {client_ip}")
                return jsonify({
                    'error': 'Too many password attempts. Try again later.',
                    'code': 'RATE_LIMITED'
                }), 429
                
            # Verify password
            if not verify_password_secure(provided_password):
                # Record failed attempt for rate limiting
                record_password_attempt(client_ip)
                logger.warning(f"Invalid super admin password attempt from {client_ip}")
                return jsonify({'error': 'Invalid super admin password'}), 403
                
            # Password correct - create verification session
            session_token = secrets.token_hex(32)
            successful_verifications[user_id] = {
                'timestamp': time.time(),
                'token': session_token,
                'ip': client_ip
            }
            
        # Log this access
        logger.info(f"Super admin action by {session['user'].get('email')} ({user_id})")
        return f(*args, **kwargs)
    return decorated_function

@super_admin_bp.route('/')
@admin_required
def super_admin_interface():
    """Super admin interface - requires admin role but no password yet"""
    return render_template('super_admin.html')

@super_admin_bp.route('/verify', methods=['POST'])
@admin_required
def verify_password():
    """Verify super admin password"""
    data = request.get_json()
    password = data.get('password')
    
    if not password:
        return jsonify({'error': 'Password required'}), 400
        
    # Rate limiting check
    client_ip = request.remote_addr
    if is_rate_limited(client_ip):
        logger.warning(f"Rate limit exceeded for super admin password attempts from {client_ip}")
        return jsonify({
            'error': 'Too many password attempts. Try again later.',
            'code': 'RATE_LIMITED'
        }), 429
        
    # Verify password
    if verify_password_secure(password):
        # Password correct - create verification session
        user_id = session['user'].get('id')
        session_token = secrets.token_hex(32)
        successful_verifications[user_id] = {
            'timestamp': time.time(),
            'token': session_token,
            'ip': client_ip
        }
        
        # Log successful verification
        logger.info(f"Super admin access granted to {session['user'].get('email')} ({user_id})")
        return jsonify({
            'status': 'success',
            'expires_in': VERIFICATION_TIMEOUT
        })
    else:
        # Record failed attempt
        record_password_attempt(client_ip)
        logger.warning(f"Invalid super admin password from {session['user'].get('email')} ({client_ip})")
        return jsonify({'error': 'Invalid password'}), 403

@super_admin_bp.route('/admins')
@super_admin_required
def get_admins():
    """Get list of all admin users"""
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
                }
                for item in admins.items
            ]
        })
    except Exception as e:
        logger.error(f"Error fetching admins: {e}")
        return jsonify({'error': str(e)}), 500

@super_admin_bp.route('/users/search')
@super_admin_required
def search_user():
    """Search for a user by email"""
    try:
        email = request.args.get('email')
        if not email:
            return jsonify({'error': 'Email is required'}), 400

        pb = current_app.pb
        users = pb.collection('users').get_list(
            query_params={
                'filter': f'email = "{email}"',
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

@super_admin_bp.route('/users/<user_id>/role', methods=['POST'])
@super_admin_required
def update_user_role(user_id):
    """Update a user's role - with protections"""
    try:
        pb = current_app.pb
        data = request.get_json()
        new_role = data.get('role')
        admin_user = session['user']
        
        # Validate role
        if new_role not in ['user', 'moderator', 'admin']:
            return jsonify({'error': 'Invalid role'}), 400
            
        # Prevent self-modification
        if user_id == admin_user['id']:
            logger.warning(f"Attempt to modify own role by {admin_user['email']}")
            return jsonify({
                'error': 'Cannot modify your own role',
                'code': 'SELF_MODIFY_DENIED'
            }), 403
            
        # Get the target user
        try:
            user = pb.collection('users').get_one(user_id)
        except Exception as e:
            logger.error(f"Error fetching user {user_id}: {e}")
            return jsonify({'error': 'User not found'}), 404
            
        current_role = getattr(user, 'role', '')
        
        # SUPER_USER_EMAILS check is now optional - only apply if configured
        super_user_emails = os.getenv('SUPER_USER_EMAILS', '')
        if super_user_emails:
            protected_emails = [email.lower().strip() for email in super_user_emails.split(',') if email.strip()]
            if getattr(user, 'email', '').lower() in protected_emails:
                logger.warning(f"Attempt to modify protected super user {getattr(user, 'email', '')} by {admin_user['email']}")
                return jsonify({
                    'error': 'Cannot modify protected super user',
                    'code': 'PROTECTED_USER'
                }), 403
            
        # Update the role
        pb.collection('users').update(user_id, {'role': new_role})
        
        # Create audit log entry
        try:
            log_entry = {
                'action': 'role_change',
                'admin_id': admin_user['id'],
                'admin_email': admin_user['email'],
                'target_user_id': user_id,
                'target_user_email': getattr(user, 'email', ''),
                'old_role': current_role,
                'new_role': new_role,
                'timestamp': datetime.now().isoformat(),
                'ip_address': request.remote_addr
            }
            
            # Store in audit_logs collection if exists, otherwise log to file
            try:
                pb.collection('audit_logs').create(log_entry)
            except Exception:
                logger.info(f"AUDIT: {log_entry}")
        except Exception as log_error:
            logger.error(f"Failed to create audit log: {log_error}")
        
        return jsonify({
            'status': 'success',
            'message': f"Changed role of {getattr(user, 'email', '')} from {current_role} to {new_role}"
        })
    except Exception as e:
        logger.error(f"Error updating user role: {e}")
        return jsonify({'error': str(e)}), 500

def clean_expired_verifications():
    """Clean expired verification sessions"""
    try:
        now = time.time()
        expired_keys = []
        for user_id, data in successful_verifications.items():
            if now - data['timestamp'] > VERIFICATION_TIMEOUT:
                expired_keys.append(user_id)
        
        for key in expired_keys:
            successful_verifications.pop(key, None)
            
    except Exception as e:
        logger.error(f"Error cleaning expired verifications: {e}")

def init_cleanup(app):
    """Initialize the verification cleanup system"""
    try:
        if hasattr(app, 'scheduler'):
            app.scheduler.add_job(
                clean_expired_verifications,
                'interval',
                minutes=60,
                id='clean_super_admin_verifications',
                replace_existing=True
            )
            logger.info("Scheduled verification cleanup job")
        else:
            logger.warning("No scheduler available, using request-based cleanup")
            
            # Add cleanup function directly to the app's before_request handlers
            def cleanup_on_request():
                # Run cleanup occasionally (1% chance per request)
                if random.random() < 0.01:
                    clean_expired_verifications()
            
            app.before_request(cleanup_on_request)
            logger.info("Added request-based cleanup handler")
            
    except Exception as e:
        logger.error(f"Failed to setup verification cleanup: {e}")
