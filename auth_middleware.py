from functools import wraps
from flask import session, redirect, url_for, request, jsonify, make_response, current_app
from pocketbase import PocketBase
import logging
import os
import time
import jwt
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)

RATE_LIMIT = 100  # requests per minute
rate_limit_data = defaultdict(list)

# Add this to track token refresh operations
refresh_attempts = defaultdict(int)

def is_rate_limited(ip):
    now = time.time()
    minute_ago = now - 60
    
    # Clean old entries
    rate_limit_data[ip] = [t for t in rate_limit_data[ip] if t > minute_ago]
    
    # Check limit
    if len(rate_limit_data[ip]) >= RATE_LIMIT:
        return True
        
    rate_limit_data[ip].append(now)
    return False

def rate_limit_middleware():
    ip = request.remote_addr
    if is_rate_limited(ip):
        return jsonify({'error': 'Rate limit exceeded'}), 429

def create_access_token(user_data, expires_delta=timedelta(minutes=60)):
    """Create a new access token"""
    payload = {
        'user_id': user_data.get('id'),
        'email': user_data.get('email', ''),
        'role': user_data.get('role', 'user'),
        'exp': datetime.utcnow() + expires_delta
    }
    secret = current_app.config.get('JWT_SECRET_KEY') or current_app.secret_key
    return jwt.encode(payload, secret, algorithm='HS256')

def create_refresh_token(user_data, expires_delta=timedelta(days=30)):
    """Create a new refresh token"""
    payload = {
        'user_id': user_data.get('id'),
        'token_type': 'refresh',
        'exp': datetime.utcnow() + expires_delta
    }
    secret = current_app.config.get('JWT_SECRET_KEY') or current_app.secret_key
    return jwt.encode(payload, secret, algorithm='HS256')

def validate_token(token):
    """Validate JWT token"""
    try:
        secret = current_app.config.get('JWT_SECRET_KEY') or current_app.secret_key
        payload = jwt.decode(token, secret, algorithms=['HS256'])
        return payload, None
    except jwt.ExpiredSignatureError:
        return None, "Token has expired"
    except jwt.InvalidTokenError:
        return None, "Invalid token"

def init_auth(app):
    if not os.getenv('ENABLE_AUTH', 'true').lower() == 'true':
        logger.info("Authentication disabled")
        return

    # Set JWT secret key
    app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY', app.secret_key)
    
    # Initialize PocketBase client
    pb = PocketBase(os.getenv('POCKETBASE_URL'))
    app.pb = pb

    @app.before_request
    def before_request():
        # Skip check if auth is disabled
        if not os.getenv('ENABLE_AUTH', 'true').lower() == 'true':
            return
        
        # Skip auth for these paths
        if request.endpoint in ['static', 'login', 'privacy', 'auth_callback', 'token_refresh', 'favicon', 'docs']:
            return

        # Check for access token in session
        access_token = session.get('access_token')
        
        if not access_token:
            # If no access token, redirect to login
            if request.is_json:
                return jsonify({'error': 'Authentication required', 'code': 'AUTH_REQUIRED'}), 401
            return redirect(url_for('login'))

        # Validate the access token
        payload, error = validate_token(access_token)
        
        if error:
            # If token is expired but we have a refresh token, try refresh
            if error == "Token has expired" and session.get('refresh_token'):
                try:
                    # Prevent excessive refresh attempts
                    ip = request.remote_addr
                    if refresh_attempts[ip] > 5:  # Max 5 refresh attempts per minute
                        session.clear()
                        refresh_attempts[ip] = 0
                        return redirect(url_for('login'))
                        
                    refresh_attempts[ip] += 1
                    
                    # Try to refresh token
                    return redirect(url_for('token_refresh', next=request.path))
                except Exception as e:
                    logger.error(f"Token refresh error: {str(e)}")
                    session.clear()
                    return redirect(url_for('login'))
            else:
                # Invalid or expired token with no refresh token
                session.clear()
                return redirect(url_for('login'))
        
        # Restore PocketBase auth if token exists in session
        pb_token = session.get('user', {}).get('token')
        if pb_token:
            try:
                # Restore PocketBase authentication state
                app.pb.auth_store.save(pb_token, None)
                logger.debug("Restored PocketBase authentication from session")
            except Exception as e:
                logger.warning(f"Failed to restore PocketBase auth: {e}")
                # Continue - we'll handle PB errors in the routes
        
        # Set user data from validated token
        if not session.get('user') or session['user'].get('id') != payload['user_id']:
            try:
                # Try to fetch user from PocketBase to get latest role
                user = app.pb.collection('users').get_one(payload['user_id'])
                
                # Get the role from PocketBase (could be admin, moderator, or user)
                user_role = getattr(user, 'role', payload.get('role', 'user'))
                
                # Store minimal user data in session
                session['user'] = {
                    'id': payload['user_id'],
                    'email': payload['email'],
                    'role': user_role,
                    'is_moderator': user_role in ['moderator', 'admin'],
                }
            except Exception as e:
                logger.error(f"Error fetching user: {str(e)}")
                # Use token data if PocketBase is unavailable
                session['user'] = {
                    'id': payload['user_id'],
                    'email': payload['email'],
                    'role': payload.get('role', 'user'),
                }
        else:
            # Refresh role from PocketBase to get latest updates
            try:
                user = app.pb.collection('users').get_one(payload['user_id'])
                user_role = getattr(user, 'role', session['user'].get('role', 'user'))
                session['user']['role'] = user_role
                session['user']['is_moderator'] = user_role in ['moderator', 'admin']
                session.modified = True
            except Exception as e:
                logger.debug(f"Could not refresh user role: {e}")

        # Always ensure session is permanent
        session.permanent = True
        
        # Clean up old refresh attempts periodically
        now = time.time()
        if now % 60 < 1:  # Roughly once a minute
            old_ips = [ip for ip, count in refresh_attempts.items() if count > 0]
            for ip in old_ips:
                refresh_attempts[ip] = 0

