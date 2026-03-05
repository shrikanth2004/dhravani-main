import secrets
from flask import request, session, abort
import os
import time
from functools import wraps
import logging  # Add this import

# Add a logger instance
logger = logging.getLogger(__name__)

def generate_csrf_token():
    """Generate a new CSRF token"""
    if 'csrf_token' not in session:
        session['csrf_token'] = secrets.token_hex(32)
        # Add timestamp to enable token expiration
        session['csrf_token_time'] = time.time()
    return session['csrf_token']

def validate_csrf_token(token):
    """Validate the CSRF token"""
    # Debugging
    session_token = session.get('csrf_token')
    
    if not token:
        logger.warning("CSRF validation failed: No token provided")
        return False
        
    if token != session_token:
        logger.warning(f"CSRF validation failed: Tokens don't match. Request token: {token[:10]}..., Session token: {session_token[:10]}..." if session_token else "None")
        return False
    
    return True

def csrf_protect(func):
    """Decorator to check CSRF token"""
    @wraps(func)
    def decorated_function(*args, **kwargs):
        # Only check POST/PUT/DELETE requests
        if request.method in ['POST', 'PUT', 'DELETE']:
            # Look for token in headers and form, with debugging
            header_token = request.headers.get('X-CSRF-Token')
            form_token = request.form.get('csrf_token')
            token = header_token or form_token
            
            logger.debug(f"CSRF check: Header token present: {header_token is not None}, Form token present: {form_token is not None}")
            
            if not validate_csrf_token(token):
                logger.warning(f"CSRF validation failed for {request.method} {request.path}")
                abort(403)  # Forbidden
        return func(*args, **kwargs)
    return decorated_function

def set_security_headers(response):
    """Set security headers for all responses"""
    # Get PocketBase URL from environment
    pocketbase_url = os.getenv('POCKETBASE_URL', '')
    
    # Prepare CSP directives
    csp_directives = [
        "default-src 'self'",
        # Allow scripts from unpkg.com (PocketBase) and CDNs
        "script-src 'self' 'unsafe-inline' https://unpkg.com https://cdn.jsdelivr.net https://cdnjs.cloudflare.com",
        # Allow styles from Google Fonts, Bootstrap, and other CDNs
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net https://cdnjs.cloudflare.com",
        # Allow font loading from Google Fonts and CDNs
        "font-src 'self' https://fonts.gstatic.com https://cdn.jsdelivr.net https://cdnjs.cloudflare.com",
        # Allow images from self and data URLs
        "img-src 'self' data:",
        # Allow connections to PocketBase and any other APIs
        f"connect-src 'self' {pocketbase_url}" if pocketbase_url else "connect-src 'self'"
    ]
    
    # Join directives with semicolons
    csp_header = "; ".join(csp_directives)
    
    # Set security headers
    response.headers['Content-Security-Policy'] = csp_header
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    
    # Add CSRF token to all responses
    if 'csrf_token' not in session:
        session['csrf_token'] = secrets.token_hex(32)
        session['csrf_token_time'] = time.time()
    
    return response
