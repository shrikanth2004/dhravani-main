import os
import sys
import logging

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import PocketBase directly
from pocketbase import PocketBase
from dotenv import load_dotenv

load_dotenv()
pb_url = os.getenv('POCKETBASE_URL')
if not pb_url:
    print("ERROR: POCKETBASE_URL not set")
    sys.exit(1)

def test_search():
    pb = PocketBase(pb_url)
    print(f"Testing PocketBase at {pb_url}")
    emails = ['shrikanth.22cs150@sode-edu.in']
    email_filters = ' || '.join([f'email ~ "{email}"' for email in emails])
    print(f"Filter: ({email_filters})")
    
    try:
        users = pb.collection('users').get_list(
            query_params={
                'filter': f'({email_filters})',
                'fields': 'id,email,name,role'
            }
        )
        print(f"Found {getattr(users, 'total_items', 0)} total, {len(users.items)} items:")
        for u in users.items:
            print(f"  - {getattr(u, 'email', 'N/A')} (role: {getattr(u, 'role', 'user')})")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == '__main__':
    test_search()
