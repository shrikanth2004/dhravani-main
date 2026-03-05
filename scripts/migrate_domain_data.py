import sys
import os
import logging

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database_manager import engine, ensure_domain_tables
from domain_subdomain import domains_and_subdomains
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_domain_data():
    """Migrate domain and subdomain data from Python file to PostgreSQL."""
    try:
        with engine.connect() as conn:
            # Create the tables if they don't exist
            if not ensure_domain_tables(conn):
                logger.error("Failed to create domain tables")
                return False
            
            # Check if tables already have data
            domain_count = conn.execute(text("SELECT COUNT(*) FROM domains")).scalar()
            if domain_count > 0:
                user_input = input("Domain data already exists. Overwrite? (y/n): ")
                if user_input.lower() != 'y':
                    logger.info("Migration cancelled by user")
                    return False
                
                # Clear existing data
                conn.execute(text("DELETE FROM subdomains"))
                conn.execute(text("DELETE FROM domains"))
                conn.commit()
                logger.info("Existing domain data cleared")
            
            # Insert domains
            domains = domains_and_subdomains["domains"]
            for code, name in domains.items():
                conn.execute(text(
                    "INSERT INTO domains (code, name) VALUES (:code, :name)"
                ), {"code": code, "name": name})
                logger.info(f"Added domain: {code} - {name}")
            
            # Insert subdomains
            for domain_code, subdomains in domains_and_subdomains["subdomains"].items():
                for subdomain in subdomains:
                    conn.execute(text("""
                        INSERT INTO subdomains (mnemonic, name, domain_code) 
                        VALUES (:mnemonic, :name, :domain_code)
                    """), {
                        "mnemonic": subdomain["mnemonic"],
                        "name": subdomain["name"],
                        "domain_code": domain_code
                    })
                    logger.info(f"Added subdomain: {subdomain['mnemonic']} - {subdomain['name']} for domain {domain_code}")
            
            conn.commit()
            
            # Verify migration
            domain_count = conn.execute(text("SELECT COUNT(*) FROM domains")).scalar()
            subdomain_count = conn.execute(text("SELECT COUNT(*) FROM subdomains")).scalar()
            logger.info(f"Migration complete. Added {domain_count} domains and {subdomain_count} subdomains")
            
            return True
            
    except Exception as e:
        logger.error(f"Error migrating domain data: {str(e)}")
        return False

if __name__ == "__main__":
    success = migrate_domain_data()
    if success:
        print("Domain data migration completed successfully!")
    else:
        print("Domain data migration failed. See logs for details.")
        sys.exit(1)
