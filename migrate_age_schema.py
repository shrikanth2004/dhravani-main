from dotenv import load_dotenv
load_dotenv()
import os
from sqlalchemy import create_engine, text, inspect
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("POSTGRES_URL")
engine = create_engine(DATABASE_URL)

def migrate_age_schema():
    """Migrate all recordings_* tables: ADD age INTEGER, DROP age_group"""
    inspector = inspect(engine)
    
    # Find all recordings tables
    tables = inspector.get_table_names()
    recordings_tables = [t for t in tables if t.startswith('recordings_')]
    
    logger.info(f"Found {len(recordings_tables)} recordings tables: {recordings_tables}")
    
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        
        for table_name in recordings_tables:
            lang = table_name.replace('recordings_', '')
            logger.info(f"\n=== Processing {table_name} ===")
            
            # Check columns
            columns = inspector.get_columns(table_name)
            col_names = [col['name'] for col in columns]
            
            has_age = 'age' in col_names
            has_age_group = 'age_group' in col_names
            
            logger.info(f"Has age column: {has_age}, Has age_group: {has_age_group}")
            logger.info(f"All columns: {col_names[:10]}...")  # First 10
            
            if not has_age:
                logger.info("Adding age INTEGER column...")
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN age INTEGER"))
                logger.info("✓ age column added")
            else:
                # Check if age is INTEGER
                age_col = next(c for c in columns if c['name']=='age')
                if age_col['type'].__class__.__name__ != 'INTEGER':
                    logger.warning(f"age column exists but not INTEGER (is {age_col['type']})")
            
            if has_age_group:
                logger.info("Dropping age_group column...")
                conn.execute(text(f"ALTER TABLE {table_name} DROP COLUMN age_group"))
                logger.info("✓ age_group dropped")
            
            logger.info(f"✓ Completed {table_name}")
    
    logger.info("\n🎉 All migrations complete!")

if __name__ == "__main__":
    migrate_age_schema()
print("Migration script ready. Run: python migrate_age_schema.py")

