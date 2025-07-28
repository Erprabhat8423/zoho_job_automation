import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.migrations import DatabaseManager
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_database_migrations():
    """Test the database migration system"""
    try:
        logger.info("Testing database migration system...")
        
        # Create database manager
        db_manager = DatabaseManager()
        
        # Test table existence check
        logger.info("Testing table existence check...")
        from database.models import Contact, Account, InternRole
        
        # Check if tables exist
        contact_exists = db_manager.table_exists('contacts')
        account_exists = db_manager.table_exists('accounts')
        intern_role_exists = db_manager.table_exists('intern_roles')
        
        logger.info(f"Contacts table exists: {contact_exists}")
        logger.info(f"Accounts table exists: {account_exists}")
        logger.info(f"Intern roles table exists: {intern_role_exists}")
        
        # Test schema sync
        logger.info("Testing schema synchronization...")
        db_manager.ensure_all_tables_exist()
        
        logger.info("Database migration test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Database migration test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_database_migrations()
    if success:
        print("Database migration test passed!")
    else:
        print("Database migration test failed!")
        sys.exit(1) 