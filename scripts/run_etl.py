import sys
import os
import logging
from datetime import datetime

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.pipeline import sync_contacts, sync_accounts, sync_intern_roles

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main ETL execution function"""
    start_time = datetime.now()
    logger.info("Starting ETL job...")
    
    try:
        # Run database migrations first
        logger.info("Step 1: Checking and updating database schema...")
        
        # Sync contacts
        logger.info("Step 2: Syncing contacts...")
        sync_contacts()
        logger.info("Contacts sync completed successfully")
        
        # Sync accounts
        logger.info("Step 3: Syncing accounts...")
        sync_accounts()
        logger.info("Accounts sync completed successfully")
        
        # Sync intern roles
        logger.info("Step 4: Syncing intern roles...")
        sync_intern_roles()
        logger.info("Intern roles sync completed successfully")
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"ETL job completed successfully in {duration}")
        print(f"ETL job completed successfully in {duration}")
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        print(f"ETL job failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
