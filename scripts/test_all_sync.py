#!/usr/bin/env python3
"""
Complete test script for all incremental sync functions
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.pipeline import sync_contacts, sync_accounts, sync_intern_roles, get_sync_tracker
from database.models import Session, Contact, Account, InternRole
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_all_sync_functions():
    """Test all three sync functions with incremental sync"""
    
    print("=== Testing All Incremental Sync Functions ===")
    
    # 1. Test contacts sync
    print("\n1. Testing contacts incremental sync...")
    try:
        sync_contacts(incremental=True)
        tracker = get_sync_tracker('contacts')
        print(f"   ✓ Contacts sync successful. Tracker: {tracker.records_synced} records, last sync: {tracker.last_sync_timestamp}")
    except Exception as e:
        print(f"   ✗ Contacts sync failed: {e}")
        return False
    
    # 2. Test accounts sync
    print("\n2. Testing accounts incremental sync...")
    try:
        sync_accounts(incremental=True)
        tracker = get_sync_tracker('accounts')
        if tracker:
            print(f"   ✓ Accounts sync successful. Tracker: {tracker.records_synced} records, last sync: {tracker.last_sync_timestamp}")
        else:
            print("   ✓ Accounts sync successful (no new records)")
    except Exception as e:
        print(f"   ✗ Accounts sync failed: {e}")
        return False
    
    # 3. Test intern roles sync
    print("\n3. Testing intern roles incremental sync...")
    try:
        sync_intern_roles(incremental=True)
        tracker = get_sync_tracker('intern_roles')
        if tracker:
            print(f"   ✓ Intern roles sync successful. Tracker: {tracker.records_synced} records, last sync: {tracker.last_sync_timestamp}")
        else:
            print("   ✓ Intern roles sync successful (no new records)")
    except Exception as e:
        print(f"   ✗ Intern roles sync failed: {e}")
        return False
    
    # 4. Check database counts
    print("\n4. Checking database record counts...")
    session = Session()
    try:
        contact_count = session.query(Contact).count()
        account_count = session.query(Account).count()
        intern_role_count = session.query(InternRole).count()
        
        print(f"   Total records in database:")
        print(f"   - Contacts: {contact_count}")
        print(f"   - Accounts: {account_count}")
        print(f"   - Intern Roles: {intern_role_count}")
        
    finally:
        session.close()
    
    print("\n=== All sync functions tested successfully! ===")
    return True

if __name__ == "__main__":
    success = test_all_sync_functions()
    sys.exit(0 if success else 1)
