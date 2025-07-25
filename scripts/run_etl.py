from etl.pipeline import sync_contacts, sync_accounts, sync_intern_roles

if __name__ == "__main__":
    sync_contacts()
    sync_accounts()
    sync_intern_roles()
    print("ETL job completed.")
