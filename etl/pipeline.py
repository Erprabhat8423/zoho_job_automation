from database.models import Contact, Session
from zoho.api_client import ZohoClient
from sqlalchemy.exc import IntegrityError
from datetime import datetime

def sync_contacts():
    zoho = ZohoClient()
    contacts = zoho.get_paginated_data("Contacts", [
        "id", "First_Name", "Last_Name", "Email", "Phone", "Account_Name", "Title", "Department", "Modified_Time"
    ])
    session = Session()

    for contact in contacts:
        existing = session.get(Contact, contact['id'])
        if existing:
            existing.first_name = contact.get('First_Name')
            existing.last_name = contact.get('Last_Name')
            existing.email = contact.get('Email')
            existing.phone = contact.get('Phone')
            existing.account_id = contact.get('Account_Name', {}).get('id') if contact.get('Account_Name') else None
            existing.title = contact.get('Title')
            existing.department = contact.get('Department')
            existing.updated_time = datetime.fromisoformat(contact['Modified_Time'].replace('Z', '+00:00'))
        else:
            new = Contact(
                id=contact['id'],
                first_name=contact.get('First_Name'),
                last_name=contact.get('Last_Name'),
                email=contact.get('Email'),
                phone=contact.get('Phone'),
                account_id=contact.get('Account_Name', {}).get('id') if contact.get('Account_Name') else None,
                title=contact.get('Title'),
                department=contact.get('Department'),
                updated_time=datetime.fromisoformat(contact['Modified_Time'].replace('Z', '+00:00'))
            )
            session.add(new)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        raise
    finally:
        session.close()

def sync_accounts():
    from database.models import Account
    zoho = ZohoClient()
    accounts = zoho.get_paginated_data("Accounts", [
        "id", "Account_Name", "Industry", "Website", "Phone", "Billing_Address", "Shipping_Address", "Modified_Time"
    ])
    session = Session()

    for account in accounts:
        existing = session.get(Account, account['id'])
        if existing:
            existing.name = account.get('Account_Name')
            existing.industry = account.get('Industry')
            existing.website = account.get('Website')
            existing.phone = account.get('Phone')
            existing.billing_address = account.get('Billing_Address')
            existing.shipping_address = account.get('Shipping_Address')
            existing.updated_time = datetime.fromisoformat(account['Modified_Time'].replace('Z', '+00:00'))
        else:
            new = Account(
                id=account['id'],
                name=account.get('Account_Name'),
                industry=account.get('Industry'),
                website=account.get('Website'),
                phone=account.get('Phone'),
                billing_address=account.get('Billing_Address'),
                shipping_address=account.get('Shipping_Address'),
                updated_time=datetime.fromisoformat(account['Modified_Time'].replace('Z', '+00:00'))
            )
            session.add(new)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        raise
    finally:
        session.close()

def sync_intern_roles():
    from database.models import InternRole
    zoho = ZohoClient()
    intern_roles = zoho.get_paginated_data("Intern_Roles", [
        "id", "Contact_Name", "Role_Title", "Start_Date", "End_Date", "Status", "Modified_Time"
    ])
    session = Session()

    for role in intern_roles:
        existing = session.get(InternRole, role['id'])
        if existing:
            existing.contact_id = role.get('Contact_Name', {}).get('id') if role.get('Contact_Name') else None
            existing.role_title = role.get('Role_Title')
            existing.start_date = datetime.fromisoformat(role['Start_Date']) if role.get('Start_Date') else None
            existing.end_date = datetime.fromisoformat(role['End_Date']) if role.get('End_Date') else None
            existing.status = role.get('Status')
            existing.updated_time = datetime.fromisoformat(role['Modified_Time'].replace('Z', '+00:00'))
        else:
            new = InternRole(
                id=role['id'],
                contact_id=role.get('Contact_Name', {}).get('id') if role.get('Contact_Name') else None,
                role_title=role.get('Role_Title'),
                start_date=datetime.fromisoformat(role['Start_Date']) if role.get('Start_Date') else None,
                end_date=datetime.fromisoformat(role['End_Date']) if role.get('End_Date') else None,
                status=role.get('Status'),
                updated_time=datetime.fromisoformat(role['Modified_Time'].replace('Z', '+00:00'))
            )
            session.add(new)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        raise
    finally:
        session.close()
