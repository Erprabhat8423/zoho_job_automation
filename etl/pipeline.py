import json
from database.models import Contact, Session, SyncTracker
from zoho.api_client import ZohoClient
from sqlalchemy.exc import IntegrityError
from datetime import datetime, timezone
from database.migrations import run_migrations
import logging

# Configure logging
logger = logging.getLogger(__name__)

def get_sync_tracker(entity_type):
    """Get sync tracker for a specific entity type"""
    session = Session()
    try:
        tracker = session.query(SyncTracker).filter_by(entity_type=entity_type).first()
        return tracker
    finally:
        session.close()

def update_sync_tracker(entity_type, last_timestamp, records_count):
    """Update sync tracker with latest sync information"""
    session = Session()
    try:
        tracker = session.query(SyncTracker).filter_by(entity_type=entity_type).first()
        if tracker:
            tracker.last_sync_timestamp = last_timestamp
            tracker.records_synced = records_count
            tracker.updated_at = datetime.now()
        else:
            tracker = SyncTracker(
                entity_type=entity_type,
                last_sync_timestamp=last_timestamp,
                records_synced=records_count
            )
            session.add(tracker)
        
        session.commit()
        logger.info(f"Updated sync tracker for {entity_type}: timestamp={last_timestamp}, records={records_count}")
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating sync tracker for {entity_type}: {e}")
        raise
    finally:
        session.close()

def build_incremental_criteria(last_sync_timestamp):
    """Build Zoho API criteria for incremental sync based on Modified_Time"""
    if not last_sync_timestamp:
        return None
    
    # Format timestamp for Zoho API (ISO format)
    # Zoho expects format: YYYY-MM-DDTHH:mm:ss+HH:mm
    if hasattr(last_sync_timestamp, 'tzinfo'):
        if last_sync_timestamp.tzinfo is None:
            last_sync_timestamp = last_sync_timestamp.replace(tzinfo=timezone.utc)
    else:
        logger.error(f"Expected datetime object but got {type(last_sync_timestamp)}: {last_sync_timestamp}")
        return None
    
    formatted_time = last_sync_timestamp.strftime("%Y-%m-%dT%H:%M:%S%z")
    # Add colon to timezone offset for Zoho API compatibility
    if formatted_time.endswith('+0000'):
        formatted_time = formatted_time[:-5] + '+00:00'
    
    criteria = f"(Modified_Time:greater_than:{formatted_time})"
    logger.info(f"Built incremental criteria: {criteria}")
    return criteria

def get_latest_modified_time(records):
    """Get the latest Modified_Time from a list of records"""
    if not records:
        return None
    
    latest_time = None
    for record in records:
        modified_time_str = record.get('Modified_Time')
        if modified_time_str:
            try:
                # Parse Zoho timestamp
                modified_time = datetime.fromisoformat(modified_time_str.replace('Z', '+00:00'))
                if latest_time is None or modified_time > latest_time:
                    latest_time = modified_time
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not parse Modified_Time '{modified_time_str}': {e}")
                continue
    
    return latest_time

def ensure_database_ready():
    """Ensure database tables exist and are up to date before running ETL"""
    logger.info("Checking database schema...")
    if not run_migrations():
        raise Exception("Database migration failed. Cannot proceed with ETL.")
    logger.info("Database schema check completed successfully")

def sync_contacts(incremental=True):
    # Ensure database is ready before syncing
    ensure_database_ready()
    
    logger.info("Starting contact sync...")
    zoho = ZohoClient()
    
    # Determine sync criteria
    criteria = None
    last_sync_info = ""
    
    if incremental:
        tracker = get_sync_tracker('contacts')
        if tracker and tracker.last_sync_timestamp:
            criteria = build_incremental_criteria(tracker.last_sync_timestamp)
            last_sync_info = f" (incremental from {tracker.last_sync_timestamp})"
            logger.info(f"Performing incremental sync from: {tracker.last_sync_timestamp}")
        else:
            logger.info("No previous sync found, performing full sync")
    else:
        logger.info("Performing full sync (incremental=False)")
    
    logger.info(f"Getting contact data from Zoho{last_sync_info}...")
    contacts = zoho.get_paginated_data("Contacts", [
        "id", "First_Name", "Last_Name", "Email", "Phone", "Account_Name", "Title", "Department", "Modified_Time",
        "Age_on_Start_Date", "Timezone", "Contact_Last_Name", "$field_states", 
        "Arrival_drop_off_address", "Gender", "Interview", "$process_flow", 
        "End_date", "Role_Owner", "Paid_Role", "Role_Success_Notes", "$approval", "Departure_date_time", 
        "Approval_date", "Requires_a_visa", "Contact_Email", 
        "Follow_up_Date", "$review_process", "Admission_Member", 
        "English_Level", "Placement_status", "Likelihood_to_convert", 
        "Role_Success_Stage", "Call_to_Conversion_Time_days", "Lead_Created_Time", 
        "University_Name", "Job_Title", "Layout", "Intro_Call_Date", 
        "Visa_Alt_Options", "Student_decision", "Email", "Arrival_date_time", 
        "Rating_New", "Role_confirmed_date", "Start_date", "Last_Activity_Time", "Industry", "Visa_F_U_Date", 
        "Location_Other", "Placement_Lead_Time_days", "Graduation_Date", 
        "Other_Payment_Status", "Days_Since_Conversion", "Name1", 
        "Average_no_of_days", "Duration", "Warm_Call", "Other_industry", 
        "Call_Scheduled_Date_Time", "Interviewer", "Visa_successful", 
        "UTM_Campaign", "Rating", "Alternative_Location1", 
        "Enrolment_to_Intro_Call_Lead_Time", "Modified_By", "$review", "Reason_for_Cancellation", 
        "Cancelled_Date_Time", "Uni_Start_Date", "Notes1", 
        "Partner_Organisation", "Modified_Time", "Date_of_Birth", 
        "Call_Booked_Date_Time", "Date_of_Cancellation", "$in_merge", "$approval_state", "Location", 
        "Industry_Choice_1", "Industry_Choice_3", "Company_decision", "Student_Bio", "Token", 
        "Additional_Information", "Placement_Deadline", "Created_Time", "Change_Log_Time__s", 
        "Community_Owner", "Created_By", 
        "Current_Location_V2", "Decision_Date", "UTM_Medium", "Description", 
        "Do_Not_Contact", "Industry_choice_2", "Job_offered_after", "Full_Name", "PS_Assigned_Date", 
        "Account_Name", "Email_Opt_Out", 
        "books_cust_id", "Student_Status", "Days_Count", 
        "Record_Status__s", "Nationality", "Type", "Cancellation_Notes", 
        "Departure_flight_number", "Locked__s", "Tag", "Last_Enriched_Time__s", 
        "Country_city_of_residence", "Refund_date", "Visa_Type_Exemption", 
        "Industry_2_Areas", "$locked_for_me", 
        "From_University_partner", "Placement_Urgency", 
        "Enrich_Status__s", "Visa_Eligible", "UTM_Content", 
        "Cohort_Start_Date", "Secondary_Email", "$is_duplicate", "Signed_Agreement", 
        "MyInterview_URL", "Interview_successful", "Skills", 
        "Link_to_CV", "Accommodation_finalised", "Send_Mail2", "UTM_GCLID", 
        "Unsubscribed_Time", "T_C_Link", 
        "Number_of_Days", "Agreement_finalised", "End_date_Auto_populated", "Industry_1_Areas", 
        "Last_Name", 
        "Total", "Visa_Owner", "Visa_Note_s", "House_rules"
    ], criteria=criteria, sort_by="Modified_Time", sort_order="asc")
    session = Session()
    
    logger.info(f"Processing {len(contacts)} contacts...")

    for i, contact in enumerate(contacts):
        logger.info(f"Processing contact {i+1}/{len(contacts)}: {contact.get('id', 'Unknown ID')}")
        existing = session.get(Contact, contact['id'])
        
        # Helper function to parse datetime
        def parse_datetime(date_str):
            if not date_str:
                return None
            try:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                return None
        
        # Helper function to convert lists to JSON string
        def list_to_json(lst):
            if not lst:
                return None
            return json.dumps(lst)
        
        # Helper function to extract nested object ID
        def extract_id(obj):
            return obj.get('id') if obj else None
        
        # Helper function to extract nested object name
        def extract_name(obj):
            return obj.get('name') if obj else None
        
        # Helper function to extract nested object email
        def extract_email(obj):
            return obj.get('email') if obj else None
        
        contact_data = {
            'id': contact['id'],
            'first_name': contact.get('First_Name'),
            'last_name': contact.get('Last_Name'),
            'email': contact.get('Email'),
            'phone': contact.get('Phone'),
            'account_id': extract_id(contact.get('Account_Name')),
            'title': contact.get('Title'),
            'department': contact.get('Department'),
            'updated_time': parse_datetime(contact.get('Modified_Time')),
            'age_on_start_date': contact.get('Age_on_Start_Date'),
            'timezone': contact.get('Timezone'),
            'contact_last_name': contact.get('Contact_Last_Name'),
            'field_states': json.dumps(contact.get('$field_states')) if contact.get('$field_states') else None,
            'arrival_drop_off_address': contact.get('Arrival_drop_off_address'),
            'gender': contact.get('Gender'),
            'interview': contact.get('Interview'),
            'process_flow': contact.get('$process_flow', False),
            'end_date': parse_datetime(contact.get('End_date')),
            'role_owner': extract_name(contact.get('Role_Owner')),
            'paid_role': contact.get('Paid_Role'),
            'role_success_notes': contact.get('Role_Success_Notes'),
            'approval': json.dumps(contact.get('$approval')) if contact.get('$approval') else None,
            'departure_date_time': parse_datetime(contact.get('Departure_date_time')),
            'approval_date': parse_datetime(contact.get('Approval_date')),
            'requires_a_visa': contact.get('Requires_a_visa'),
            'contact_email': contact.get('Contact_Email'),
            'follow_up_date': parse_datetime(contact.get('Follow_up_Date')),
            'review_process': json.dumps(contact.get('$review_process')) if contact.get('$review_process') else None,
            'admission_member': contact.get('Admission_Member'),
            'english_level': contact.get('English_Level'),
            'placement_status': contact.get('Placement_status'),
            'likelihood_to_convert': contact.get('Likelihood_to_convert'),
            'role_success_stage': contact.get('Role_Success_Stage'),
            'call_to_conversion_time_days': contact.get('Call_to_Conversion_Time_days'),
            'lead_created_time': parse_datetime(contact.get('Lead_Created_Time')),
            'university_name': contact.get('University_Name'),
            'job_title': contact.get('Job_Title'),
            'layout_id': extract_id(contact.get('Layout')),
            'layout_display_label': contact.get('Layout', {}).get('display_label') if contact.get('Layout') else None,
            'layout_name': contact.get('Layout', {}).get('name') if contact.get('Layout') else None,
            'intro_call_date': parse_datetime(contact.get('Intro_Call_Date')),
            'visa_alt_options': list_to_json(contact.get('Visa_Alt_Options')),
            'student_decision': contact.get('Student_decision'),
            'arrival_date_time': parse_datetime(contact.get('Arrival_date_time')),
            'rating_new': contact.get('Rating_New'),
            'role_confirmed_date': parse_datetime(contact.get('Role_confirmed_date')),
            'start_date': parse_datetime(contact.get('Start_date')),
            'last_activity_time': parse_datetime(contact.get('Last_Activity_Time')),
            'industry': contact.get('Industry'),
            'visa_f_u_date': parse_datetime(contact.get('Visa_F_U_Date')),
            'location_other': contact.get('Location_Other'),
            'placement_lead_time_days': contact.get('Placement_Lead_Time_days'),
            'graduation_date': parse_datetime(contact.get('Graduation_Date')),
            'other_payment_status': contact.get('Other_Payment_Status'),
            'days_since_conversion': contact.get('Days_Since_Conversion'),
            'name1': contact.get('Name1'),
            'average_no_of_days': contact.get('Average_no_of_days'),
            'duration': contact.get('Duration'),
            'warm_call': contact.get('Warm_Call'),
            'other_industry': contact.get('Other_industry'),
            'call_scheduled_date_time': parse_datetime(contact.get('Call_Scheduled_Date_Time')),
            'interviewer': extract_name(contact.get('Interviewer')),
            'visa_successful': contact.get('Visa_successful'),
            'utm_campaign': contact.get('UTM_Campaign'),
            'rating': list_to_json(contact.get('Rating')),
            'alternative_location1': contact.get('Alternative_Location1'),
            'enrolment_to_intro_call_lead_time': contact.get('Enrolment_to_Intro_Call_Lead_Time'),
            'review': json.dumps(contact.get('$review')) if contact.get('$review') else None,
            'reason_for_cancellation': contact.get('Reason_for_Cancellation'),
            'cancelled_date_time': parse_datetime(contact.get('Cancelled_Date_Time')),
            'uni_start_date': parse_datetime(contact.get('Uni_Start_Date')),
            'notes1': contact.get('Notes1'),
            'partner_organisation': contact.get('Partner_Organisation'),
            'date_of_birth': parse_datetime(contact.get('Date_of_Birth')),
            'call_booked_date_time': parse_datetime(contact.get('Call_Booked_Date_Time')),
            'date_of_cancellation': parse_datetime(contact.get('Date_of_Cancellation')),
            'in_merge': contact.get('$in_merge', False),
            'approval_state': contact.get('$approval_state'),
            'location': contact.get('Location'),
            'industry_choice_1': contact.get('Industry_Choice_1'),
            'industry_choice_3': contact.get('Industry_Choice_3'),
            'company_decision': contact.get('Company_decision'),
            'student_bio': contact.get('Student_Bio'),
            'token': contact.get('Token'),
            'additional_information': contact.get('Additional_Information'),
            'placement_deadline': parse_datetime(contact.get('Placement_Deadline')),
            'created_time': parse_datetime(contact.get('Created_Time')),
            'change_log_time': parse_datetime(contact.get('Change_Log_Time__s')),
            'community_owner': extract_name(contact.get('Community_Owner')),
            'created_by_email': extract_email(contact.get('Created_By')),
            'current_location_v2': contact.get('Current_Location_V2'),
            'decision_date': parse_datetime(contact.get('Decision_Date')),
            'utm_medium': contact.get('UTM_Medium'),
            'description': contact.get('Description'),
            'do_not_contact': contact.get('Do_Not_Contact', False),
            'industry_choice_2': contact.get('Industry_choice_2'),
            'job_offered_after': contact.get('Job_offered_after'),
            'full_name': contact.get('Full_Name'),
            'ps_assigned_date': parse_datetime(contact.get('PS_Assigned_Date')),

            'account_name': extract_name(contact.get('Account_Name')),
            'email_opt_out': contact.get('Email_Opt_Out', False),

            'books_cust_id': contact.get('books_cust_id'),

            'student_status': contact.get('Student_Status'),
            'days_count': contact.get('Days_Count'),
            'record_status': contact.get('Record_Status__s'),
            'nationality': contact.get('Nationality'),

            'type': contact.get('Type'),
            'cancellation_notes': contact.get('Cancellation_Notes'),
            'departure_flight_number': contact.get('Departure_flight_number'),
            'locked': contact.get('Locked__s', False),
            'tag': list_to_json(contact.get('Tag')),
            'last_enriched_time': parse_datetime(contact.get('Last_Enriched_Time__s')),

            'country_city_of_residence': contact.get('Country_city_of_residence'),

            'refund_date': parse_datetime(contact.get('Refund_date')),

            'visa_type_exemption': contact.get('Visa_Type_Exemption'),

            'industry_2_areas': contact.get('Industry_2_Areas'),

            'locked_for_me': contact.get('$locked_for_me', False),
            'from_university_partner': contact.get('From_University_partner'),

            'placement_urgency': contact.get('Placement_Urgency'),
            'enrich_status': contact.get('Enrich_Status__s'),
            'visa_eligible': contact.get('Visa_Eligible'),

            'utm_content': contact.get('UTM_Content'),

            'cohort_start_date': parse_datetime(contact.get('Cohort_Start_Date')),
            'secondary_email': contact.get('Secondary_Email'),
            'is_duplicate': contact.get('$is_duplicate', False),
            'signed_agreement': contact.get('Signed_Agreement'),
            'myinterview_url': contact.get('MyInterview_URL'),
            'interview_successful': contact.get('Interview_successful'),
            'skills': contact.get('Skills'),

            'link_to_cv': contact.get('Link_to_CV'),

            'accommodation_finalised': contact.get('Accommodation_finalised'),
            'send_mail2': contact.get('Send_Mail2', False),

            'utm_gclid': contact.get('UTM_GCLID'),
            'unsubscribed_time': parse_datetime(contact.get('Unsubscribed_Time')),
            't_c_link': contact.get('T_C_Link'),

            'number_of_days': contact.get('Number_of_Days'),
            'agreement_finalised': contact.get('Agreement_finalised'),
            'end_date_auto_populated': parse_datetime(contact.get('End_date_Auto_populated')),
            'industry_1_areas': contact.get('Industry_1_Areas'),

        

            'total': contact.get('Total'),
            'visa_owner': extract_name(contact.get('Visa_Owner')),
            'visa_notes': contact.get('Visa_Note_s'),
            'house_rules': contact.get('House_rules')
        }
        
        
        if existing:
            for key, value in contact_data.items():
                if key != 'id':  # Don't update the primary key
                    try:
                        setattr(existing, key, value)
                    except Exception as e:
                        logger.error(f"Error setting attribute {key} with value {value} (type: {type(value)}) for contact {contact.get('id', 'Unknown')}: {str(e)}")
                        if isinstance(value, dict):
                            logger.error(f"Dict value found for key {key}: {value}")
                        raise
        else:
            try:
                new = Contact(**contact_data)
                session.add(new)
            except Exception as e:
                logger.error(f"Error creating Contact object for contact {contact.get('id', 'Unknown')}: {str(e)}")
                # Log the problematic field
                for key, value in contact_data.items():
                    if isinstance(value, dict):
                        logger.error(f"Dict field found: {key} = {value} for contact {contact.get('id', 'Unknown')}")
                raise
    
    try:
        session.commit()
        
        # Update sync tracker after successful commit
        if contacts:
            latest_modified_time = get_latest_modified_time(contacts)
            if latest_modified_time:
                update_sync_tracker('contacts', latest_modified_time, len(contacts))
        
        logger.info(f"Contacts sync completed successfully - processed {len(contacts)} records")
    except Exception as e:
        logger.error(f"Error during contact sync commit: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()

def sync_accounts(incremental=True):
    # Ensure database is ready before syncing
    ensure_database_ready()
    
    from database.models import Account
    zoho = ZohoClient()
    
    # Build criteria for incremental sync
    criteria = None
    if incremental:
        tracker = get_sync_tracker('accounts')
        if tracker:
            criteria = build_incremental_criteria(tracker.last_sync_timestamp)
            if criteria:
                logger.info(f"Using incremental sync criteria for accounts: {criteria}")
            else:
                logger.info("No previous sync found for accounts, performing full sync")
        else:
            logger.info("No previous sync found for accounts, performing full sync")
    
    accounts = zoho.get_paginated_data("Accounts", [
        "id", "Account_Name", "Industry", "Billing_Address", "Shipping_Address",
        "Owner", "Cleanup_Start_Date", "$field_states", "Management_Status",
        "Company_Work_Policy", "Last_Activity_Time", "Last_Full_Due_Diligence_Date", "Company_Industry",
        "Company_Desciption", "$process_flow", "Approval_status",
        "Street", "$locked_for_me", "Classic_Partnership", "State_Region", "Cleanup_Status", "Uni_Region", "$approval",
        "Uni_Outreach_Status", "Enrich_Status__s", "$review_process",
        "Roles_available", "Roles",
        "City", "Postcode", "Outreach_Notes", "Company_Industry_Other", "No_Employees", "Industry_areas",
        "Placement_s_Revision_Required", "Country",
        "$is_duplicate", "Uni_State_if_in_US", "Follow_up_Date", "$review_process",
        "$layout_id", "$review", "Cleanup_Notes", "Gold_Rating",
        "Account_Notes", "Standard_working_hours", "Due_Diligence_Fields_to_Revise", "Uni_Country", "Cleanup_Phase", "Next_Reply_Date",
        "Record_Status__s", "Type", "Layout", "Modified_Time",
        "$in_merge", "Uni_Timezone", "Upon_to_Remote_interns", "Locked__s", "Company_Address", "Tag", "$approval_state",
        "$pathfinder", "Location", "Location_other", "Account_Status"
    ], criteria=criteria, sort_by="Modified_Time", sort_order="asc")
    session = Session()
    
    logger.info(f"Processing {len(accounts)} accounts...")
    
    for i, account in enumerate(accounts):
        existing = session.get(Account, account['id'])
        
        # Helper function to parse datetime
        def parse_datetime(date_str):
            if not date_str:
                return None
            try:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                return None
        
        # Helper function to convert lists to JSON string
        def list_to_json(lst):
            if not lst:
                return None
            return json.dumps(lst)
        
        # Helper function to extract nested object ID
        def extract_id(obj):
            return obj.get('id') if obj else None
        
        # Helper function to extract nested object name
        def extract_name(obj):
            return obj.get('name') if obj else None
        
        # Helper function to extract nested object email
        def extract_email(obj):
            return obj.get('email') if obj else None
        
        account_data = {
            'id': account['id'],
            'name': account.get('Account_Name'),
            'industry': account.get('Industry'),
            'billing_address': account.get('Billing_Address'),
            'shipping_address': account.get('Shipping_Address'),
            'owner_id': extract_id(account.get('Owner')),
            'owner_name': extract_name(account.get('Owner')),
            'owner_email': extract_email(account.get('Owner')),
            'cleanup_start_date': parse_datetime(account.get('Cleanup_Start_Date')),
            'field_states': account.get('$field_states'),
            'management_status': account.get('Management_Status'),
            'company_work_policy': list_to_json(account.get('Company_Work_Policy')),
            'last_activity_time': parse_datetime(account.get('Last_Activity_Time')),
            'last_full_due_diligence_date': parse_datetime(account.get('Last_Full_Due_Diligence_Date')),
            'company_industry': account.get('Company_Industry'),
            'company_description': account.get('Company_Desciption'),
            'process_flow': account.get('$process_flow', False),
            'approval_status': account.get('Approval_status'),
            'street': account.get('Street'),
            'locked_for_me': account.get('$locked_for_me', False),
            'classic_partnership': account.get('Classic_Partnership'),
            'state_region': account.get('State_Region'),
            'cleanup_status': account.get('Cleanup_Status'),
            'uni_region': account.get('Uni_Region'),
            'approval': json.dumps(account.get('$approval')) if account.get('$approval') else None,
            'uni_outreach_status': account.get('Uni_Outreach_Status'),
            'enrich_status': account.get('Enrich_Status__s'),
            'review_process': json.dumps(account.get('$review_process')) if account.get('$review_process') else None,
            'roles_available': account.get('Roles_available'),
            'roles': account.get('Roles'),
            'city': account.get('City'),
            'postcode': account.get('Postcode'),
            'outreach_notes': account.get('Outreach_Notes'),
            'company_industry_other': account.get('Company_Industry_Other'),
            'no_employees': account.get('No_Employees'),
            'industry_areas': account.get('Industry_areas'),

            'placement_revision_required': account.get('Placement_s_Revision_Required'),
            'country': account.get('Country'),


            'is_duplicate': account.get('$is_duplicate', False),
            'uni_state_if_in_us': account.get('Uni_State_if_in_US'),
            'follow_up_date': parse_datetime(account.get('Follow_up_Date')),
            'review_process': json.dumps(account.get('$review_process')) if account.get('$review_process') else None,
            'layout_id': extract_id(account.get('$layout_id')),
            'layout_display_label': account.get('$layout_id', {}).get('display_label') if account.get('$layout_id') else None,
            'layout_name': account.get('$layout_id', {}).get('name') if account.get('$layout_id') else None,

            'review': account.get('$review'),
            'cleanup_notes': account.get('Cleanup_Notes'),
            'gold_rating': account.get('Gold_Rating', False),
            'account_notes': account.get('Account_Notes'),

            'standard_working_hours': account.get('Standard_working_hours'),
            'due_diligence_fields_to_revise': list_to_json(account.get('Due_Diligence_Fields_to_Revise')),
            'uni_country': account.get('Uni_Country'),
            'cleanup_phase': account.get('Cleanup_Phase'),
            'next_reply_date': parse_datetime(account.get('Next_Reply_Date')),
            'record_status': account.get('Record_Status__s'),

            'type': account.get('Type'),

            'in_merge': account.get('$in_merge', False),
            'uni_timezone': account.get('Uni_Timezone'),
            'upon_to_remote_interns': account.get('Upon_to_Remote_interns', False),
            'locked': account.get('Locked__s', False),
            'company_address': account.get('Company_Address'),
            'tag': list_to_json(account.get('Tag')),
            'approval_state': account.get('$approval_state'),
            'pathfinder': account.get('$pathfinder', False),

            'location': account.get('Location'),
            'location_other': account.get('Location_other'),
            'account_status': account.get('Account_Status'),
            'modified_time': parse_datetime(account.get('Modified_Time'))
        }
        
        if existing:
            for key, value in account_data.items():
                if key != 'id':  # Don't update the primary key
                    setattr(existing, key, value)
        else:
            new = Account(**account_data)
            session.add(new)
    
    try:
        session.commit()
        
        # Update sync tracker for accounts
        if incremental and accounts:
            latest_modified_time = get_latest_modified_time(accounts)
            if latest_modified_time:
                update_sync_tracker('accounts', latest_modified_time, len(accounts))
                logger.info(f"Updated sync tracker for accounts: {len(accounts)} records, latest timestamp: {latest_modified_time}")
        
        logger.info(f"Successfully processed {len(accounts)} accounts")
    except IntegrityError:
        session.rollback()
        raise
    finally:
        session.close()

def sync_intern_roles(incremental=True):
    # Ensure database is ready before syncing
    ensure_database_ready()
    
    from database.models import InternRole
    zoho = ZohoClient()
    
    # Build criteria for incremental sync
    criteria = None
    if incremental:
        tracker = get_sync_tracker('intern_roles')
        if tracker:
            criteria = build_incremental_criteria(tracker.last_sync_timestamp)
            if criteria:
                logger.info(f"Using incremental sync criteria for intern roles: {criteria}")
            else:
                logger.info("No previous sync found for intern roles, performing full sync")
        else:
            logger.info("No previous sync found for intern roles, performing full sync")
    
    intern_roles = zoho.get_paginated_data("Intern_Roles", [
        "id", "Name", "Role_Title", "Role_Description_Requirements", "Role_Status", 
        "Role_Function", "Role_Department_Size", "Role_Attachments_JD", 
        "Role_Tags", "Start_Date", "End_Date", "Created_Time", "Modified_Time",
        "Intern_Company", "Company_Work_Policy", "Location", 
        "Open_to_Remote", "Due_Diligence_Status_2",
        "Account_Outreach_Status", "Record_Status__s", "Approval_State", "Management_Status", 
        "Placement_Fields_to_Revise", "Placement_Revision_Notes", "Gold_Rating", "Locked__s"
    ], criteria=criteria, sort_by="Modified_Time", sort_order="asc")
    session = Session()
    
    logger.info(f"Processing {len(intern_roles)} intern roles...")

    for i, role in enumerate(intern_roles):
        existing = session.get(InternRole, role['id'])
        
        # Helper function to parse datetime
        def parse_datetime(date_str):
            if not date_str:
                return None
            try:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                return None
        
        # Helper function to convert lists to JSON string
        def list_to_json(lst):
            if not lst:
                return None
            return json.dumps(lst)
        
        # Helper function to extract nested object ID
        def extract_id(obj):
            return obj.get('id') if obj else None
        
        # Helper function to extract nested object name
        def extract_name(obj):
            return obj.get('name') if obj else None
        
        # Helper function to extract nested object email
        def extract_email(obj):
            return obj.get('email') if obj else None
        
        role_data = {
            'id': role['id'],
            'name': role.get('Name'),
            'role_title': role.get('Role_Title'),
            'role_description_requirements': role.get('Role_Description_Requirements'),
            'role_status': role.get('Role_Status'),
            'role_function': role.get('Role_Function'),
            'role_department_size': role.get('Role_Department_Size'),
            'role_attachments_jd': list_to_json(role.get('Role_Attachments_JD')),
            'role_tags': list_to_json(role.get('Role_Tags')),
            'start_date': parse_datetime(role.get('Start_Date')),
            'end_date': parse_datetime(role.get('End_Date')),
            'created_time': parse_datetime(role.get('Created_Time')),
            'intern_company_id': extract_id(role.get('Intern_Company')),
            'intern_company_name': extract_name(role.get('Intern_Company')),
            'company_work_policy': list_to_json(role.get('Company_Work_Policy')),
            'location': role.get('Location'),
            'open_to_remote': role.get('Open_to_Remote'),
            'due_diligence_status_2': role.get('Due_Diligence_Status_2'),
            'account_outreach_status': role.get('Account_Outreach_Status'),
            'record_status': role.get('Record_Status__s'),
            'approval_state': role.get('Approval_State'),
            'management_status': role.get('Management_Status'),
            'placement_fields_to_revise': list_to_json(role.get('Placement_Fields_to_Revise')),
            'placement_revision_notes': role.get('Placement_Revision_Notes'),
            'gold_rating': role.get('Gold_Rating', False),
            'locked': role.get('Locked__s', False)
        }
        
        if existing:
            for key, value in role_data.items():
                if key != 'id':  # Don't update the primary key
                    setattr(existing, key, value)
        else:
            new = InternRole(**role_data)
            session.add(new)
    
    try:
        session.commit()
        
        # Update sync tracker for intern roles
        if incremental and intern_roles:
            latest_modified_time = get_latest_modified_time(intern_roles)
            if latest_modified_time:
                update_sync_tracker('intern_roles', latest_modified_time, len(intern_roles))
                logger.info(f"Updated sync tracker for intern roles: {len(intern_roles)} records, latest timestamp: {latest_modified_time}")
        
        logger.info(f"Successfully processed {len(intern_roles)} intern roles")
    except IntegrityError:
        session.rollback()
        raise
    finally:
        session.close()
