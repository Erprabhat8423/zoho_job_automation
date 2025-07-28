import sys
import os
import logging
from datetime import datetime
import json

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from zoho.api_client import ZohoClient
from database.models import Contact, Session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('debug_etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def debug_contacts_sync():
    """Debug the contacts sync to identify the problematic field"""
    logger.info("Starting debug of contacts sync...")
    
    zoho = ZohoClient()
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
    ])
    
    logger.info(f"Retrieved {len(contacts)} contacts from Zoho")
    
    # Test with just the first contact
    if contacts:
        contact = contacts[0]
        logger.info(f"Testing with contact ID: {contact.get('id')}")
        
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
        
        # Test each field individually
        contact_data = {}
        
        # Test basic fields first
        basic_fields = {
            'id': contact.get('id'),
            'first_name': contact.get('First_Name'),
            'last_name': contact.get('Last_Name'),
            'email': contact.get('Email'),
            'phone': contact.get('Phone'),
        }
        
        logger.info("Testing basic fields...")
        for field_name, value in basic_fields.items():
            logger.info(f"Field: {field_name}, Type: {type(value)}, Value: {value}")
            contact_data[field_name] = value
        
        # Test complex fields that might cause issues
        complex_fields = {
            'account_id': extract_id(contact.get('Account_Name')),
            'account_name': contact.get('Account_Name'),
            'field_states': contact.get('$field_states'),
            'approval': json.dumps(contact.get('$approval')) if contact.get('$approval') else None,
            'review_process': json.dumps(contact.get('$review_process')) if contact.get('$review_process') else None,
            'visa_alt_options': list_to_json(contact.get('Visa_Alt_Options')),
            'rating': list_to_json(contact.get('Rating')),
            'tag': list_to_json(contact.get('Tag')),
        }
        
        logger.info("Testing complex fields...")
        for field_name, value in complex_fields.items():
            logger.info(f"Field: {field_name}, Type: {type(value)}, Value: {value}")
            contact_data[field_name] = value
        
        # Test datetime fields
        datetime_fields = {
            'updated_time': parse_datetime(contact.get('Modified_Time')),
            'end_date': parse_datetime(contact.get('End_date')),
            'departure_date_time': parse_datetime(contact.get('Departure_date_time')),
            'approval_date': parse_datetime(contact.get('Approval_date')),
            'follow_up_date': parse_datetime(contact.get('Follow_up_Date')),
            'lead_created_time': parse_datetime(contact.get('Lead_Created_Time')),
            'intro_call_date': parse_datetime(contact.get('Intro_Call_Date')),
            'arrival_date_time': parse_datetime(contact.get('Arrival_date_time')),
            'role_confirmed_date': parse_datetime(contact.get('Role_confirmed_date')),
            'start_date': parse_datetime(contact.get('Start_date')),
            'last_activity_time': parse_datetime(contact.get('Last_Activity_Time')),
            'visa_f_u_date': parse_datetime(contact.get('Visa_F_U_Date')),
            'graduation_date': parse_datetime(contact.get('Graduation_Date')),
            'call_scheduled_date_time': parse_datetime(contact.get('Call_Scheduled_Date_Time')),
            'cancelled_date_time': parse_datetime(contact.get('Cancelled_Date_Time')),
            'uni_start_date': parse_datetime(contact.get('Uni_Start_Date')),
            'date_of_birth': parse_datetime(contact.get('Date_of_Birth')),
            'call_booked_date_time': parse_datetime(contact.get('Call_Booked_Date_Time')),
            'date_of_cancellation': parse_datetime(contact.get('Date_of_Cancellation')),
            'placement_deadline': parse_datetime(contact.get('Placement_Deadline')),
            'created_time': parse_datetime(contact.get('Created_Time')),
            'change_log_time': parse_datetime(contact.get('Change_Log_Time__s')),
            'decision_date': parse_datetime(contact.get('Decision_Date')),
            'ps_assigned_date': parse_datetime(contact.get('PS_Assigned_Date')),
            'last_enriched_time': parse_datetime(contact.get('Last_Enriched_Time__s')),
            'refund_date': parse_datetime(contact.get('Refund_date')),
            'cohort_start_date': parse_datetime(contact.get('Cohort_Start_Date')),
            'unsubscribed_time': parse_datetime(contact.get('Unsubscribed_Time')),
            'end_date_auto_populated': parse_datetime(contact.get('End_date_Auto_populated')),
        }
        
        logger.info("Testing datetime fields...")
        for field_name, value in datetime_fields.items():
            logger.info(f"Field: {field_name}, Type: {type(value)}, Value: {value}")
            contact_data[field_name] = value
        
        # Test boolean fields
        boolean_fields = {
            'process_flow': contact.get('$process_flow', False),
            'in_merge': contact.get('$in_merge', False),
            'do_not_contact': contact.get('Do_Not_Contact', False),
            'locked': contact.get('Locked__s', False),
            'locked_for_me': contact.get('$locked_for_me', False),
            'is_duplicate': contact.get('$is_duplicate', False),
            'send_mail2': contact.get('Send_Mail2', False),
            'email_opt_out': contact.get('Email_Opt_Out', False),
        }
        
        logger.info("Testing boolean fields...")
        for field_name, value in boolean_fields.items():
            logger.info(f"Field: {field_name}, Type: {type(value)}, Value: {value}")
            contact_data[field_name] = value
        
        # Test integer fields
        integer_fields = {
            'age_on_start_date': contact.get('Age_on_Start_Date'),
            'call_to_conversion_time_days': contact.get('Call_to_Conversion_Time_days'),
            'placement_lead_time_days': contact.get('Placement_Lead_Time_days'),
            'days_since_conversion': contact.get('Days_Since_Conversion'),
            'average_no_of_days': contact.get('Average_no_of_days'),
            'enrolment_to_intro_call_lead_time': contact.get('Enrolment_to_Intro_Call_Lead_Time'),
            'days_count': contact.get('Days_Count'),
            'number_of_days': contact.get('Number_of_Days'),
        }
        
        logger.info("Testing integer fields...")
        for field_name, value in integer_fields.items():
            logger.info(f"Field: {field_name}, Type: {type(value)}, Value: {value}")
            contact_data[field_name] = value
        
        # Test string fields
        string_fields = {
            'title': contact.get('Title'),
            'department': contact.get('Department'),
            'timezone': contact.get('Timezone'),
            'contact_last_name': contact.get('Contact_Last_Name'),
            'arrival_drop_off_address': contact.get('Arrival_drop_off_address'),
            'gender': contact.get('Gender'),
            'interview': contact.get('Interview'),
            'role_owner': contact.get('Role_Owner'),
            'paid_role': contact.get('Paid_Role'),
            'role_success_notes': contact.get('Role_Success_Notes'),
            'requires_a_visa': contact.get('Requires_a_visa'),
            'contact_email': contact.get('Contact_Email'),
            'admission_member': contact.get('Admission_Member'),
            'english_level': contact.get('English_Level'),
            'placement_status': contact.get('Placement_status'),
            'likelihood_to_convert': contact.get('Likelihood_to_convert'),
            'role_success_stage': contact.get('Role_Success_Stage'),
            'university_name': contact.get('University_Name'),
            'job_title': contact.get('Job_Title'),
            'layout_id': extract_id(contact.get('Layout')),
            'layout_display_label': contact.get('Layout', {}).get('display_label') if contact.get('Layout') else None,
            'layout_name': contact.get('Layout', {}).get('name') if contact.get('Layout') else None,
            'student_decision': contact.get('Student_decision'),
            'rating_new': contact.get('Rating_New'),
            'industry': contact.get('Industry'),
            'location_other': contact.get('Location_Other'),
            'other_payment_status': contact.get('Other_Payment_Status'),
            'name1': contact.get('Name1'),
            'duration': contact.get('Duration'),
            'warm_call': contact.get('Warm_Call'),
            'other_industry': contact.get('Other_industry'),
            'interviewer': contact.get('Interviewer'),
            'visa_successful': contact.get('Visa_successful'),
            'utm_campaign': contact.get('UTM_Campaign'),
            'alternative_location1': contact.get('Alternative_Location1'),
            'review': contact.get('$review'),
            'reason_for_cancellation': contact.get('Reason_for_Cancellation'),
            'notes1': contact.get('Notes1'),
            'partner_organisation': contact.get('Partner_Organisation'),
            'community_owner': contact.get('Community_Owner'),
            'created_by_email': extract_email(contact.get('Created_By')),
            'current_location_v2': contact.get('Current_Location_V2'),
            'utm_medium': contact.get('UTM_Medium'),
            'description': contact.get('Description'),
            'industry_choice_2': contact.get('Industry_choice_2'),
            'job_offered_after': contact.get('Job_offered_after'),
            'full_name': contact.get('Full_Name'),
            'books_cust_id': contact.get('books_cust_id'),
            'student_status': contact.get('Student_Status'),
            'record_status': contact.get('Record_Status__s'),
            'nationality': contact.get('Nationality'),
            'type': contact.get('Type'),
            'cancellation_notes': contact.get('Cancellation_Notes'),
            'departure_flight_number': contact.get('Departure_flight_number'),
            'country_city_of_residence': contact.get('Country_city_of_residence'),
            'visa_type_exemption': contact.get('Visa_Type_Exemption'),
            'industry_2_areas': contact.get('Industry_2_Areas'),
            'from_university_partner': contact.get('From_University_partner'),
            'placement_urgency': contact.get('Placement_Urgency'),
            'enrich_status': contact.get('Enrich_Status__s'),
            'visa_eligible': contact.get('Visa_Eligible'),
            'utm_content': contact.get('UTM_Content'),
            'secondary_email': contact.get('Secondary_Email'),
            'signed_agreement': contact.get('Signed_Agreement'),
            'myinterview_url': contact.get('MyInterview_URL'),
            'interview_successful': contact.get('Interview_successful'),
            'skills': contact.get('Skills'),
            'link_to_cv': contact.get('Link_to_CV'),
            'accommodation_finalised': contact.get('Accommodation_finalised'),
            'utm_gclid': contact.get('UTM_GCLID'),
            't_c_link': contact.get('T_C_Link'),
            'agreement_finalised': contact.get('Agreement_finalised'),
            'industry_1_areas': contact.get('Industry_1_Areas'),
            'total': contact.get('Total'),
            'visa_owner': contact.get('Visa_Owner'),
            'visa_notes': contact.get('Visa_Note_s'),
            'house_rules': contact.get('House_rules'),
        }
        
        logger.info("Testing string fields...")
        for field_name, value in string_fields.items():
            logger.info(f"Field: {field_name}, Type: {type(value)}, Value: {value}")
            contact_data[field_name] = value
        
        # Now try to create the Contact object
        logger.info("Attempting to create Contact object...")
        try:
            new_contact = Contact(**contact_data)
            logger.info("Successfully created Contact object!")
        except Exception as e:
            logger.error(f"Failed to create Contact object: {str(e)}")
            # Try to identify which field is causing the issue
            for field_name, value in contact_data.items():
                try:
                    test_contact = Contact(**{field_name: value})
                except Exception as field_error:
                    logger.error(f"Field '{field_name}' with value '{value}' (type: {type(value)}) caused error: {str(field_error)}")
    
    else:
        logger.warning("No contacts retrieved from Zoho")

if __name__ == "__main__":
    debug_contacts_sync() 