from sqlalchemy import Column, String, Integer, DateTime, create_engine, Text, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

Base = declarative_base()

# Sync tracking table
class SyncTracker(Base):
    __tablename__ = "sync_tracker"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_type = Column(String(50), nullable=False, unique=True)  # 'contacts', 'accounts', 'intern_roles'
    last_sync_timestamp = Column(DateTime, nullable=True)  # Last Modified_Time processed
    records_synced = Column(Integer, default=0)  # Count of records processed in last sync
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    def __repr__(self):
        return f"<SyncTracker(entity_type='{self.entity_type}', last_sync_timestamp='{self.last_sync_timestamp}')>"

class Contact(Base):
    __tablename__ = "contacts"
    id = Column(String(255), primary_key=True)
    email = Column(String(255))
    first_name = Column(String(255))
    last_name = Column(String(255))
    phone = Column(String(255))
    account_id = Column(String(255))  # Foreign key to Account
    title = Column(String(255))
    department = Column(String(255))
    updated_time = Column(DateTime)
    
    # New fields from the provided JSON data
    age_on_start_date = Column(Integer)
    timezone = Column(String(255))
    contact_last_name = Column(String(255))
    field_states = Column(Text)  # JSON as text
    arrival_drop_off_address = Column(Text)
    gender = Column(String(255))
    interview = Column(String(255))
    process_flow = Column(Boolean)
    end_date = Column(DateTime)
    role_owner = Column(String(255))
    paid_role = Column(String(255))
    role_success_notes = Column(Text)
    approval = Column(Text)  # JSON as text
    departure_date_time = Column(DateTime)
    approval_date = Column(DateTime)
    requires_a_visa = Column(String(255))
    contact_email = Column(String(255))
    follow_up_date = Column(DateTime)
    review_process = Column(Text)  # JSON as text
    admission_member = Column(String(255))
    english_level = Column(String(255))
    placement_status = Column(String(255))
    likelihood_to_convert = Column(String(255))
    role_success_stage = Column(String(255))
    call_to_conversion_time_days = Column(Integer)
    lead_created_time = Column(DateTime)
    university_name = Column(String(255))
    job_title = Column(String(255))
    layout_id = Column(String(255))
    layout_display_label = Column(String(255))
    layout_name = Column(String(255))
    intro_call_date = Column(DateTime)
    visa_alt_options = Column(Text)  # JSON array as text
    student_decision = Column(String(255))
    arrival_date_time = Column(DateTime)
    rating_new = Column(String(255))
    role_confirmed_date = Column(DateTime)
    start_date = Column(DateTime)
    last_activity_time = Column(DateTime)
    industry = Column(String(255))
    visa_f_u_date = Column(DateTime)
    location_other = Column(String(255))
    placement_lead_time_days = Column(Integer)
    graduation_date = Column(DateTime)
    other_payment_status = Column(Text)
    days_since_conversion = Column(Integer)
    name1 = Column(Text)
    average_no_of_days = Column(Integer)
    duration = Column(Text)
    warm_call = Column(Text)
    other_industry = Column(Text)
    call_scheduled_date_time = Column(DateTime)
    interviewer = Column(Text)
    visa_successful = Column(Text)
    utm_campaign = Column(Text)
    rating = Column(Text)  # JSON array as text
    alternative_location1 = Column(Text)
    enrolment_to_intro_call_lead_time = Column(Integer)
    review = Column(Text)
    reason_for_cancellation = Column(Text)
    cancelled_date_time = Column(DateTime)
    uni_start_date = Column(DateTime)
    notes1 = Column(Text)
    partner_organisation = Column(Text)
    date_of_birth = Column(DateTime)
    call_booked_date_time = Column(DateTime)
    date_of_cancellation = Column(DateTime)
    in_merge = Column(Boolean)
    approval_state = Column(Text)
    location = Column(Text)
    industry_choice_1 = Column(Text)
    industry_choice_3 = Column(Text)
    company_decision = Column(Text)
    student_bio = Column(Text)
    token = Column(Text)
    additional_information = Column(Text)
    placement_deadline = Column(DateTime)
    created_time = Column(DateTime)
    change_log_time = Column(DateTime)
    community_owner = Column(Text)
    created_by_email = Column(Text)
    current_location_v2 = Column(Text)
    decision_date = Column(DateTime)
    utm_medium = Column(Text)
    description = Column(Text)
    do_not_contact = Column(Boolean)
    industry_choice_2 = Column(Text)
    job_offered_after = Column(Text)
    full_name = Column(Text)
    ps_assigned_date = Column(DateTime)
    account_name = Column(Text)
    email_opt_out = Column(Boolean)
    books_cust_id = Column(Text)
    student_status = Column(Text)
    days_count = Column(Integer)
    record_status = Column(Text)
    nationality = Column(Text)
    type = Column(Text)
    cancellation_notes = Column(Text)
    departure_flight_number = Column(Text)
    locked = Column(Boolean)
    tag = Column(Text)  # JSON array as text
    last_enriched_time = Column(DateTime)
    country_city_of_residence = Column(Text)
    refund_date = Column(DateTime)
    visa_type_exemption = Column(Text)
    industry_2_areas = Column(Text)
    locked_for_me = Column(Boolean)
    from_university_partner = Column(Text)
    placement_urgency = Column(Text)
    enrich_status = Column(Text)
    visa_eligible = Column(Text)
    utm_content = Column(Text)
    cohort_start_date = Column(DateTime)
    secondary_email = Column(Text)
    is_duplicate = Column(Boolean)
    signed_agreement = Column(Text)
    myinterview_url = Column(Text)
    interview_successful = Column(Text)
    skills = Column(Text)
    link_to_cv = Column(Text)
    accommodation_finalised = Column(Text)
    send_mail2 = Column(Boolean)
    utm_gclid = Column(Text)
    unsubscribed_time = Column(DateTime)
    t_c_link = Column(Text)
    number_of_days = Column(Integer)
    agreement_finalised = Column(Text)
    end_date_auto_populated = Column(DateTime)
    industry_1_areas = Column(Text)
    total = Column(Text)
    visa_owner = Column(Text)
    visa_notes = Column(Text)
    house_rules = Column(Text)

class Account(Base):
    __tablename__ = "accounts"
    id = Column(String(255), primary_key=True)
    name = Column(String(255))
    industry = Column(String(255))
    billing_address = Column(Text)
    shipping_address = Column(Text)
    
    # New fields from Zoho API
    owner_id = Column(String(255))
    owner_name = Column(String(255))
    owner_email = Column(String(255))
    cleanup_start_date = Column(DateTime)
    field_states = Column(Text)  # JSON as text
    management_status = Column(String(255))
    company_work_policy = Column(Text)  # JSON array as text
    last_activity_time = Column(DateTime)
    last_full_due_diligence_date = Column(DateTime)
    company_industry = Column(String(255))
    company_description = Column(Text)
    process_flow = Column(Boolean)
    approval_status = Column(String(255))
    street = Column(Text)
    locked_for_me = Column(Boolean)
    classic_partnership = Column(String(255))
    state_region = Column(String(255))
    cleanup_status = Column(String(255))
    uni_region = Column(String(255))
    approval = Column(Text)  # JSON as text
    uni_outreach_status = Column(String(255))
    enrich_status = Column(String(255))
    roles_available = Column(Text)
    roles = Column(Text)
    city = Column(String(255))
    postcode = Column(String(255))
    outreach_notes = Column(Text)
    company_industry_other = Column(String(255))
    no_employees = Column(String(255))
    industry_areas = Column(Text)
    placement_revision_required = Column(String(255))
    country = Column(String(255))
    is_duplicate = Column(Boolean)
    uni_state_if_in_us = Column(String(255))
    follow_up_date = Column(DateTime)
    review_process = Column(Text)  # JSON as text
    layout_id = Column(String(255))
    layout_display_label = Column(String(255))
    layout_name = Column(String(255))
    review = Column(String(255))
    cleanup_notes = Column(Text)
    gold_rating = Column(Boolean)
    account_notes = Column(Text)
    standard_working_hours = Column(String(255))
    due_diligence_fields_to_revise = Column(Text)  # JSON array as text
    uni_country = Column(String(255))
    cleanup_phase = Column(String(255))
    next_reply_date = Column(DateTime)
    record_status = Column(String(255))
    type = Column(String(255))
    in_merge = Column(Boolean)
    uni_timezone = Column(String(255))
    upon_to_remote_interns = Column(Boolean)
    locked = Column(Boolean)
    company_address = Column(Text)
    tag = Column(Text)  # JSON array as text
    approval_state = Column(String(255))
    pathfinder = Column(Boolean)
    location = Column(String(255))
    location_other = Column(String(255))
    account_status = Column(String(255))

class InternRole(Base):
    __tablename__ = "intern_roles"
    id = Column(String(255), primary_key=True)
    
    # Basic role information
    name = Column(String(255))
    role_title = Column(String(255))
    role_description_requirements = Column(Text)
    role_status = Column(String(255))
    role_function = Column(String(255))
    role_department_size = Column(String(255))
    role_attachments_jd = Column(Text)  # JSON array as text
    role_tags = Column(Text)  # JSON array as text
    
    # Dates
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    created_time = Column(DateTime)
    
    # Company information
    intern_company_id = Column(String(255))
    intern_company_name = Column(String(255))
    company_work_policy = Column(Text)  # JSON array as text
    
    # Location and remote work
    location = Column(String(255))
    open_to_remote = Column(String(255))
    
    # Status and approval
    due_diligence_status_2 = Column(String(255))
    account_outreach_status = Column(String(255))
    record_status = Column(String(255))
    approval_state = Column(String(255))
    
    # Additional fields
    management_status = Column(String(255))
    placement_fields_to_revise = Column(Text)  # JSON array as text
    placement_revision_notes = Column(Text)
    gold_rating = Column(Boolean)
    locked = Column(Boolean)

# Database Setup with default values
def get_database_url():
    """Get database URL with default values for missing environment variables"""
    user = os.getenv('MYSQL_USER', 'root')
    password = os.getenv('MYSQL_PASSWORD', 'password')
    host = os.getenv('MYSQL_HOST', 'localhost')
    port = os.getenv('MYSQL_PORT', '3306')
    db = os.getenv('MYSQL_DB', 'beyond_academy')
    
    return f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"

class Document(Base):
    __tablename__ = "documents"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    contact_id = Column(String(255), nullable=False)  # Zoho contact ID
    document_id = Column(String(255), nullable=False)  # Zoho attachment/document ID
    document_name = Column(String(255), nullable=False)  # Original filename
    document_type = Column(String(50), nullable=False)  # 'CV', 'Resume', 'Portfolio', etc.
    file_path = Column(String(500), nullable=False)  # Local file path where downloaded
    file_size = Column(Integer)  # File size in bytes
    download_date = Column(DateTime, default=datetime.now)  # When downloaded
    zoho_created_time = Column(DateTime)  # When created in Zoho
    zoho_modified_time = Column(DateTime)  # When last modified in Zoho
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    def __repr__(self):
        return f"<Document(contact_id='{self.contact_id}', document_name='{self.document_name}', file_path='{self.file_path}')>"


class Skill(Base):
    __tablename__ = "skills"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    contact_id = Column(String(255), nullable=False)  # Zoho contact ID
    document_id = Column(Integer, nullable=False)  # Reference to document table
    skill_name = Column(String(255), nullable=False)  # Name of the skill
    skill_category = Column(String(100))  # Technical, Programming, Language, Soft Skill, etc.
    proficiency_level = Column(String(50))  # Beginner, Intermediate, Advanced, Expert
    years_experience = Column(String(50))  # Years of experience with the skill
    confidence_score = Column(Float)  # Confidence in extraction (0.0-1.0)
    extraction_method = Column(String(50), nullable=False)  # OpenAI GPT-3.5-turbo, Manual, etc.
    source_context = Column(Text)  # Context where skill was found
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    def __repr__(self):
        return f"<Skill(contact_id='{self.contact_id}', skill_name='{self.skill_name}', skill_category='{self.skill_category}')>"


DB_URL = get_database_url()
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)
