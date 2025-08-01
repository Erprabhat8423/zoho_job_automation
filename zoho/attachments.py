#!/usr/bin/env python3
"""
Zoho CRM Attachments Module
Handles fetching and downloading contact attachments (CVs, resumes, etc.)
"""

import os
import re
import logging
import threading
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from .api_client import ZohoClient
from .auth import get_access_token

# Import database models for saving document mappings
try:
    from database.models import Session, Document
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False

logger = logging.getLogger(__name__)

# Import skill extractor
try:
    from .skill_extractor import SkillExtractor
    SKILL_EXTRACTION_AVAILABLE = True
    logger.info("Skill extractor loaded successfully")
except ImportError as e:
    SKILL_EXTRACTION_AVAILABLE = False
    logger.warning(f"Skill extractor not available: {e}")

class ZohoAttachmentManager:
    """Manages downloading and organizing contact attachments from Zoho CRM"""
    
    def __init__(self, download_dir: str = "downloads"):
        """
        Initialize the attachment manager
        
        Args:
            download_dir: Directory to store downloaded files
        """
        self.zoho_client = ZohoClient()
        self.download_dir = os.path.normpath(download_dir)
        self.ensure_download_directory()
        
        # Initialize skill extractor
        if SKILL_EXTRACTION_AVAILABLE:
            self.skill_extractor = SkillExtractor()
        else:
            self.skill_extractor = None
        
        # Patterns to identify CV/Resume files
        self.cv_patterns = [
            r'.*cv.*\.pdf$',
            r'.*resume.*\.pdf$', 
            r'.*curriculum.*vitae.*\.pdf$',
            r'.*bio.*\.pdf$',
            r'.*profile.*\.pdf$',
            r'.*portfolio.*\.pdf$'
        ]
    
    def ensure_download_directory(self):
        """Create download directory if it doesn't exist"""
        try:
            if not os.path.exists(self.download_dir):
                os.makedirs(self.download_dir, exist_ok=True)
                logger.info(f"Created download directory: {self.download_dir}")
        except Exception as e:
            logger.error(f"Failed to create download directory {self.download_dir}: {e}")
            # Fallback to current directory
            self.download_dir = os.path.normpath("downloads")
            if not os.path.exists(self.download_dir):
                os.makedirs(self.download_dir, exist_ok=True)
                logger.info(f"Created fallback download directory: {self.download_dir}")
    
    def get_contact_attachments(self, contact_id: str) -> List[Dict]:
        """
        Get all attachments for a specific contact
        
        Args:
            contact_id: Zoho contact ID
            
        Returns:
            List of attachment dictionaries
        """
        try:
            logger.info(f"Fetching attachments for contact {contact_id}")
            
            # Get access token
            access_token = get_access_token()
            if not access_token:
                logger.error("Failed to get access token")
                return []
            
            # API endpoint for contact attachments
            url = f"https://www.zohoapis.com/crm/v2/Contacts/{contact_id}/Attachments"
            
            headers = {
                'Authorization': f'Zoho-oauthtoken {access_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                attachments = data.get('data', [])
                logger.info(f"Found {len(attachments)} attachments for contact {contact_id}")
                return attachments
            elif response.status_code == 204:
                logger.info(f"No attachments found for contact {contact_id}")
                return []
            else:
                logger.error(f"Failed to fetch attachments: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching attachments for contact {contact_id}: {e}")
            return []
    
    def is_cv_file(self, filename: str) -> bool:
        """
        Check if a filename matches CV/resume patterns
        
        Args:
            filename: Name of the file
            
        Returns:
            True if filename appears to be a CV/resume
        """
        if not filename:
            return False
            
        filename_lower = filename.lower()
        
        # Check against CV patterns
        for pattern in self.cv_patterns:
            if re.match(pattern, filename_lower, re.IGNORECASE):
                return True
        
        # Additional checks for common CV indicators
        cv_keywords = ['cv', 'resume', 'curriculum', 'vitae', 'bio', 'profile', 'portfolio']
        for keyword in cv_keywords:
            if keyword in filename_lower and filename_lower.endswith('.pdf'):
                return True
        
        return False
    
    def filter_cv_attachments(self, attachments: List[Dict]) -> List[Dict]:
        """
        Filter attachments to find CV/resume files
        
        Args:
            attachments: List of attachment dictionaries
            
        Returns:
            List of CV/resume attachments
        """
        cv_attachments = []
        
        for attachment in attachments:
            filename = attachment.get('File_Name', '')
            file_type = attachment.get('$type', '')
            
            # Only process actual file attachments (not links)
            if file_type == 'Attachment' and self.is_cv_file(filename):
                cv_attachments.append(attachment)
                logger.info(f"Identified CV file: {filename}")
        
        return cv_attachments
    
    def download_attachment(self, contact_id: str, attachment_id: str, filename: str, contact_name: str = None, attachment_data: Dict = None) -> Optional[str]:
        """
        Download a specific attachment and save mapping to database
        
        Args:
            contact_id: Zoho contact ID
            attachment_id: Zoho attachment ID
            filename: Name of the file
            contact_name: Name of the contact (for organizing files)
            attachment_data: Full attachment data from Zoho API
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            logger.info(f"Downloading attachment {attachment_id} for contact {contact_id}")
            
            # Check if any document already exists for this contact
            if DATABASE_AVAILABLE:
                session = Session()
                try:
                    # Look for any existing document for this contact (prioritize CV/Resume types)
                    existing_doc = session.query(Document).filter(
                        Document.contact_id == contact_id,
                        Document.document_type.in_(['CV', 'Resume'])
                    ).first()
                    
                    # If no CV/Resume found, check for the specific document ID
                    if not existing_doc:
                        existing_doc = session.query(Document).filter(
                            Document.contact_id == contact_id,
                            Document.document_id == attachment_id
                        ).first()
                    
                    if existing_doc:
                        logger.info(f"Found existing document for contact {contact_id}: {existing_doc.document_name}")
                        
                        # Delete the old file if it exists
                        if existing_doc.file_path and os.path.exists(existing_doc.file_path):
                            try:
                                os.remove(existing_doc.file_path)
                                logger.info(f"Deleted old file: {existing_doc.file_path}")
                            except Exception as e:
                                logger.warning(f"Failed to delete old file {existing_doc.file_path}: {e}")
                        
                        # Delete existing skills for this document to re-extract them
                        if self.skill_extractor and self.is_cv_file(filename):
                            try:
                                from database.models import Skill
                                deleted_skills = session.query(Skill).filter(
                                    Skill.contact_id == contact_id,
                                    Skill.document_id == existing_doc.id
                                ).delete()
                                if deleted_skills > 0:
                                    logger.info(f"Deleted {deleted_skills} existing skills for re-extraction")
                                session.commit()
                            except Exception as e:
                                logger.warning(f"Error deleting existing skills: {e}")
                                session.rollback()
                        
                        # Continue with fresh download and update existing record
                        logger.info(f"Will update existing document record (ID: {existing_doc.id}) with new file")
                        
                except Exception as e:
                    logger.warning(f"Error checking existing document: {e}")
                finally:
                    session.close()
            
            # Get access token
            access_token = get_access_token()
            if not access_token:
                logger.error("Failed to get access token")
                return None
            
            # API endpoint for downloading attachment
            url = f"https://www.zohoapis.com/crm/v2/Contacts/{contact_id}/Attachments/{attachment_id}"
            
            headers = {
                'Authorization': f'Zoho-oauthtoken {access_token}'
            }
            
            response = requests.get(url, headers=headers, stream=True)
            
            if response.status_code == 200:
                # Create safe filename
                safe_filename = self.create_safe_filename(filename, contact_name, contact_id)
                file_path = os.path.join(self.download_dir, safe_filename)
                
                # Ensure the directory exists before writing
                directory = os.path.dirname(file_path)
                if not os.path.exists(directory):
                    os.makedirs(directory, exist_ok=True)
                    logger.info(f"Created directory: {directory}")
                
                # Download file
                file_size = 0
                try:
                    with open(file_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            file_size += len(chunk)
                    
                    logger.info(f"Successfully downloaded: {file_path} ({file_size} bytes)")
                except Exception as e:
                    logger.error(f"Error writing file {file_path}: {e}")
                    return None
                
                # Save document mapping to database
                db_document_id = self.save_document_mapping(
                    contact_id=contact_id,
                    document_id=attachment_id,
                    document_name=filename,
                    file_path=file_path,
                    file_size=file_size,
                    attachment_data=attachment_data
                )
                
                # Extract skills if this is a CV/Resume and we have skill extraction available
                if (db_document_id and self.skill_extractor and 
                    self.is_cv_file(filename) and filename.lower().endswith('.pdf')):
                    logger.info(f"Starting asynchronous skill extraction for {filename}")
                    # Run skill extraction in background thread to not block webhook response
                    # Force re-extraction since we may have deleted old skills
                    skill_thread = threading.Thread(
                        target=self._extract_skills_async,
                        args=(file_path, contact_id, db_document_id, filename),
                        daemon=True
                    )
                    skill_thread.start()
                    logger.info(f"Skill extraction thread started for {filename}")
                
                return file_path
            else:
                logger.error(f"Failed to download attachment: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading attachment {attachment_id}: {e}")
            return None
    
    def save_document_mapping(self, contact_id: str, document_id: str, document_name: str, 
                            file_path: str, file_size: int, attachment_data: Dict = None) -> Optional[int]:
        """
        Save document mapping to database
        
        Args:
            contact_id: Zoho contact ID
            document_id: Zoho attachment/document ID
            document_name: Original filename
            file_path: Local file path where downloaded
            file_size: File size in bytes
            attachment_data: Full attachment data from Zoho API
            
        Returns:
            Database document ID if successful, None otherwise
        """
        if not DATABASE_AVAILABLE:
            logger.warning("Database not available. Skipping document mapping save.")
            return None
        
        try:
            session = Session()
            
            # Determine document type based on filename
            document_type = self.determine_document_type(document_name)
            
            # Parse Zoho dates if available
            zoho_created_time = None
            zoho_modified_time = None
            
            if attachment_data:
                created_time_str = attachment_data.get('Created_Time')
                modified_time_str = attachment_data.get('Modified_Time')
                
                if created_time_str:
                    try:
                        zoho_created_time = datetime.fromisoformat(created_time_str.replace('Z', '+00:00'))
                    except:
                        pass
                        
                if modified_time_str:
                    try:
                        zoho_modified_time = datetime.fromisoformat(modified_time_str.replace('Z', '+00:00'))
                    except:
                        pass
            
            # Check if document already exists - prioritize by contact_id first for CV/Resume replacement
            existing_doc = None
            
            # First, look for any existing CV/Resume document for this contact (for replacement)
            if document_type in ['CV', 'Resume']:
                existing_doc = session.query(Document).filter(
                    Document.contact_id == contact_id,
                    Document.document_type.in_(['CV', 'Resume'])
                ).first()
                
                if existing_doc:
                    logger.info(f"Found existing CV/Resume for contact {contact_id}, will replace: {existing_doc.document_name}")
            
            # If no CV/Resume replacement found, check for exact document_id match
            if not existing_doc:
                existing_doc = session.query(Document).filter(
                    Document.contact_id == contact_id,
                    Document.document_id == document_id
                ).first()
            
            # If still not found, check for same filename to avoid duplicates
            if not existing_doc:
                existing_doc = session.query(Document).filter(
                    Document.contact_id == contact_id,
                    Document.document_name == document_name,
                    Document.document_type == document_type
                ).first()
                
                if existing_doc:
                    logger.info(f"Found existing document with same name and type for contact {contact_id}: {document_name}")
            
            db_document_id = None
            
            if existing_doc:
                # Update existing record with new information
                existing_doc.document_id = document_id  # Update with latest document_id
                existing_doc.document_name = document_name
                existing_doc.file_path = file_path  # Update with new file path
                existing_doc.file_size = file_size
                existing_doc.document_type = document_type
                existing_doc.zoho_created_time = zoho_created_time
                existing_doc.zoho_modified_time = zoho_modified_time
                existing_doc.download_date = datetime.now()  # Update download timestamp
                existing_doc.updated_at = datetime.now()
                db_document_id = existing_doc.id
                logger.info(f"Updated existing document mapping for {document_name} (ID: {db_document_id}) with new path: {file_path}")
            else:
                # Create new record only if no existing document found
                new_doc = Document(
                    contact_id=contact_id,
                    document_id=document_id,
                    document_name=document_name,
                    document_type=document_type,
                    file_path=file_path,
                    file_size=file_size,
                    zoho_created_time=zoho_created_time,
                    zoho_modified_time=zoho_modified_time
                )
                session.add(new_doc)
                session.flush()  # Flush to get the ID
                db_document_id = new_doc.id
                logger.info(f"Created new document mapping for {document_name} (ID: {db_document_id})")
            
            session.commit()
            return db_document_id
            
        except Exception as e:
            logger.error(f"Error saving document mapping: {e}")
            session.rollback()
            return None
        finally:
            session.close()
    
    def determine_document_type(self, filename: str) -> str:
        """
        Determine document type based on filename
        
        Args:
            filename: Name of the file
            
        Returns:
            Document type string
        """
        if not filename:
            return 'Unknown'
            
        filename_lower = filename.lower()
        
        if any(keyword in filename_lower for keyword in ['cv', 'curriculum', 'vitae']):
            return 'CV'
        elif 'resume' in filename_lower:
            return 'Resume'
        elif any(keyword in filename_lower for keyword in ['portfolio', 'work']):
            return 'Portfolio'
        elif any(keyword in filename_lower for keyword in ['certificate', 'cert']):
            return 'Certificate'
        elif any(keyword in filename_lower for keyword in ['cover', 'letter']):
            return 'Cover Letter'
        elif filename_lower.endswith('.pdf'):
            return 'PDF Document'
        else:
            return 'Document'
    
    def create_safe_filename(self, original_filename: str, contact_name: str = None, contact_id: str = None) -> str:
        """
        Create a safe filename for the downloaded file
        
        Args:
            original_filename: Original file name
            contact_name: Name of the contact
            contact_id: Contact ID
            
        Returns:
            Safe filename string
        """
        # Clean the original filename
        safe_original = re.sub(r'[<>:"/\\|?*]', '_', original_filename)
        
        # Create prefix from contact name or ID
        prefix = ""
        if contact_name:
            # Clean contact name
            clean_name = re.sub(r'[<>:"/\\|?*]', '_', contact_name)
            clean_name = re.sub(r'\s+', '_', clean_name)
            prefix = f"{clean_name}_"
        elif contact_id:
            prefix = f"contact_{contact_id}_"
        
        # Add timestamp to avoid duplicates
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Split filename and extension
        name, ext = os.path.splitext(safe_original)
        
        return f"{prefix}{name}_{timestamp}{ext}"
    
    def _extract_skills_async(self, file_path: str, contact_id: str, db_document_id: int, filename: str):
        """
        Extract skills asynchronously in a background thread
        
        Args:
            file_path: Path to the downloaded PDF file
            contact_id: Zoho contact ID
            db_document_id: Database document ID
            filename: Original filename for logging
        """
        try:
            logger.info(f"Background skill extraction started for {filename}")
            extracted_skills = self.skill_extractor.extract_and_save_skills(
                pdf_path=file_path,
                contact_id=contact_id,
                document_id=db_document_id
            )
            logger.info(f"Background skill extraction completed: {len(extracted_skills)} skills extracted from {filename}")
        except Exception as e:
            logger.error(f"Error in background skill extraction for {filename}: {e}")
    
    def download_contact_cvs(self, contact_id: str, contact_name: str = None) -> List[str]:
        """
        Download all CV attachments for a specific contact
        
        Args:
            contact_id: Zoho contact ID
            contact_name: Name of the contact
            
        Returns:
            List of paths to downloaded files
        """
        logger.info(f"Processing CV downloads for contact {contact_id} ({contact_name or 'Unknown'})")
        
        # Get all attachments
        attachments = self.get_contact_attachments(contact_id)
        if not attachments:
            logger.info(f"No attachments found for contact {contact_id}")
            return []
        
        # Filter for CV files
        cv_attachments = self.filter_cv_attachments(attachments)
        if not cv_attachments:
            logger.info(f"No CV files found for contact {contact_id}")
            return []
        
        downloaded_files = []
        
        # Download each CV file
        for attachment in cv_attachments:
            attachment_id = attachment['id']
            filename = attachment['File_Name']
            
            file_path = self.download_attachment(
                contact_id=contact_id, 
                attachment_id=attachment_id, 
                filename=filename, 
                contact_name=contact_name,
                attachment_data=attachment
            )
            if file_path:
                downloaded_files.append(file_path)
        
        logger.info(f"Downloaded {len(downloaded_files)} CV files for contact {contact_id}")
        return downloaded_files
    
    def get_attachment_info(self, attachment: Dict) -> Dict:
        """
        Extract useful information from an attachment
        
        Args:
            attachment: Attachment dictionary from Zoho API
            
        Returns:
            Dictionary with attachment information
        """
        return {
            'id': attachment.get('id'),
            'filename': attachment.get('File_Name'),
            'size': attachment.get('Size'),
            'type': attachment.get('$type'),
            'created_time': attachment.get('Created_Time'),
            'modified_time': attachment.get('Modified_Time'),
            'link_url': attachment.get('$link_url'),
            'file_id': attachment.get('$file_id'),
            'owner': attachment.get('Owner', {}).get('name'),
            'is_cv': self.is_cv_file(attachment.get('File_Name', ''))
        }
