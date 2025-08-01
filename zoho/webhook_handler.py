#!/usr/bin/env python3
"""
Zoho CRM Webhook Handler
Handles incoming webhook notifications from Zoho CRM and triggers appropriate actions
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify
import hmac
import hashlib
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from zoho.attachments import ZohoAttachmentManager
from zoho.api_client import ZohoClient
from database.models import Session, Contact
from etl.pipeline import sync_contacts

logger = logging.getLogger(__name__)


class ZohoWebhookHandler:
    """Handles Zoho CRM webhook notifications"""
    
    def __init__(self):
        """Initialize the webhook handler"""
        # Get configuration from environment variables
        cv_download_dir = os.getenv('CV_DOWNLOAD_DIR', 'downloads')
        self.attachment_manager = ZohoAttachmentManager(download_dir=cv_download_dir)
        self.webhook_secret = os.getenv('WEBHOOK_SECRET', 'your_webhook_secret_key_here')
        self.zoho_client = ZohoClient()
        
    def verify_webhook_signature(self, payload: str, signature: str) -> bool:
        """
        Verify the webhook signature from Zoho
        
        Args:
            payload: Raw webhook payload
            signature: Signature from Zoho webhook headers
            
        Returns:
            True if signature is valid
        """
        try:
            # Calculate expected signature
            expected_signature = hmac.new(
                self.webhook_secret.encode(),
                payload.encode(),
                hashlib.sha256
            ).hexdigest()
            
            # Compare signatures
            return hmac.compare_digest(signature, expected_signature)
        except Exception as e:
            logger.error(f"Error verifying webhook signature: {e}")
            return False
    
    def process_contact_update(self, webhook_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process contact update webhook notification
        
        Args:
            webhook_data: Webhook payload data
            
        Returns:
            Processing result dictionary
        """
        try:
            logger.info("Processing contact update webhook")
            
            # Extract contact information from webhook
            contact_info = self.extract_contact_info(webhook_data)
            if not contact_info:
                return {'status': 'error', 'message': 'Failed to extract contact info'}
            
            # Log extracted contact info for debugging
            logger.info(f"Extracted contact info: {json.dumps(contact_info, indent=2)}")
            
            contact_id = contact_info.get('id')
            role_success_stage = contact_info.get('role_success_stage')
            contact_name = contact_info.get('name')
            
            # If role_success_stage is not available or contact name is Unknown, 
            # fetch complete contact details from Zoho
            if not role_success_stage or contact_name == 'Unknown':
                logger.info(f"Fetching complete contact details from Zoho for contact {contact_id}")
                complete_contact_data = self.zoho_client.get_contact_by_id(contact_id)
                
                if complete_contact_data:
                    # Update contact_info with complete data
                    complete_contact_info = self.extract_contact_info(complete_contact_data)
                    if complete_contact_info:
                        # Merge the data, preferring complete data over webhook data
                        for key, value in complete_contact_info.items():
                            if value and (not contact_info.get(key) or contact_info.get(key) == 'Unknown'):
                                contact_info[key] = value
                        
                        role_success_stage = contact_info.get('role_success_stage')
                        contact_name = contact_info.get('name')
                        logger.info(f"Updated contact info with complete data: {json.dumps(contact_info, indent=2)}")
                else:
                    logger.warning(f"Failed to fetch complete contact details for {contact_id}")
            
            logger.info(f"Contact {contact_id} ({contact_name}) - Role Success Stage: {role_success_stage}")
            
            # Check if role_success_stage is "Ready to Pitch"
            if role_success_stage == "Ready to Pitch":
                logger.info(f"Triggering CV download for contact {contact_id} - Ready to Pitch!")
                
                # Update contact in local database first
                self.update_local_contact(contact_info)
                
                # Download CV files
                downloaded_files = self.attachment_manager.download_contact_cvs(contact_id, contact_name)
                
                result = {
                    'status': 'success',
                    'message': f'Processed Ready to Pitch trigger for {contact_name}',
                    'contact_id': contact_id,
                    'contact_name': contact_name,
                    'downloaded_files': downloaded_files,
                    'files_count': len(downloaded_files)
                }
                
                logger.info(f"CV download completed: {len(downloaded_files)} files downloaded")
                return result
            else:
                # Just update the local database
                self.update_local_contact(contact_info)
                
                return {
                    'status': 'success',
                    'message': f'Contact updated but no CV download triggered (stage: {role_success_stage})',
                    'contact_id': contact_id,
                    'contact_name': contact_name
                }
                
        except Exception as e:
            logger.error(f"Error processing contact update webhook: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def extract_contact_info(self, webhook_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract contact information from webhook payload
        
        Args:
            webhook_data: Raw webhook data
            
        Returns:
            Extracted contact information or None
        """
        try:
            logger.info(f"Extracting contact info from webhook data: {json.dumps(webhook_data, indent=2)}")
            
            # Zoho webhook structure may vary, adjust as needed
            if 'data' in webhook_data:
                data = webhook_data['data']
                if isinstance(data, list) and len(data) > 0:
                    contact_data = data[0]
                elif isinstance(data, dict):
                    contact_data = data
                else:
                    logger.error("Unexpected webhook data structure")
                    return None
            else:
                contact_data = webhook_data
            
            # Handle the case where webhook just contains IDs (like "ids": "123456789")
            # and we need to extract the contact ID from the IDs field
            contact_id = None
            if 'ids' in webhook_data and isinstance(webhook_data['ids'], str):
                contact_id = webhook_data['ids']
                logger.info(f"Found contact ID in 'ids' field: {contact_id}")
            elif 'id' in contact_data:
                contact_id = contact_data.get('id') or contact_data.get('ID')
            
            # Extract relevant fields - handle both JSON and form-encoded formats
            contact_info = {
                'id': contact_id,
                'name': self.get_contact_full_name(contact_data),
                'first_name': (contact_data.get('First_Name') or 
                              contact_data.get('first_name') or 
                              contact_data.get('firstName')),
                'last_name': (contact_data.get('Last_Name') or 
                             contact_data.get('last_name') or 
                             contact_data.get('lastName')),
                'email': (contact_data.get('Email') or 
                         contact_data.get('email')),
                'role_success_stage': (contact_data.get('Role_Success_Stage') or
                                     contact_data.get('role_success_stage') or
                                     contact_data.get('Role_Success_Stage__c') or
                                     contact_data.get('stage')),
                'phone': (contact_data.get('Phone') or 
                         contact_data.get('phone')),
                'modified_time': (contact_data.get('Modified_Time') or
                                contact_data.get('modified_time') or
                                contact_data.get('updatedTime'))
            }
            
            logger.info(f"Extracted contact info: {json.dumps(contact_info, indent=2)}")
            return contact_info
            
        except Exception as e:
            logger.error(f"Error extracting contact info: {e}")
            return None
    
    def get_contact_full_name(self, contact_data: Dict[str, Any]) -> str:
        """
        Get full name from contact data
        
        Args:
            contact_data: Contact data dictionary
            
        Returns:
            Full name string
        """
        # Handle both JSON and form-encoded formats
        first_name = (contact_data.get('First_Name') or 
                     contact_data.get('first_name') or 
                     contact_data.get('firstName') or '')
        last_name = (contact_data.get('Last_Name') or 
                    contact_data.get('last_name') or 
                    contact_data.get('lastName') or '')
        
        # Try Full_Name field if available
        full_name = (contact_data.get('Full_Name') or 
                    contact_data.get('name') or 
                    contact_data.get('fullName'))
        if full_name:
            return full_name
        
        # Construct from first and last name
        name_parts = [name for name in [first_name, last_name] if name]
        return ' '.join(name_parts) if name_parts else 'Unknown'
    
    def update_local_contact(self, contact_info: Dict[str, Any]) -> bool:
        """
        Update contact in local database
        
        Args:
            contact_info: Contact information dictionary
            
        Returns:
            True if update was successful
        """
        try:
            session = Session()
            
            # Find existing contact
            contact = session.query(Contact).filter(Contact.id == contact_info['id']).first()
            
            if contact:
                # Update existing contact
                if contact_info.get('role_success_stage'):
                    contact.role_success_stage = contact_info['role_success_stage']
                if contact_info.get('first_name'):
                    contact.first_name = contact_info['first_name']
                if contact_info.get('last_name'):
                    contact.last_name = contact_info['last_name']
                if contact_info.get('email'):
                    contact.email = contact_info['email']
                if contact_info.get('phone'):
                    contact.phone = contact_info['phone']
                
                # Update modified time
                contact.updated_time = datetime.now()
                
                session.commit()
                logger.info(f"Updated local contact {contact_info['id']}")
                return True
            else:
                logger.warning(f"Contact {contact_info['id']} not found in local database")
                # Optionally trigger a sync to get the contact
                return False
                
        except Exception as e:
            logger.error(f"Error updating local contact: {e}")
            session.rollback()
            return False
        finally:
            session.close()

# Flask application for webhook endpoint
app = Flask(__name__)
webhook_handler = ZohoWebhookHandler()

@app.route('/webhook/zoho/contact', methods=['POST'])
def handle_contact_webhook():
    """Handle Zoho CRM contact webhook notifications"""
    try:
        # Get raw payload for signature verification
        raw_payload = request.get_data(as_text=True)
        print(f"Raw payload: {raw_payload}")
        print(f"Content-Type: {request.content_type}")
        print(f"Headers: {dict(request.headers)}")
    
        # Parse payload based on content type
        webhook_data = None
        content_type = request.content_type or ''

        # Try to parse JSON from raw payload first
        try:
            webhook_data = json.loads(raw_payload)
            logger.info("Successfully parsed JSON from raw payload")
        except json.JSONDecodeError:
            # If raw payload isn't JSON, try form data
            if 'application/x-www-form-urlencoded' in content_type:
                form_data = request.form
                if 'data' in form_data:
                    try:
                        webhook_data = json.loads(form_data['data'])
                        logger.info("Successfully parsed JSON from form data")
                    except json.JSONDecodeError:
                        webhook_data = dict(form_data)
                        logger.info("Parsed as form data dictionary")
                else:
                    webhook_data = dict(form_data)
                    logger.info("Parsed as form data dictionary")
            else:
                # If all else fails, create a simple data structure
                webhook_data = {'raw_data': raw_payload, 'content_type': content_type}
                logger.info("Stored as raw data with content type info")
        
        if not webhook_data:
            logger.error("No data in webhook request")
            return jsonify({'error': 'No data found'}), 400
        
        logger.info(f"Received webhook: {json.dumps(webhook_data, indent=2)}")
        logger.info(f"Webhook data keys: {list(webhook_data.keys())}")
        
        # Process the webhook
        result = webhook_handler.process_contact_update(webhook_data)
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error handling webhook: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/webhook/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()}), 200

@app.route('/webhook/test-cv-download/<contact_id>', methods=['POST'])
def test_cv_download(contact_id):
    """Test endpoint to manually trigger CV download for a contact"""
    try:
        # Get contact from database
        session = Session()
        contact = session.query(Contact).filter(Contact.id == contact_id).first()
        
        if not contact:
            return jsonify({'error': f'Contact {contact_id} not found'}), 404
        
        contact_name = f"{contact.first_name or ''} {contact.last_name or ''}".strip()
        
        # Download CVs
        downloaded_files = webhook_handler.attachment_manager.download_contact_cvs(contact_id, contact_name)
        
        return jsonify({
            'status': 'success',
            'contact_id': contact_id,
            'contact_name': contact_name,
            'downloaded_files': downloaded_files,
            'files_count': len(downloaded_files)
        }), 200
        
    except Exception as e:
        logger.error(f"Error in test CV download: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/webhook/test-skill-extraction/<contact_id>', methods=['POST'])
def test_skill_extraction(contact_id):
    """Test endpoint to manually trigger skill extraction for a contact's documents"""
    try:
        from database.models import Document, Skill
        
        # Get contact from database
        session = Session()
        contact = session.query(Contact).filter(Contact.id == contact_id).first()
        
        if not contact:
            return jsonify({'error': f'Contact {contact_id} not found'}), 404
        
        # Get all documents for this contact
        documents = session.query(Document).filter(Document.contact_id == contact_id).all()
        
        if not documents:
            return jsonify({'error': f'No documents found for contact {contact_id}'}), 404
        
        results = []
        for doc in documents:
            if doc.document_type in ['CV', 'Resume'] and doc.file_path.lower().endswith('.pdf'):
                try:
                    logger.info(f"Testing skill extraction for document: {doc.document_name}")
                    
                    # Check if file exists
                    if not os.path.exists(doc.file_path):
                        logger.warning(f"File not found: {doc.file_path}")
                        continue
                    
                    # Extract skills
                    skill_ids = webhook_handler.attachment_manager.skill_extractor.extract_and_save_skills(
                        pdf_path=doc.file_path,
                        contact_id=contact_id,
                        document_id=doc.id
                    )
                    
                    # Get the extracted skills
                    skills = session.query(Skill).filter(
                        Skill.contact_id == contact_id,
                        Skill.document_id == doc.id
                    ).all()
                    
                    results.append({
                        'document_id': doc.id,
                        'document_name': doc.document_name,
                        'skills_extracted': len(skill_ids),
                        'total_skills': len(skills),
                        'skills': [{'skill_name': s.skill_name, 'category': s.skill_category, 'proficiency': s.proficiency_level} for s in skills[-10:]]  # Last 10 skills
                    })
                    
                except Exception as e:
                    logger.error(f"Error extracting skills from {doc.document_name}: {e}")
                    results.append({
                        'document_id': doc.id,
                        'document_name': doc.document_name,
                        'error': str(e)
                    })
        
        return jsonify({
            'status': 'success',
            'contact_id': contact_id,
            'contact_name': f"{contact.first_name or ''} {contact.last_name or ''}".strip(),
            'documents_processed': len(results),
            'results': results
        }), 200
        
    except Exception as e:
        logger.error(f"Error in test skill extraction: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/webhook/skills/<contact_id>', methods=['GET'])
def get_contact_skills(contact_id):
    """Get all extracted skills for a contact"""
    try:
        from database.models import Skill
        
        session = Session()
        
        # Get all skills for this contact
        skills = session.query(Skill).filter(Skill.contact_id == contact_id).all()
        
        if not skills:
            return jsonify({'message': f'No skills found for contact {contact_id}', 'skills': []}), 200
        
        skills_data = []
        for skill in skills:
            skills_data.append({
                'id': skill.id,
                'skill_name': skill.skill_name,
                'category': skill.skill_category,
                'proficiency_level': skill.proficiency_level,
                'years_experience': skill.years_experience,
                'confidence_score': skill.confidence_score,
                'extraction_method': skill.extraction_method,
                'created_at': skill.created_at.isoformat() if skill.created_at else None,
                'document_id': skill.document_id
            })
        
        return jsonify({
            'status': 'success',
            'contact_id': contact_id,
            'total_skills': len(skills_data),
            'skills': skills_data
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting skills for contact {contact_id}: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run the Flask app with configuration from .env
    port = int(os.getenv('WEBHOOK_PORT', 5000))
    debug_mode = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    print(f"Starting webhook server on port {port}")
    print(f"Debug mode: {debug_mode}")
    print(f"CV download directory: {os.getenv('CV_DOWNLOAD_DIR', 'downloads')}")
    
    app.run(host='0.0.0.0', port=port, debug=debug_mode)
