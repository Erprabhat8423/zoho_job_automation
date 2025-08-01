"""
Skill extraction module for CV processing using OpenAI
"""
import os
import logging
from typing import List, Dict, Optional
import PyPDF2
import openai
from database.models import Session, Skill

logger = logging.getLogger(__name__)

class SkillExtractor:
    """Handles skill extraction from CV PDF files using OpenAI"""
    
    def __init__(self):
        """Initialize the skill extractor with OpenAI API key"""
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        if not self.openai_api_key:
            logger.warning("OPENAI_API_KEY not found in environment variables")
            raise ValueError("OpenAI API key is required for skill extraction")
        
        openai.api_key = self.openai_api_key
        logger.info("SkillExtractor initialized successfully")
    
    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        Extract text content from PDF file
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Extracted text content
        """
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    text += page.extract_text() + "\n"
                
                logger.info(f"Successfully extracted {len(text)} characters from {pdf_path}")
                return text.strip()
                
        except Exception as e:
            logger.error(f"Error extracting text from PDF {pdf_path}: {e}")
            return ""
    
    def extract_skills_with_openai(self, cv_text: str) -> List[Dict[str, str]]:
        """
        Extract skills from CV text using OpenAI
        
        Args:
            cv_text: Text content of the CV
            
        Returns:
            List of skill dictionaries with name, category, and proficiency
        """
        try:
            prompt = f"""
            Analyze the following CV text and extract all technical skills, soft skills, and competencies.
            For each skill, provide:
            1. skill_name: The name of the skill
            2. category: Category (Technical, Programming, Language, Soft Skill, Tool/Software, Domain Knowledge, etc.)
            3. proficiency_level: Estimated proficiency (Beginner, Intermediate, Advanced, Expert) based on context
            
            Format the response as a JSON array of objects with these exact fields: skill_name, category, proficiency_level
            
            CV Text:
            {cv_text[:4000]}  # Limit to first 4000 characters to stay within token limits
            
            Respond with only the JSON array, no additional text.
            """
            
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are an expert HR assistant that extracts skills from CVs. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1500,
                temperature=0.3
            )
            
            # Parse the response
            response_text = response.choices[0].message.content.strip()
            logger.info(f"OpenAI response received: {len(response_text)} characters")
            logger.debug(f"OpenAI raw response: {response_text[:500]}...")  # Log first 500 chars for debugging
            
            # Try to parse as JSON
            import json
            try:
                skills_data = json.loads(response_text)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse OpenAI JSON response: {e}")
                logger.error(f"Raw response (first 1000 chars): {response_text[:1000]}")
                
                # Try to extract JSON from the response if it's wrapped in other text
                import re
                json_match = re.search(r'\[.*\]', response_text, re.DOTALL)
                if json_match:
                    try:
                        skills_data = json.loads(json_match.group())
                        logger.info("Successfully extracted JSON from wrapped response")
                    except json.JSONDecodeError:
                        logger.error("Failed to extract JSON even from matched array")
                        return []
                else:
                    logger.error("No JSON array found in response")
                    return []
            
            if not isinstance(skills_data, list):
                logger.warning("OpenAI response is not a list, attempting to extract array")
                return []
            
            # Validate the structure
            valid_skills = []
            for skill in skills_data:
                if isinstance(skill, dict) and 'skill_name' in skill:
                    valid_skills.append({
                        'skill_name': skill.get('skill_name', '').strip(),
                        'category': skill.get('category', 'Other').strip(),
                        'proficiency_level': skill.get('proficiency_level', 'Intermediate').strip()
                    })
            
            logger.info(f"Successfully extracted {len(valid_skills)} skills from CV")
            return valid_skills
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse OpenAI JSON response: {e}")
            return []
        except Exception as e:
            logger.error(f"Error extracting skills with OpenAI: {e}")
            return []
    
    def save_skills_to_database(self, skills: List[Dict[str, str]], contact_id: str, document_id: int) -> List[int]:
        """
        Save extracted skills to the database
        
        Args:
            skills: List of skill dictionaries
            contact_id: Zoho contact ID
            document_id: Database document ID
            
        Returns:
            List of created skill IDs
        """
        session = Session()
        created_skill_ids = []
        
        try:
            for skill_data in skills:
                if not skill_data.get('skill_name'):
                    continue
                
                # Check if skill already exists for this contact and document
                existing_skill = session.query(Skill).filter_by(
                    contact_id=contact_id,
                    document_id=document_id,
                    skill_name=skill_data['skill_name']
                ).first()
                
                if existing_skill:
                    logger.info(f"Skill '{skill_data['skill_name']}' already exists for contact {contact_id}")
                    continue
                
                # Create new skill record
                skill = Skill(
                    contact_id=contact_id,
                    document_id=document_id,
                    skill_name=skill_data['skill_name'],
                    skill_category=skill_data.get('category', 'Other'),
                    proficiency_level=skill_data.get('proficiency_level', 'Intermediate'),
                    extraction_method='OpenAI GPT-3.5-turbo',
                    confidence_score=0.8  # Default confidence for OpenAI extraction
                )
                
                session.add(skill)
                session.flush()  # Get the ID
                created_skill_ids.append(skill.id)
                
                logger.info(f"Created skill: {skill_data['skill_name']} (ID: {skill.id})")
            
            session.commit()
            logger.info(f"Successfully saved {len(created_skill_ids)} skills to database")
            return created_skill_ids
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving skills to database: {e}")
            return []
        finally:
            session.close()
    
    def extract_and_save_skills(self, pdf_path: str, contact_id: str, document_id: int) -> List[int]:
        """
        Complete workflow: extract text from PDF, extract skills with OpenAI, save to database
        
        Args:
            pdf_path: Path to the PDF file
            contact_id: Zoho contact ID
            document_id: Database document ID
            
        Returns:
            List of created skill IDs
        """
        logger.info(f"Starting skill extraction workflow for {pdf_path}")
        
        # Extract text from PDF
        cv_text = self.extract_text_from_pdf(pdf_path)
        if not cv_text:
            logger.warning(f"No text extracted from {pdf_path}")
            return []
        
        # Extract skills using OpenAI
        skills = self.extract_skills_with_openai(cv_text)
        if not skills:
            logger.warning(f"No skills extracted from {pdf_path}")
            return []
        
        # Save skills to database
        skill_ids = self.save_skills_to_database(skills, contact_id, document_id)
        
        logger.info(f"Completed skill extraction workflow: {len(skill_ids)} skills saved")
        return skill_ids
