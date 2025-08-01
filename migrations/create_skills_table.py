#!/usr/bin/env python3
"""
Migration to create skills table for storing extracted skills from CVs
"""

import sys
import os
from datetime import datetime

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import engine, Base, Skill
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_skills_table():
    """Create the skills table"""
    try:
        logger.info("Creating skills table...")
        
        # Create the skills table
        Skill.__table__.create(engine, checkfirst=True)
        
        logger.info("Skills table created successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error creating skills table: {e}")
        return False

def verify_table_creation():
    """Verify that the skills table was created correctly"""
    try:
        with engine.connect() as connection:
            result = connection.execute(text("DESCRIBE skills"))
            columns = result.fetchall()
            
            logger.info("Skills table structure:")
            for column in columns:
                logger.info(f"  {column}")
                
        return True
        
    except Exception as e:
        logger.error(f"Error verifying skills table: {e}")
        return False

if __name__ == "__main__":
    logger.info("Starting skills table migration...")
    
    if create_skills_table():
        logger.info("Skills table migration completed successfully!")
        
        if verify_table_creation():
            logger.info("Skills table verification completed successfully!")
        else:
            logger.error("Skills table verification failed!")
    else:
        logger.error("Skills table migration failed!")
