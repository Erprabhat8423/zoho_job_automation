#!/usr/bin/env python3
"""
Migration to create the documents table
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import Base, engine, Document
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_documents_table():
    """Create the documents table"""
    try:
        logger.info("Creating documents table...")
        
        # Create the table
        Document.__table__.create(engine, checkfirst=True)
        
        logger.info("Documents table created successfully!")
        
    except Exception as e:
        logger.error(f"Error creating documents table: {e}")
        raise

if __name__ == "__main__":
    create_documents_table()
