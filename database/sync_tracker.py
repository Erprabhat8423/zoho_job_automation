from sqlalchemy import Column, String, Integer, DateTime, create_engine, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class SyncTracker(Base):
    __tablename__ = "sync_tracker"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_type = Column(String(50), nullable=False)  # 'contacts', 'accounts', 'intern_roles'
    last_sync_timestamp = Column(DateTime, nullable=True)  # Last Modified_Time processed
    records_synced = Column(Integer, default=0)  # Count of records processed in last sync
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    def __repr__(self):
        return f"<SyncTracker(entity_type='{self.entity_type}', last_sync_timestamp='{self.last_sync_timestamp}')>"
