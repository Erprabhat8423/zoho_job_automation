from sqlalchemy import Column, String, Integer, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

Base = declarative_base()

class Contact(Base):
    __tablename__ = "contacts"
    id = Column(String, primary_key=True)
    email = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    phone = Column(String)
    account_id = Column(String)  # Foreign key to Account
    title = Column(String)
    department = Column(String)
    updated_time = Column(DateTime)

class Account(Base):
    __tablename__ = "accounts"
    id = Column(String, primary_key=True)
    name = Column(String)
    industry = Column(String)
    website = Column(String)
    phone = Column(String)
    billing_address = Column(String)
    shipping_address = Column(String)
    updated_time = Column(DateTime)

class InternRole(Base):
    __tablename__ = "intern_roles"
    id = Column(String, primary_key=True)
    contact_id = Column(String)  # Foreign key to Contact
    role_title = Column(String)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    status = Column(String)
    updated_time = Column(DateTime)

# Database Setup
DB_URL = f"mysql+mysqlconnector://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)
