import os
import sys
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError
from database.models import Base, Contact, Account, InternRole, SyncTracker, get_database_url
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.engine = create_engine(get_database_url())
        self.inspector = inspect(self.engine)
    
    def table_exists(self, table_name):
        """Check if a table exists in the database"""
        try:
            return self.inspector.has_table(table_name)
        except Exception as e:
            logger.error(f"Error checking if table {table_name} exists: {e}")
            return False
    
    def create_table(self, model_class):
        """Create a table for the given model class"""
        table_name = model_class.__tablename__
        try:
            if not self.table_exists(table_name):
                logger.info(f"Creating table: {table_name}")
                model_class.__table__.create(self.engine, checkfirst=True)
                logger.info(f"Successfully created table: {table_name}")
                return True
            else:
                logger.info(f"Table {table_name} already exists")
                return False
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise
    
    def get_table_columns(self, table_name):
        """Get all columns for a table"""
        try:
            return [col['name'] for col in self.inspector.get_columns(table_name)]
        except Exception as e:
            logger.error(f"Error getting columns for table {table_name}: {e}")
            return []
    
    def add_column(self, table_name, column_name, column_type):
        """Add a new column to an existing table"""
        try:
            with self.engine.connect() as conn:
                # Get the SQL type string
                if hasattr(column_type, '__str__'):
                    type_str = str(column_type)
                else:
                    type_str = column_type

                # Enforce TEXT for all String-based types to avoid row size issues
                if 'String' in type_str or 'VARCHAR' in type_str:
                    sql_type = 'TEXT'
                elif 'TEXT' in type_str:
                    sql_type = 'TEXT'
                elif 'INTEGER' in type_str or 'Integer' in type_str:
                    sql_type = 'INTEGER'
                elif 'DATETIME' in type_str:
                    sql_type = 'DATETIME'
                elif 'BOOLEAN' in type_str:
                    sql_type = 'BOOLEAN'
                elif 'FLOAT' in type_str:
                    sql_type = 'FLOAT'
                else:
                    sql_type = 'TEXT'  # Default fallback

                alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_type}"
                conn.execute(text(alter_sql))
                conn.commit()
                logger.info(f"Successfully added column {column_name} to table {table_name}")
                return True
        except Exception as e:
            logger.error(f"Error adding column {column_name} to table {table_name}: {e}")
            return False

    
    def sync_table_schema(self, model_class):
        """Sync table schema with model definition"""
        table_name = model_class.__tablename__
        
        # First, ensure table exists
        if not self.table_exists(table_name):
            self.create_table(model_class)
            return True
        
        # Get existing columns
        existing_columns = self.get_table_columns(table_name)
        
        # Get model columns
        model_columns = [column.name for column in model_class.__table__.columns]
        
        # Find missing columns
        missing_columns = [col for col in model_columns if col not in existing_columns]
        
        if missing_columns:
            logger.info(f"Found {len(missing_columns)} missing columns in {table_name}: {missing_columns}")
            
            for column_name in missing_columns:
                # Find the column definition in the model
                column_def = model_class.__table__.columns[column_name]
                self.add_column(table_name, column_name, column_def.type)
            
            return True
        else:
            logger.info(f"Table {table_name} schema is up to date")
            return False
    
    def ensure_all_tables_exist(self):
        """Ensure all tables exist and are up to date"""
        models = [Contact, Account, InternRole, SyncTracker]
        
        logger.info("Starting database schema check and update...")
        
        for model in models:
            try:
                self.sync_table_schema(model)
            except Exception as e:
                logger.error(f"Error syncing schema for {model.__name__}: {e}")
                raise
        
        logger.info("Database schema check and update completed successfully")

def run_migrations():
    """Run database migrations"""
    try:
        db_manager = DatabaseManager()
        db_manager.ensure_all_tables_exist()
        return True
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False

if __name__ == "__main__":
    success = run_migrations()
    if success:
        print("Database migrations completed successfully")
    else:
        print("Database migrations failed")
        sys.exit(1) 