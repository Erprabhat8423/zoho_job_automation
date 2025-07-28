# Beyond Academy ETL Pipeline

This project contains an ETL (Extract, Transform, Load) pipeline for syncing data from Zoho CRM to a MySQL database.

## Features

- **Automatic Database Schema Management**: The system automatically checks for table existence and creates tables if they don't exist
- **Schema Migration**: When new fields are added to models, the system automatically adds the corresponding columns to existing tables
- **Virtual Environment Support**: All scripts are designed to run in the `beyond_venv` virtual environment
- **Comprehensive Logging**: Detailed logging of all operations with both file and console output
- **Error Handling**: Robust error handling with proper rollback mechanisms

## Prerequisites

- Python 3.8+
- MySQL Server
- Virtual environment (`beyond_venv`)

## Installation

1. **Clone the repository** (if not already done)
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**:
   - Copy `config.env.example` to `.env`
   - Update the values in `.env` with your actual configuration

## Configuration

### Environment Variables

Create a `.env` file in the project root with the following variables:

```env
# Database Configuration
MYSQL_USER=root
MYSQL_PASSWORD=password
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DB=beyond_academy

# Zoho API Configuration
ZOHO_CLIENT_ID=your_zoho_client_id
ZOHO_CLIENT_SECRET=your_zoho_client_secret
ZOHO_REFRESH_TOKEN=your_zoho_refresh_token

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=etl.log
```

## Running the ETL Pipeline

### Option 1: Using Batch Script (Windows)
```bash
run_etl.bat
```

### Option 2: Using PowerShell Script (Windows)
```powershell
.\run_etl.ps1
```

### Option 3: Manual Execution
```bash
# Activate virtual environment
beyond_venv\Scripts\activate

# Set environment variables (if not using .env file)
set MYSQL_USER=root
set MYSQL_PASSWORD=password
set MYSQL_HOST=localhost
set MYSQL_PORT=3306
set MYSQL_DB=beyond_academy

# Run the ETL script
python scripts\run_etl.py
```

## How It Works

### 1. Database Schema Check
When the ETL pipeline starts, it first checks if all required tables exist:
- `contacts` table
- `accounts` table  
- `intern_roles` table

If any table doesn't exist, it creates the table with the proper schema.

### 2. Schema Migration
The system compares the current model definitions with the existing database schema:
- If new fields are found in the models but not in the database tables, it automatically adds the missing columns
- This ensures that schema changes in the code are automatically reflected in the database

### 3. Data Synchronization
After ensuring the database schema is up to date, the system:
- Extracts data from Zoho CRM API
- Transforms the data according to the model definitions
- Loads the data into the MySQL database with proper error handling

## Database Models

### Contact
- Basic contact information (name, email, phone, etc.)
- Links to accounts via `account_id`

### Account
- Company/organization information
- Extensive fields for business data
- Support for JSON fields stored as TEXT

### InternRole
- Internship role information
- Links to contacts and accounts
- Comprehensive role details and requirements

## Logging

The system provides comprehensive logging:
- **File Logging**: All logs are saved to `etl.log`
- **Console Logging**: Real-time output to console
- **Log Levels**: INFO, WARNING, ERROR levels supported

## Error Handling

- **Database Errors**: Proper rollback on database errors
- **API Errors**: Graceful handling of Zoho API failures
- **Schema Errors**: Detailed error messages for migration issues

## Adding New Fields

To add new fields to existing models:

1. **Update the model** in `database/models.py`
2. **Run the ETL pipeline** - it will automatically detect and add the new columns
3. **No manual database changes required**

Example:
```python
class Contact(Base):
    __tablename__ = "contacts"
    # ... existing fields ...
    new_field = Column(String)  # New field will be auto-added
```

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check MySQL server is running
   - Verify database credentials in `.env` file
   - Ensure database exists

2. **Virtual Environment Issues**
   - Make sure `beyond_venv` is properly created
   - Activate the virtual environment before running scripts

3. **Missing Dependencies**
   - Run `pip install -r requirements.txt` in the virtual environment

4. **Permission Issues**
   - Ensure the MySQL user has CREATE, ALTER, INSERT, UPDATE permissions

### Log Files

Check `etl.log` for detailed error information and debugging.

## Project Structure

```
Beyond_academy/
├── beyond_venv/           # Virtual environment
├── config/                # Configuration files
├── database/              # Database models and migrations
│   ├── models.py         # SQLAlchemy models
│   └── migrations.py     # Database migration system
├── etl/                  # ETL pipeline
│   └── pipeline.py       # Main ETL functions
├── scripts/              # Scripts
│   └── run_etl.py       # Main ETL runner
├── zoho/                 # Zoho API integration
├── run_etl.bat          # Windows batch script
├── run_etl.ps1          # PowerShell script
├── config.env.example    # Environment variables template
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## Support

For issues or questions, check the log files and ensure all prerequisites are met.
