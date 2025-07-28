# PowerShell script to run ETL in virtual environment
Write-Host "Starting ETL job in virtual environment..." -ForegroundColor Green

# Activate virtual environment
& ".\beyond_venv\Scripts\Activate.ps1"

# Set environment variables (you can modify these as needed)
$env:MYSQL_USER = "root"
$env:MYSQL_PASSWORD = "password"
$env:MYSQL_HOST = "localhost"
$env:MYSQL_PORT = "3306"
$env:MYSQL_DB = "beyond_academy"

# Run the ETL script
python scripts\run_etl.py

# Deactivate virtual environment
deactivate

Write-Host "ETL job completed." -ForegroundColor Green
Read-Host "Press Enter to continue" 