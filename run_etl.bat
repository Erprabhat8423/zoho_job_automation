@echo off
echo Starting ETL job in virtual environment...

REM Activate virtual environment
call beyond_venv\Scripts\activate.bat

REM Set environment variables (you can modify these as needed)
set MYSQL_USER=root
set MYSQL_PASSWORD=password
set MYSQL_HOST=localhost
set MYSQL_PORT=3306
set MYSQL_DB=beyond_academy

REM Run the ETL script
python scripts\run_etl.py

REM Deactivate virtual environment
deactivate

echo ETL job completed.
pause 