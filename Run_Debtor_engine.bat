@echo off
REM Activate the conda base environment from the current user profile, import new transactions, then start the web app.
REM Settings (database path, transaction folder) come from .env in this folder.
CALL "%USERPROFILE%\miniconda3\Scripts\activate.bat" base
cd /d "%~dp0"
python update_transaction_to_db.py
python app.py
pause
