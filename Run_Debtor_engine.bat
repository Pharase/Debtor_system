@echo off
REM Activate conda environment and run the script

CALL "C:\Users\PAMC-NB-Alpha\miniconda3\Scripts\activate.bat" activate base
python "C:\Pam_card\system_transform\update_transaction_to_db.py"
python "C:\Pam_card\system_transform\app.py"
pause
