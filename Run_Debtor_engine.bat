@echo off
REM Activate conda environment and run the script

CALL "C:\Users\<user>\miniconda3\Scripts\activate.bat" activate base
python "C:\card_data\system_transform\update_transaction_to_db.py"
python "C:\card_data\system_transform\app.py"
pause
