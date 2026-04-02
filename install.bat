@echo off
echo Installing required packages...
cd /d "%~dp0"
python -m pip install -r requirements.txt
echo.
echo Done! You can now run the script with:
echo python generate_data_dictionary.py your_file.sql
pause
