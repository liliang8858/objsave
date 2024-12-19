@echo off
:: ObSave Service Starter
:: This script starts the ObSave service with the default configuration

:: Activate virtual environment if it exists
if exist "objectstorage_env\Scripts\activate.bat" (
    call objectstorage_env\Scripts\activate.bat
)

:: Run the service
python scripts/run.py %*

:: Deactivate virtual environment
if exist "objectstorage_env\Scripts\deactivate.bat" (
    call objectstorage_env\Scripts\deactivate.bat
)
