@echo off
REM Run script for MASX AI ETL CPU Pipeline
REM Provides a convenient way to start the FastAPI server on Windows

setlocal enabledelayedexpansion

echo.
echo 🚀 MASX AI ETL CPU Pipeline
echo ================================================

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python is not installed or not in PATH
    echo Please install Python 3.12+ and try again
    pause
    exit /b 1
)

REM Check Python version
for /f "tokens=2" %%i in ('python --version 2^>^&1') do set python_version=%%i
echo ℹ️  Python version: !python_version!

REM Check if virtual environment exists
if not exist "venv" (
    echo ⚠️  Virtual environment not found. Creating one...
    python -m venv venv
    if errorlevel 1 (
        echo ❌ Failed to create virtual environment
        pause
        exit /b 1
    )
    echo ✅ Virtual environment created
)

REM Activate virtual environment
echo ℹ️  Activating virtual environment...
call venv\Scripts\activate.bat

REM Install requirements if needed
if not exist "venv\.requirements_installed" (
    echo ℹ️  Installing requirements...
    python -m pip install --upgrade pip
    pip install -r requirements.txt
    
    if errorlevel 1 (
        echo ❌ Failed to install requirements
        pause
        exit /b 1
    )
    
    REM Download spaCy models
    echo ℹ️  Downloading spaCy models...
    python -m spacy download en_core_web_sm
    python -m spacy download es_core_news_sm
    python -m spacy download fr_core_news_sm
    python -m spacy download de_core_news_sm
    
    REM Mark requirements as installed
    echo. > venv\.requirements_installed
    echo ✅ Requirements installed
)

REM Check if .env file exists
if not exist ".env" (
    if exist "env.example" (
        echo ⚠️  .env file not found. Copying from env.example...
        copy env.example .env
        echo ⚠️  Please edit .env file with your configuration before running again
        pause
        exit /b 1
    ) else (
        echo ❌ .env file not found and env.example is not available
        pause
        exit /b 1
    )
)

REM Check if .env file has required variables
findstr /C:"SUPABASE_URL=" .env >nul
if errorlevel 1 (
    echo ⚠️  .env file exists but may not be properly configured
    echo ℹ️  Please ensure SUPABASE_URL and SUPABASE_KEY are set in .env
)

REM Run the application
echo ℹ️  Starting MASX AI ETL CPU Pipeline...
python run.py

REM Keep window open if there's an error
if errorlevel 1 (
    echo.
    echo ❌ Application failed to start
    pause
)
