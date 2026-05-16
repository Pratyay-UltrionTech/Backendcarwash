@echo off
echo ========================================
echo  Fixing Backend - Please Wait...
echo ========================================

cd /d "C:\Users\krish\OneDrive\Desktop\client\backend"

echo [1/5] Removing broken virtual environment...
if exist .venv (
    rmdir /s /q .venv
    echo Done.
) else (
    echo No .venv found, skipping.
)

echo [2/5] Creating fresh virtual environment...
python -m venv .venv
if errorlevel 1 (
    echo ERROR: Failed to create venv. Is Python installed?
    pause
    exit /b 1
)

echo [3/5] Activating virtual environment...
call .venv\Scripts\activate

echo [4/5] Upgrading pip and installing dependencies...
python -m pip install --upgrade pip setuptools wheel
python -m pip install --force-reinstall -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install dependencies.
    pause
    exit /b 1
)

echo [5/5] Starting backend server...
echo ========================================
echo  Backend running at http://localhost:8000
echo  API Docs at   http://localhost:8000/docs
echo ========================================
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
pause
