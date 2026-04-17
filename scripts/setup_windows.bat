@echo off
REM ============================================================================
REM  MCX Relay - Windows Setup Script
REM  Installs Python dependencies and creates a Windows Scheduled Task
REM  that runs the relay Mon-Fri at 08:50 IST (03:20 UTC).
REM
REM  Prerequisites:
REM    - Python 3.12+ installed
REM    - Run this script as Administrator
REM ============================================================================

setlocal enabledelayedexpansion

echo.
echo  ============================================
echo   MCX Relay - Windows Setup
echo  ============================================
echo.

REM -- Step 0: Check admin privileges ----------------------------------------
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] This script must be run as Administrator.
    echo         Right-click and select "Run as administrator".
    pause
    exit /b 1
)

REM -- Step 1: Detect project root --------------------------------------------
set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."
pushd "%PROJECT_DIR%"
set "PROJECT_DIR=%CD%"
popd

echo [1/5] Project directory: %PROJECT_DIR%

REM -- Step 2: Check Python ---------------------------------------------------
echo.
echo [2/5] Checking Python installation...
set "PYTHON_CMD="

python --version >nul 2>&1
if %errorlevel% equ 0 (
    set "PYTHON_CMD=python"
    goto :python_found
)

python3 --version >nul 2>&1
if %errorlevel% equ 0 (
    set "PYTHON_CMD=python3"
    goto :python_found
)

py -3 --version >nul 2>&1
if %errorlevel% equ 0 (
    set "PYTHON_CMD=py -3"
    goto :python_found
)

for %%p in (
    "%LOCALAPPDATA%\Programs\Python\Python314\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
    "%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe"
    "C:\Python314\python.exe"
    "C:\Python313\python.exe"
    "C:\Python312\python.exe"
) do (
    if exist %%p (
        set "PYTHON_CMD=%%~p"
        goto :python_found
    )
)

echo [ERROR] Python not found on PATH or in common locations.
echo         Install Python 3.12+ from https://www.python.org/downloads/
echo         IMPORTANT: Check "Add Python to PATH" during install.
pause
exit /b 1

:python_found
for /f "tokens=*" %%v in ('%PYTHON_CMD% --version 2^>^&1') do set PYVER=%%v
echo         Found: %PYVER%
echo         Command: %PYTHON_CMD%

REM Resolve full path to python.exe
for /f "tokens=*" %%p in ('%PYTHON_CMD% -c "import sys; print(sys.executable)"') do (
    set "PYTHON_EXE=%%p"
)
echo         Executable: %PYTHON_EXE%

REM -- Step 3: Install Python dependencies ------------------------------------
echo.
echo [3/5] Installing Python dependencies...
%PYTHON_CMD% -m pip install --upgrade pip >nul 2>&1
%PYTHON_CMD% -m pip install mcxpy>=0.0.3 pandas>=2.0.0 curl_cffi>=0.5.0 yfinance>=0.2.0 xlrd>=2.0.1
if %errorlevel% neq 0 (
    echo [ERROR] pip install failed.
    echo         Try running: %PYTHON_CMD% -m ensurepip --upgrade
    pause
    exit /b 1
)
echo         Dependencies installed.

REM -- Step 4: Create logs directory ------------------------------------------
echo.
echo [4/5] Creating logs directory...
if not exist "%PROJECT_DIR%\logs" mkdir "%PROJECT_DIR%\logs"
echo         Logs: %PROJECT_DIR%\logs\

REM -- Step 5: Create Scheduled Task ------------------------------------------
echo.
echo [5/5] Creating Windows Scheduled Task...

REM Create a wrapper script that the Task Scheduler will run
echo @echo off > "%PROJECT_DIR%\scripts\run_relay.bat"
echo chcp 65001 ^>nul >> "%PROJECT_DIR%\scripts\run_relay.bat"
echo set PYTHONIOENCODING=utf-8 >> "%PROJECT_DIR%\scripts\run_relay.bat"
echo cd /d "%PROJECT_DIR%" >> "%PROJECT_DIR%\scripts\run_relay.bat"
echo "%PYTHON_EXE%" "%PROJECT_DIR%\scripts\mcx_relay.py" --loop ^>^> "%PROJECT_DIR%\logs\relay.log" 2^>^&1 >> "%PROJECT_DIR%\scripts\run_relay.bat"

REM Remove existing task if present
schtasks /delete /tn "MCXRelay" /f >nul 2>&1

REM Create task: runs Mon-Fri at 03:20 UTC (08:50 IST)
REM Uses SYSTEM account so it runs even when no user is logged in
schtasks /create /tn "MCXRelay" /tr "\"%PROJECT_DIR%\scripts\run_relay.bat\"" /sc weekly /d MON,TUE,WED,THU,FRI /st 03:20 /ru SYSTEM /rl HIGHEST /f
if %errorlevel% neq 0 (
    echo [WARNING] SYSTEM account failed, trying current user...
    schtasks /create /tn "MCXRelay" /tr "\"%PROJECT_DIR%\scripts\run_relay.bat\"" /sc weekly /d MON,TUE,WED,THU,FRI /st 03:20 /rl HIGHEST /f
)

REM Also create a restart safety net: if relay dies, restart at 14:00 IST (08:30 UTC)
schtasks /delete /tn "MCXRelayRestart" /f >nul 2>&1
schtasks /create /tn "MCXRelayRestart" /tr "\"%PROJECT_DIR%\scripts\run_relay.bat\"" /sc weekly /d MON,TUE,WED,THU,FRI /st 08:30 /ru SYSTEM /rl HIGHEST /f >nul 2>&1

REM Create daily verification task at 01:30 UTC (07:00 IST)
echo @echo off > "%PROJECT_DIR%\scripts\run_verify.bat"
echo chcp 65001 ^>nul >> "%PROJECT_DIR%\scripts\run_verify.bat"
echo set PYTHONIOENCODING=utf-8 >> "%PROJECT_DIR%\scripts\run_verify.bat"
echo cd /d "%PROJECT_DIR%" >> "%PROJECT_DIR%\scripts\run_verify.bat"
echo "%PYTHON_EXE%" "%PROJECT_DIR%\scripts\daily_verify.py" --days 3 ^>^> "%PROJECT_DIR%\logs\daily_verify.log" 2^>^&1 >> "%PROJECT_DIR%\scripts\run_verify.bat"

schtasks /delete /tn "MCXDailyVerify" /f >nul 2>&1
schtasks /create /tn "MCXDailyVerify" /tr "\"%PROJECT_DIR%\scripts\run_verify.bat\"" /sc weekly /d MON,TUE,WED,THU,FRI,SAT /st 01:30 /ru SYSTEM /rl HIGHEST /f

echo.
echo  ============================================
echo   Setup Complete!
echo  ============================================
echo.
echo   Scheduled Task: MCXRelay
echo   Schedule:       Mon-Fri at 03:20 UTC (08:50 IST)
echo   Safety restart: Mon-Fri at 08:30 UTC (14:00 IST)
echo   Logs:           %PROJECT_DIR%\logs\relay.log
echo   Python:         %PYTHON_EXE%
echo.
echo   Useful commands (run as Admin):
echo     schtasks /query /tn "MCXRelay" /v    - Check task status
echo     schtasks /run /tn "MCXRelay"          - Start relay now
echo     schtasks /end /tn "MCXRelay"          - Stop relay
echo     schtasks /delete /tn "MCXRelay" /f    - Remove task
echo.
echo   To start the relay RIGHT NOW:
echo     schtasks /run /tn "MCXRelay"
echo.
echo   The relay will:
echo     - Start at 08:50 IST every weekday
echo     - Auto-restart at 14:00 IST if it died
echo     - Self-heal missing data on startup (7-day catchup)
echo     - Refresh margins on startup and after EOD
echo     - Send heartbeat to Supabase every 15 min
echo     - Log to %PROJECT_DIR%\logs\relay.log
echo.

pause
