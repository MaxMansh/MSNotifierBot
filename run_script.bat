@echo off
title Bot
set PROJECT_DIR=%~dp0
cd /d "%PROJECT_DIR%"
call ".venv\Scripts\activate.bat"
python "Main.pyw" > "log.txt" 2>&1
