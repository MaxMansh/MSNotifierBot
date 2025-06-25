@echo off
title Bot
cd "C:\path_bot"
call ".venv\Scripts\activate.bat"
python "Main.pyw" > "log.txt" 2>&1
