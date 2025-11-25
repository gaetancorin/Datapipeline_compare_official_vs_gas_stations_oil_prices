@echo off
REM --------------------------------------
REM Download z_score_visualization.ipynb to the script folder
REM --------------------------------------

REM Get the folder where this script is located
SET "SCRIPT_DIR=%~dp0"

REM URL of the file to download
SET "URL=https://open-documentations.s3.eu-west-3.amazonaws.com/z_score_visualization.ipynb"

REM Destination path
SET "DEST=%SCRIPT_DIR%z_score_visualization.ipynb"

REM Download using PowerShell
powershell -Command "Invoke-WebRequest -Uri '%URL%' -OutFile '%DEST%'"

echo File downloaded to %DEST%
pause