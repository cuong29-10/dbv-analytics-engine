@echo off
REM Dong goi app thanh mot file .exe duy nhat de gui cho tung nguoi.
REM KHONG nhung du lieu: cache chi sinh ra khi nguoi dung bam "Keo du lieu tu Microsoft Fabric ve".
cd /d "%~dp0"

py -3.13 -m PyInstaller --onefile --name DBV_Analytics_Engine ^
  --add-data "ui.html;." ^
  --add-data "fabric_config.json;." ^
  --console --clean --noconfirm ^
  server.py

echo.
echo Xong. File nam o: dist\DBV_Analytics_Engine.exe
pause
