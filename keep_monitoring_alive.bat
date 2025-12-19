@echo off
echo Starting UBA Self-Monitoring System with Auto-Restart
echo Press Ctrl+C to stop completely
echo.

:start
echo [%date% %time%] Starting monitoring system...
python start_self_monitoring.py

echo [%date% %time%] Monitoring system stopped. Restarting in 10 seconds...
timeout /t 10 /nobreak > nul

goto start