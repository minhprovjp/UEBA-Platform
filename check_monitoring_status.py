#!/usr/bin/env python3
"""
Check Self-Monitoring System Status
"""

import sys
import os
import json
import time
from pathlib import Path
from datetime import datetime, timedelta

# Add engine directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'engine'))

def check_log_files():
    """Check log files for recent activity"""
    print("=" * 60)
    print("LOG FILE STATUS")
    print("=" * 60)
    
    log_files = [
        "logs/self_monitoring.log",
        "logs/self_monitoring_service.log",
        "logs/self_monitoring_simple.log",
        "logs/self_monitoring_audit.log"
    ]
    
    for log_file in log_files:
        if Path(log_file).exists():
            stat = Path(log_file).stat()
            size = stat.st_size
            modified = datetime.fromtimestamp(stat.st_mtime)
            age = datetime.now() - modified
            
            print(f"✓ {log_file}")
            print(f"  Size: {size} bytes")
            print(f"  Last modified: {modified}")
            print(f"  Age: {age}")
            
            # Show last few lines
            try:
                with open(log_file, 'r') as f:
                    lines = f.readlines()
                    if lines:
                        print(f"  Last entry: {lines[-1].strip()}")
                    else:
                        print("  File is empty")
            except Exception as e:
                print(f"  Error reading file: {e}")
            print()
        else:
            print(f"✗ {log_file} - Not found")

def check_database_files():
    """Check database files"""
    print("=" * 60)
    print("DATABASE FILES STATUS")
    print("=" * 60)
    
    db_files = [
        "data/shadow_monitoring.db",
        "data/integrity_validation.db"
    ]
    
    for db_file in db_files:
        if Path(db_file).exists():
            stat = Path(db_file).stat()
            size = stat.st_size
            modified = datetime.fromtimestamp(stat.st_mtime)
            age = datetime.now() - modified
            
            print(f"✓ {db_file}")
            print(f"  Size: {size} bytes")
            print(f"  Last modified: {modified}")
            print(f"  Age: {age}")
            print()
        else:
            print(f"✗ {db_file} - Not found")

def check_config_files():
    """Check configuration files"""
    print("=" * 60)
    print("CONFIGURATION FILES STATUS")
    print("=" * 60)
    
    config_files = [
        "self_monitoring_config.json",
        ".self_monitoring_key"
    ]
    
    for config_file in config_files:
        if Path(config_file).exists():
            stat = Path(config_file).stat()
            size = stat.st_size
            modified = datetime.fromtimestamp(stat.st_mtime)
            
            print(f"✓ {config_file}")
            print(f"  Size: {size} bytes")
            print(f"  Last modified: {modified}")
            
            if config_file.endswith('.json'):
                try:
                    with open(config_file, 'r') as f:
                        config = json.load(f)
                    print(f"  Valid JSON: Yes")
                    print(f"  Keys: {list(config.keys())}")
                except Exception as e:
                    print(f"  Valid JSON: No - {e}")
            print()
        else:
            print(f"✗ {config_file} - Not found")

def test_database_connection():
    """Test database connection"""
    print("=" * 60)
    print("DATABASE CONNECTION TEST")
    print("=" * 60)
    
    try:
        import mysql.connector
        from mysql.connector import Error as MySQLError
        
        # Load config
        if not Path("self_monitoring_config.json").exists():
            print("✗ Configuration file not found")
            return False
        
        with open("self_monitoring_config.json", 'r') as f:
            config = json.load(f)
        
        db_config = config.get('database', {})
        
        print(f"Testing connection to {db_config.get('host', 'localhost')}:{db_config.get('port', 3306)}")
        print(f"Database: {db_config.get('database', 'uba_db')}")
        print(f"User: {db_config.get('user', 'uba_user')}")
        
        connection = mysql.connector.connect(
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', 3306),
            database=db_config.get('database', 'uba_db'),
            user=db_config.get('user', 'uba_user'),
            password=db_config.get('password', ''),
            connection_timeout=5
        )
        
        if connection.is_connected():
            print("✓ Database connection successful")
            
            cursor = connection.cursor()
            cursor.execute("SHOW PROCESSLIST")
            processes = cursor.fetchall()
            print(f"✓ Active connections: {len(processes)}")
            
            cursor.close()
            connection.close()
            return True
        else:
            print("✗ Database connection failed")
            return False
            
    except MySQLError as e:
        print(f"✗ Database error: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

def check_running_processes():
    """Check if monitoring processes are running"""
    print("=" * 60)
    print("PROCESS STATUS")
    print("=" * 60)
    
    try:
        import psutil
        
        python_processes = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['name'] and 'python' in proc.info['name'].lower():
                    cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                    if 'self_monitoring' in cmdline or 'monitoring' in cmdline:
                        python_processes.append({
                            'pid': proc.info['pid'],
                            'cmdline': cmdline
                        })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        if python_processes:
            print("✓ Found monitoring processes:")
            for proc in python_processes:
                print(f"  PID {proc['pid']}: {proc['cmdline']}")
        else:
            print("✗ No monitoring processes found")
            
    except ImportError:
        print("⚠ psutil not available, cannot check processes")
    except Exception as e:
        print(f"✗ Error checking processes: {e}")

def analyze_recent_activity():
    """Analyze recent activity in logs"""
    print("=" * 60)
    print("RECENT ACTIVITY ANALYSIS")
    print("=" * 60)
    
    log_file = "logs/self_monitoring_service.log"
    if not Path(log_file).exists():
        log_file = "logs/self_monitoring_simple.log"
    
    if not Path(log_file).exists():
        print("✗ No log files found for analysis")
        return
    
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
        
        if not lines:
            print("✗ Log file is empty")
            return
        
        # Analyze last 20 lines
        recent_lines = lines[-20:]
        
        print(f"Analyzing last {len(recent_lines)} log entries from {log_file}:")
        print("-" * 40)
        
        for line in recent_lines:
            line = line.strip()
            if line:
                print(line)
        
        # Look for specific patterns
        error_count = sum(1 for line in recent_lines if 'ERROR' in line)
        warning_count = sum(1 for line in recent_lines if 'WARNING' in line)
        
        print("-" * 40)
        print(f"Errors in recent activity: {error_count}")
        print(f"Warnings in recent activity: {warning_count}")
        
    except Exception as e:
        print(f"✗ Error analyzing log file: {e}")

def main():
    """Main entry point"""
    print("UBA Self-Monitoring System Status Check")
    print(f"Timestamp: {datetime.now()}")
    
    check_config_files()
    check_database_files()
    check_log_files()
    test_database_connection()
    check_running_processes()
    analyze_recent_activity()
    
    print("=" * 60)
    print("STATUS CHECK COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()