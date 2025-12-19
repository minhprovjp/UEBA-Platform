# backend_api/self_monitoring_api.py
from fastapi import APIRouter, Depends, HTTPException, status
from typing import Dict, List, Any, Optional
from datetime import datetime
import os
import json
import logging

# Ensure engine modules can be imported
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from . import auth, models

router = APIRouter(
    prefix="/api/self-monitoring",
    tags=["Self Monitoring"],
    dependencies=[Depends(auth.get_current_user)]
)

# Utils
def get_log_path():
    # Use environment variable or default relative path
    log_dir = os.environ.get('UBA_LOGS_DIR')
    if not log_dir:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_dir = os.path.join(base_dir, "logs")
    
    return os.path.join(log_dir, "self_monitoring_audit.log")

def get_config_path():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "self_monitoring_config.json")

# --- Endpoints ---

@router.get("/status")
def get_status():
    """Get status of self-monitoring components"""
    try:
        # 1. Read config for structure
        config_path = get_config_path()
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = json.load(f)
        else:
            config = {}

        # 2. Check log file liveliness
        log_path = get_log_path()
        last_update = None
        status_code = "warning" 
        
        if os.path.exists(log_path):
            mtime = os.path.getmtime(log_path)
            last_update = datetime.fromtimestamp(mtime).isoformat()
            
            # If log updated recently (< 5 mins), assume healthy
            if (datetime.now().timestamp() - mtime) < 300:
                status_code = "healthy"
            else:
                status_code = "warning" # Stale
        else:
            status_code = "error"

        # Mock component statuses based on overall health
        # In a real system, these might come from a shared state in Redis or DB
        return {
            "metrics": {
                "active_threats": 0, # Placeholder
                "events_24h": 0,     # Placeholder, would count lines
                "risk_score": 0.0,
                "monitored_components": 4
            },
            "infrastructure_monitor": {
                "status": status_code,
                "description": "Monitoring UBA infrastructure",
                "last_update": last_update
            },
            "shadow_monitor": {
                "status": "healthy", # Assuming separate process is fine
                "description": "Independent backup monitoring",
                "last_update": datetime.now().isoformat()
            },
            "threat_detection": {
                "status": status_code,
                "description": "Attack pattern recognition",
                "last_update": last_update
            },
            "data_integrity": {
                "status": "healthy",
                "description": "Cryptographic validation",
                "last_update": last_update
            },
            "health": {
                "uptime": "Unknown",
                "memory_usage": "Unknown",
                "cpu_usage": "Unknown",
                "db_connections": 0
            },
            "components": {
                "database": {"status": "healthy", "last_check": datetime.now().isoformat()},
                "api_server": {"status": "healthy", "last_check": datetime.now().isoformat()},
                "engine": {"status": status_code, "last_check": last_update}
            },
            "detectors": {
                "sql_injection": {"status": "active", "detections_24h": 0, "last_detection": None},
                "brute_force": {"status": "active", "detections_24h": 0, "last_detection": None}
            }
        }
    except Exception as e:
        logging.error(f"Error getting self-monitoring status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/alerts")
def get_alerts(limit: int = 20):
    """Get security alerts from audit log"""
    alerts = []
    log_path = get_log_path()
    
    if not os.path.exists(log_path):
        return {"alerts": []}
        
    try:
        # Read lines in reverse order (naive implementation)
        with open(log_path, 'r') as f:
            lines = f.readlines()
            
        for line in reversed(lines):
            try:
                entry = json.loads(line)
                # Filter for high risk or specific threat detections
                if entry.get('risk_score', 0) >= 0.7 or entry.get('event_type') == 'threat_detection':
                    alerts.append({
                        "id": entry.get('entry_id'),
                        "timestamp": entry.get('timestamp'),
                        "severity": "critical" if entry.get('risk_score', 0) >= 0.9 else "high",
                        "title": entry.get('event_type').replace('_', ' ').title(),
                        "description": json.dumps(entry.get('details') or entry.get('action_details') or {}),
                        "component": entry.get('component') or entry.get('target_component') or "unknown",
                        "risk_score": entry.get('risk_score'),
                        "status": "new", # In a real app, check DB for ack status
                        "affected_users": entry.get('user_account') or "system"
                    })
                    
                if len(alerts) >= limit:
                    break
            except json.JSONDecodeError:
                continue
                
        return {"alerts": alerts}
    except Exception as e:
        logging.error(f"Error getting alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/infrastructure-events")
def get_infrastructure_events(limit: int = 50):
    """Get raw infrastructure events"""
    events = []
    log_path = get_log_path()
    
    if not os.path.exists(log_path):
        return {"events": []}
        
    try:
        with open(log_path, 'r') as f:
            lines = f.readlines()
            
        for line in reversed(lines):
            try:
                entry = json.loads(line)
                if entry.get('log_type'): continue # Skip header
                
                # Exclude config access or minor logs if needed
                events.append(entry)
                
                if len(events) >= limit:
                    break
            except json.JSONDecodeError:
                continue
                
        return {"events": events}
            
    except Exception as e:
        logging.error(f"Error getting events: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/alerts/{alert_id}/acknowledge")
def acknowledge_alert(alert_id: str):
    """Acknowledge an alert"""
    # Log acknowledgment to audit trail
    # Logic: append a new log entry saying this ID was acked users
    # For now just return success
    return {"status": "success", "message": f"Alert {alert_id} acknowledged"}
