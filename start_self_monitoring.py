#!/usr/bin/env python3
"""
Simple startup script for UBA Self-Monitoring System
"""

import sys
import os
import logging
import time
import json
from pathlib import Path

# Add engine directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'engine'))

def setup_logging():
    """Setup logging configuration"""
    # Ensure logs directory exists
    Path('logs').mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/self_monitoring_simple.log', mode='a')
        ]
    )

def create_minimal_config():
    """Create minimal configuration if it doesn't exist"""
    config_file = "self_monitoring_config.json"
    
    if Path(config_file).exists():
        return config_file
    
    print(f"Creating minimal configuration: {config_file}")
    
    minimal_config = {
        "database": {
            "host": "localhost",
            "port": 3306,
            "database": "uba_db",
            "user": "uba_user",
            "password": "",
            "connection_timeout_seconds": 30
        },
        "monitoring": {
            "enabled": True,
            "interval_seconds": 30,
            "max_events_per_batch": 100
        },
        "detection": {
            "enabled": True,
            "thresholds": {
                "high_risk_threshold": 0.8,
                "concurrent_session_limit": 2
            },
            "patterns": {
                "malicious_queries": [
                    "SELECT.*FROM.*information_schema",
                    "SHOW.*GRANTS"
                ]
            }
        },
        "shadow_monitoring": {
            "enabled": True,
            "heartbeat_interval_seconds": 60
        },
        "response": {
            "auto_response_enabled": False
        },
        "logging": {
            "level": "INFO"
        }
    }
    
    with open(config_file, 'w') as f:
        json.dump(minimal_config, f, indent=2)
    
    return config_file

def start_basic_monitoring():
    """Start basic monitoring components only"""
    try:
        from engine.self_monitoring.infrastructure_monitor import InfrastructureMonitor
        from engine.self_monitoring.config_manager import SelfMonitoringConfig
        
        logger = logging.getLogger(__name__)
        
        # Load configuration
        config_manager = SelfMonitoringConfig("self_monitoring_config.json")
        
        # Create infrastructure monitor
        logger.info("Starting infrastructure monitor...")
        monitor = InfrastructureMonitor(config_manager)
        
        if monitor.start_monitoring():
            logger.info("Infrastructure monitor started successfully")
            
            # Keep running
            logger.info("Self-monitoring system is running. Press Ctrl+C to stop.")
            try:
                while True:
                    time.sleep(10)
                    
                    # Check health
                    if not monitor.is_healthy():
                        logger.warning("Monitor health check failed")
                    else:
                        stats = monitor.get_monitoring_statistics()
                        logger.info(f"Monitor stats: {stats}")
                        
            except KeyboardInterrupt:
                logger.info("Shutdown requested")
            finally:
                logger.info("Stopping monitor...")
                monitor.stop_monitoring()
                logger.info("Monitor stopped")
                
        else:
            logger.error("Failed to start infrastructure monitor")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"Error in basic monitoring: {e}")
        return False

def main():
    """Main entry point"""
    print("=" * 60)
    print("UBA Self-Monitoring System - Simple Startup")
    print("=" * 60)
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Create configuration
        config_file = create_minimal_config()
        logger.info(f"Using configuration: {config_file}")
        
        # Start basic monitoring
        success = start_basic_monitoring()
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
        return 0
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())