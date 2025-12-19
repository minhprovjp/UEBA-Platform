#!/usr/bin/env python3
"""
Self-Monitoring Service - Runs continuously with auto-restart
"""

import sys
import os
import logging
import time
import json
import signal
import threading
from pathlib import Path
from datetime import datetime

# Add engine directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'engine'))

class SelfMonitoringService:
    """Self-monitoring service with auto-restart capability"""
    
    def __init__(self):
        self.running = False
        self.monitor = None
        self.restart_count = 0
        self.max_restarts = 5
        self.logger = None
        self.setup_logging()
        
        # Signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def setup_logging(self):
        """Setup logging configuration"""
        Path('logs').mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('logs/self_monitoring_service.log', mode='a')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def start_monitor(self):
        """Start the monitoring components"""
        try:
            from engine.self_monitoring.infrastructure_monitor import InfrastructureMonitor
            from engine.self_monitoring.config_manager import SelfMonitoringConfig
            
            # Load configuration
            config_manager = SelfMonitoringConfig("self_monitoring_config.json")
            
            # Create infrastructure monitor
            self.monitor = InfrastructureMonitor(config_manager)
            
            if self.monitor.start_monitoring():
                self.logger.info("Infrastructure monitor started successfully")
                return True
            else:
                self.logger.error("Failed to start infrastructure monitor")
                return False
                
        except Exception as e:
            self.logger.error(f"Error starting monitor: {e}")
            return False
    
    def stop_monitor(self):
        """Stop the monitoring components"""
        try:
            if self.monitor:
                self.logger.info("Stopping monitor...")
                self.monitor.stop_monitoring()
                self.monitor = None
                self.logger.info("Monitor stopped")
        except Exception as e:
            self.logger.error(f"Error stopping monitor: {e}")
    
    def check_monitor_health(self):
        """Check if monitor is healthy"""
        try:
            if not self.monitor:
                return False
            
            return self.monitor.is_healthy()
            
        except Exception as e:
            self.logger.error(f"Error checking monitor health: {e}")
            return False
    
    def run_service(self):
        """Main service loop with auto-restart"""
        self.logger.info("Starting UBA Self-Monitoring Service")
        self.running = True
        
        while self.running and self.restart_count < self.max_restarts:
            try:
                # Start monitor
                if not self.start_monitor():
                    self.logger.error("Failed to start monitor, retrying in 30 seconds...")
                    time.sleep(30)
                    self.restart_count += 1
                    continue
                
                self.logger.info("Self-monitoring service is running...")
                
                # Main monitoring loop
                last_health_check = time.time()
                health_check_interval = 60  # Check every minute
                
                while self.running:
                    current_time = time.time()
                    
                    # Periodic health check
                    if current_time - last_health_check >= health_check_interval:
                        if not self.check_monitor_health():
                            self.logger.warning("Monitor health check failed, restarting...")
                            self.stop_monitor()
                            self.restart_count += 1
                            break
                        else:
                            # Log statistics
                            try:
                                stats = self.monitor.get_monitoring_statistics()
                                self.logger.info(f"Service healthy - Stats: {stats}")
                            except Exception as e:
                                self.logger.warning(f"Could not get stats: {e}")
                        
                        last_health_check = current_time
                    
                    # Sleep for a short interval
                    # print("DEBUG: Sleeping...")
                    time.sleep(10)
                
                # Clean shutdown
                self.stop_monitor()
                
                if self.running and self.restart_count < self.max_restarts:
                    self.logger.info(f"Restarting service (attempt {self.restart_count + 1}/{self.max_restarts})")
                    time.sleep(10)  # Wait before restart
                
            except KeyboardInterrupt:
                self.logger.info("Keyboard interrupt received")
                self.running = False
            except Exception as e:
                self.logger.error(f"Unexpected error in service loop: {e}")
                self.restart_count += 1
                if self.restart_count < self.max_restarts:
                    self.logger.info(f"Restarting after error (attempt {self.restart_count}/{self.max_restarts})")
                    time.sleep(30)
        
        if self.restart_count >= self.max_restarts:
            self.logger.error(f"Maximum restart attempts ({self.max_restarts}) reached. Service stopping.")
        
        self.stop_monitor()
        self.logger.info("UBA Self-Monitoring Service stopped")

def create_service_config():
    """Create service configuration if it doesn't exist"""
    config_file = "self_monitoring_config.json"
    
    if Path(config_file).exists():
        return config_file
    
    print(f"Creating service configuration: {config_file}")
    
    service_config = {
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
                "concurrent_session_limit": 2,
                "unauthorized_access_attempts": 3
            },
            "patterns": {
                "malicious_queries": [
                    "SELECT.*FROM.*information_schema",
                    "SHOW.*GRANTS",
                    "SELECT.*user.*password",
                    "UNION.*SELECT"
                ],
                "reconnaissance_indicators": [
                    "SHOW.*DATABASES",
                    "SHOW.*TABLES",
                    "DESCRIBE.*"
                ]
            }
        },
        "shadow_monitoring": {
            "enabled": True,
            "heartbeat_interval_seconds": 60,
            "primary_health_check_interval_seconds": 30
        },
        "response": {
            "auto_response_enabled": False,
            "max_actions_per_minute": 5
        },
        "logging": {
            "level": "INFO",
            "file_path": "logs/self_monitoring.log",
            "max_file_size_mb": 100
        }
    }
    
    with open(config_file, 'w') as f:
        json.dump(service_config, f, indent=2)
    
    return config_file

def main():
    """Main entry point"""
    print("=" * 60)
    print("UBA Self-Monitoring Service")
    print("=" * 60)
    print("This service runs continuously and monitors your UBA database")
    print("Press Ctrl+C to stop the service")
    print("=" * 60)
    
    try:
        # Create configuration
        config_file = create_service_config()
        print(f"Using configuration: {config_file}")
        
        # Create and run service
        service = SelfMonitoringService()
        service.run_service()
        
        return 0
        
    except KeyboardInterrupt:
        print("\nService stopped by user")
        return 0
    except Exception as e:
        print(f"Fatal error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())