"""
System Startup and Configuration Management for UBA Self-Monitoring

This module provides system configuration, startup procedures, and initialization
management for the complete UBA self-monitoring system.
"""

import logging
import os
import sys
import json
import signal
import time
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import argparse

try:
    from .integration_orchestrator import IntegrationOrchestrator
    from .config_manager import SelfMonitoringConfig
    from .crypto_logger import CryptoLogger
except ImportError:
    # For direct execution
    from integration_orchestrator import IntegrationOrchestrator
    from config_manager import SelfMonitoringConfig
    from crypto_logger import CryptoLogger


class SystemStartup:
    """
    Manages system startup, configuration, and shutdown procedures
    for the UBA self-monitoring system.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize system startup manager.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.orchestrator: Optional[IntegrationOrchestrator] = None
        self.logger = self._setup_logging()
        self._shutdown_requested = False
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('logs/self_monitoring_startup.log', mode='a')
            ]
        )
        return logging.getLogger(__name__)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self._shutdown_requested = True
    
    def validate_system_requirements(self) -> bool:
        """
        Validate system requirements and dependencies.
        
        Returns:
            bool: True if all requirements are met, False otherwise
        """
        try:
            self.logger.info("Validating system requirements...")
            
            # Check Python version
            if sys.version_info < (3, 8):
                self.logger.error("Python 3.8 or higher is required")
                return False
            
            # Check required directories exist
            required_dirs = [
                'logs',
                'engine/self_monitoring',
                'data/processed'
            ]
            
            for dir_path in required_dirs:
                if not Path(dir_path).exists():
                    self.logger.info(f"Creating required directory: {dir_path}")
                    Path(dir_path).mkdir(parents=True, exist_ok=True)
            
            # Check configuration file
            if self.config_path and not Path(self.config_path).exists():
                self.logger.error(f"Configuration file not found: {self.config_path}")
                return False
            
            # Test database connectivity (simplified check)
            try:
                import mysql.connector
                self.logger.info("MySQL connector available")
            except ImportError:
                self.logger.error("MySQL connector not available")
                return False
            
            # Check required Python packages
            required_packages = [
                'threading',
                'queue',
                'datetime',
                'json',
                'hashlib',
                'uuid'
            ]
            
            for package in required_packages:
                try:
                    __import__(package)
                except ImportError:
                    self.logger.error(f"Required package not available: {package}")
                    return False
            
            self.logger.info("System requirements validation completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating system requirements: {e}")
            return False
    
    def initialize_configuration(self) -> bool:
        """
        Initialize system configuration.
        
        Returns:
            bool: True if configuration initialized successfully, False otherwise
        """
        try:
            self.logger.info("Initializing system configuration...")
            
            # Load configuration
            config_manager = SelfMonitoringConfig(self.config_path)
            config = config_manager.load_config()
            
            # Validate configuration
            is_valid, errors = config_manager.validate_config(config)
            if not is_valid:
                self.logger.error(f"Configuration validation failed: {errors}")
                return False
            
            # Set up crypto logger
            crypto_logger = CryptoLogger()
            
            # Log system initialization
            crypto_logger.log_monitoring_event(
                "system_initialization",
                "self_monitoring_system",
                {
                    "config_path": self.config_path,
                    "startup_time": datetime.now().isoformat(),
                    "python_version": sys.version,
                    "working_directory": os.getcwd()
                }
            )
            
            self.logger.info("System configuration initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error initializing configuration: {e}")
            return False
    
    def start_system(self) -> bool:
        """
        Start the complete UBA self-monitoring system.
        
        Returns:
            bool: True if system started successfully, False otherwise
        """
        try:
            self.logger.info("Starting UBA self-monitoring system...")
            
            # Validate requirements
            if not self.validate_system_requirements():
                return False
            
            # Initialize configuration
            if not self.initialize_configuration():
                return False
            
            # Create and start orchestrator
            self.orchestrator = IntegrationOrchestrator(self.config_path)
            
            if not self.orchestrator.start_system():
                self.logger.error("Failed to start integration orchestrator")
                return False
            
            self.logger.info("UBA self-monitoring system started successfully")
            
            # Wait for shutdown signal
            self._wait_for_shutdown()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting system: {e}")
            return False
        finally:
            self.stop_system()
    
    def stop_system(self) -> bool:
        """
        Stop the UBA self-monitoring system.
        
        Returns:
            bool: True if system stopped successfully, False otherwise
        """
        try:
            self.logger.info("Stopping UBA self-monitoring system...")
            
            if self.orchestrator:
                success = self.orchestrator.stop_system()
                if success:
                    self.logger.info("UBA self-monitoring system stopped successfully")
                else:
                    self.logger.error("Error stopping system components")
                return success
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping system: {e}")
            return False
    
    def _wait_for_shutdown(self):
        """Wait for shutdown signal or system failure"""
        try:
            self.logger.info("System running. Press Ctrl+C to stop.")
            
            while not self._shutdown_requested:
                # Check system health
                if self.orchestrator and not self.orchestrator.is_healthy():
                    self.logger.error("System health check failed, initiating shutdown")
                    break
                
                time.sleep(5)
                
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
        except Exception as e:
            self.logger.error(f"Error in shutdown wait loop: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        Get current system status.
        
        Returns:
            Dict containing system status information
        """
        try:
            if not self.orchestrator:
                return {
                    "status": "not_started",
                    "timestamp": datetime.now().isoformat()
                }
            
            system_status = self.orchestrator.get_system_status()
            metrics = self.orchestrator.get_data_flow_metrics()
            
            return {
                "status": "running" if system_status.overall_health == "healthy" else "degraded",
                "timestamp": system_status.timestamp.isoformat(),
                "monitoring_active": system_status.monitoring_active,
                "shadow_monitoring_active": system_status.shadow_monitoring_active,
                "detection_active": system_status.detection_active,
                "response_active": system_status.response_active,
                "dashboard_active": system_status.dashboard_active,
                "active_threats": system_status.active_threats,
                "processed_events": system_status.processed_events,
                "response_actions_executed": system_status.response_actions_executed,
                "events_per_second": metrics.throughput_events_per_second,
                "processing_latency_ms": metrics.processing_latency_ms
            }
            
        except Exception as e:
            self.logger.error(f"Error getting system status: {e}")
            return {
                "status": "error",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
    
    def restart_system(self) -> bool:
        """
        Restart the UBA self-monitoring system.
        
        Returns:
            bool: True if system restarted successfully, False otherwise
        """
        try:
            self.logger.info("Restarting UBA self-monitoring system...")
            
            # Stop current system
            if not self.stop_system():
                self.logger.error("Failed to stop system for restart")
                return False
            
            # Wait a moment for cleanup
            time.sleep(2)
            
            # Start system again
            return self.start_system()
            
        except Exception as e:
            self.logger.error(f"Error restarting system: {e}")
            return False


def create_default_config(config_path: str) -> bool:
    """
    Create a default configuration file.
    
    Args:
        config_path: Path where to create the configuration file
        
    Returns:
        bool: True if configuration created successfully, False otherwise
    """
    try:
        default_config = {
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
                "max_events_per_batch": 1000
            },
            "detection": {
                "enabled": True,
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
                        "DESCRIBE.*",
                        "SELECT.*COUNT.*FROM"
                    ]
                },
                "thresholds": {
                    "concurrent_session_limit": 2,
                    "privilege_escalation_keywords": [
                        "GRANT", "REVOKE", "CREATE USER", "DROP USER", "ALTER USER"
                    ]
                }
            },
            "response": {
                "isolation_thresholds": {
                    "critical": "complete",
                    "high": "service",
                    "medium": "network",
                    "low": "none"
                },
                "credential_rotation": {
                    "auto_rotate_on_compromise": True,
                    "rotation_interval_hours": 24,
                    "rollback_window_minutes": 30
                }
            },
            "alerting": {
                "enabled": True,
                "email_notifications": False,
                "alert_thresholds": {
                    "high_risk_score": 0.7,
                    "critical_risk_score": 0.9
                }
            },
            "system": {
                "health_check_interval": 60,
                "metrics_update_interval": 30
            },
            "processing": {
                "batch_size": 100,
                "max_queue_size": 10000
            }
        }
        
        with open(config_path, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        print(f"Default configuration created at: {config_path}")
        return True
        
    except Exception as e:
        print(f"Error creating default configuration: {e}")
        return False


def main():
    """Main entry point for the UBA self-monitoring system"""
    parser = argparse.ArgumentParser(description='UBA Self-Monitoring System')
    parser.add_argument(
        '--config', 
        type=str, 
        help='Path to configuration file'
    )
    parser.add_argument(
        '--create-config', 
        type=str, 
        help='Create default configuration file at specified path'
    )
    parser.add_argument(
        '--status', 
        action='store_true', 
        help='Show system status and exit'
    )
    
    args = parser.parse_args()
    
    # Create default configuration if requested
    if args.create_config:
        if create_default_config(args.create_config):
            sys.exit(0)
        else:
            sys.exit(1)
    
    # Initialize startup manager
    startup = SystemStartup(args.config)
    
    # Show status if requested
    if args.status:
        status = startup.get_system_status()
        print(json.dumps(status, indent=2))
        sys.exit(0)
    
    # Start the system
    try:
        success = startup.start_system()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()