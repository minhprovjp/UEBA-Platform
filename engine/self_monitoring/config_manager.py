"""
Configuration Manager for UBA Self-Monitoring System

Handles loading, validation, and management of self-monitoring configuration parameters
with secure defaults and integrity validation.
"""

import json
import os
import logging
from typing import Dict, Any, Tuple, List, Optional
from datetime import datetime, timezone
from pathlib import Path

from .interfaces import ConfigurationInterface
from .crypto_logger import CryptoLogger


class SelfMonitoringConfig(ConfigurationInterface):
    """Configuration manager for self-monitoring system"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager
        
        Args:
            config_path: Path to configuration file. If None, uses default path.
        """
        self.config_path = config_path or self._get_default_config_path()
        self.logger = logging.getLogger(__name__)
        self.crypto_logger = CryptoLogger()
        self._config_cache = None
        self._last_modified = None
        
    def _get_default_config_path(self) -> str:
        """Get default configuration file path"""
        base_dir = Path(__file__).parent.parent.parent
        return str(base_dir / "self_monitoring_config.json")
    
    def load_config(self) -> Dict[str, Any]:
        """
        Load monitoring configuration with caching and integrity validation
        
        Returns:
            Dict containing configuration parameters
        """
        try:
            # Check if file exists, create with defaults if not
            if not os.path.exists(self.config_path):
                self.logger.info(f"Config file not found at {self.config_path}, creating with defaults")
                default_config = self.get_secure_defaults()
                self.save_config(default_config)
                return default_config
            
            # Check if cached config is still valid
            current_modified = os.path.getmtime(self.config_path)
            if (self._config_cache is not None and 
                self._last_modified is not None and 
                current_modified == self._last_modified):
                return self._config_cache
            
            # Load and validate configuration
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Validate configuration
            is_valid, errors = self.validate_config(config)
            if not is_valid:
                self.logger.error(f"Invalid configuration: {errors}")
                # Fall back to secure defaults
                config = self.get_secure_defaults()
                self.save_config(config)
            
            # Update cache
            self._config_cache = config
            self._last_modified = current_modified
            
            # Log configuration load
            self.crypto_logger.log_config_access("LOAD", self.config_path, success=True)
            
            return config
            
        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}")
            self.crypto_logger.log_config_access("LOAD", self.config_path, success=False, error=str(e))
            # Return secure defaults on error
            return self.get_secure_defaults()
    
    def save_config(self, config: Dict[str, Any]) -> bool:
        """
        Save monitoring configuration with validation and integrity protection
        
        Args:
            config: Configuration dictionary to save
            
        Returns:
            bool: True if save was successful
        """
        try:
            # Validate configuration before saving
            is_valid, errors = self.validate_config(config)
            if not is_valid:
                self.logger.error(f"Cannot save invalid configuration: {errors}")
                return False
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            
            # Add metadata
            config_with_metadata = {
                **config,
                "_metadata": {
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "version": "1.0.0",
                    "checksum": self.crypto_logger.create_checksum(json.dumps(config, sort_keys=True))
                }
            }
            
            # Save configuration
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(config_with_metadata, f, indent=2, ensure_ascii=False)
            
            # Update cache
            self._config_cache = config
            self._last_modified = os.path.getmtime(self.config_path)
            
            # Log configuration save
            self.crypto_logger.log_config_access("SAVE", self.config_path, success=True)
            
            self.logger.info(f"Configuration saved successfully to {self.config_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving configuration: {e}")
            self.crypto_logger.log_config_access("SAVE", self.config_path, success=False, error=str(e))
            return False
    
    def validate_config(self, config: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate configuration parameters
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        try:
            # Required sections
            required_sections = [
                'monitoring', 'detection', 'response', 'integrity', 
                'shadow_monitoring', 'database', 'logging'
            ]
            
            for section in required_sections:
                if section not in config:
                    errors.append(f"Missing required section: {section}")
            
            # Validate monitoring section
            if 'monitoring' in config:
                monitoring = config['monitoring']
                if not isinstance(monitoring.get('enabled'), bool):
                    errors.append("monitoring.enabled must be boolean")
                
                if not isinstance(monitoring.get('interval_seconds'), int) or monitoring.get('interval_seconds', 0) <= 0:
                    errors.append("monitoring.interval_seconds must be positive integer")
            
            # Validate database section
            if 'database' in config:
                db = config['database']
                required_db_fields = ['host', 'port', 'database', 'user']
                for field in required_db_fields:
                    if not db.get(field):
                        errors.append(f"database.{field} is required")
                
                if not isinstance(db.get('port'), int) or not (1 <= db.get('port', 0) <= 65535):
                    errors.append("database.port must be valid port number (1-65535)")
            
            # Validate detection thresholds
            if 'detection' in config and 'thresholds' in config['detection']:
                thresholds = config['detection']['thresholds']
                
                # Risk score thresholds
                risk_thresholds = ['low_risk_threshold', 'medium_risk_threshold', 'high_risk_threshold']
                for threshold in risk_thresholds:
                    value = thresholds.get(threshold)
                    if value is not None and (not isinstance(value, (int, float)) or not (0 <= value <= 1)):
                        errors.append(f"detection.thresholds.{threshold} must be float between 0 and 1")
            
            # Validate response section
            if 'response' in config:
                response = config['response']
                if not isinstance(response.get('auto_response_enabled'), bool):
                    errors.append("response.auto_response_enabled must be boolean")
                
                if 'max_actions_per_minute' in response:
                    max_actions = response['max_actions_per_minute']
                    if not isinstance(max_actions, int) or max_actions <= 0:
                        errors.append("response.max_actions_per_minute must be positive integer")
            
            # Validate logging section
            if 'logging' in config:
                logging_config = config['logging']
                if 'level' in logging_config:
                    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
                    if logging_config['level'] not in valid_levels:
                        errors.append(f"logging.level must be one of: {valid_levels}")
            
        except Exception as e:
            errors.append(f"Configuration validation error: {str(e)}")
        
        return len(errors) == 0, errors
    
    def get_secure_defaults(self) -> Dict[str, Any]:
        """
        Get secure default configuration
        
        Returns:
            Dict containing secure default configuration
        """
        return {
            "monitoring": {
                "enabled": True,
                "interval_seconds": 30,
                "max_events_per_batch": 1000,
                "event_retention_days": 90,
                "components_to_monitor": [
                    "uba_db",
                    "uba_user", 
                    "performance_schema",
                    "uba_persistent_log"
                ]
            },
            "detection": {
                "enabled": True,
                "thresholds": {
                    "low_risk_threshold": 0.3,
                    "medium_risk_threshold": 0.6,
                    "high_risk_threshold": 0.8,
                    "unauthorized_access_attempts": 3,
                    "concurrent_session_limit": 2,
                    "privilege_escalation_keywords": [
                        "GRANT", "REVOKE", "CREATE USER", "DROP USER", 
                        "ALTER USER", "SET PASSWORD"
                    ]
                },
                "patterns": {
                    "malicious_queries": [
                        "SELECT.*FROM.*information_schema",
                        "SELECT.*FROM.*performance_schema.*user",
                        "SHOW.*GRANTS",
                        "SELECT.*authentication_string"
                    ],
                    "reconnaissance_indicators": [
                        "SHOW DATABASES",
                        "SHOW TABLES",
                        "DESCRIBE.*",
                        "EXPLAIN.*"
                    ]
                }
            },
            "response": {
                "auto_response_enabled": True,
                "max_actions_per_minute": 10,
                "actions": {
                    "credential_rotation_enabled": True,
                    "session_termination_enabled": True,
                    "component_isolation_enabled": True,
                    "backup_activation_enabled": True
                },
                "escalation": {
                    "critical_threat_timeout_seconds": 300,
                    "notification_channels": ["email", "log"]
                }
            },
            "integrity": {
                "enabled": True,
                "checksum_algorithm": "SHA-256",
                "verification_interval_seconds": 300,
                "tamper_detection_enabled": True,
                "backup_verification_enabled": True
            },
            "shadow_monitoring": {
                "enabled": True,
                "independent_database": {
                    "enabled": True,
                    "connection_string": "sqlite:///shadow_monitoring.db"
                },
                "heartbeat_interval_seconds": 60,
                "primary_health_check_interval_seconds": 30,
                "failover_timeout_seconds": 120
            },
            "database": {
                "host": "localhost",
                "port": 3306,
                "database": "uba_db",
                "user": "uba_user",
                "password": "",  # Should be set via environment variable
                "connection_timeout_seconds": 30,
                "query_timeout_seconds": 60,
                "max_connections": 10
            },
            "logging": {
                "level": "INFO",
                "file_path": "logs/self_monitoring.log",
                "max_file_size_mb": 100,
                "backup_count": 5,
                "crypto_logging_enabled": True,
                "audit_trail_enabled": True
            },
            "security": {
                "encryption_enabled": True,
                "key_rotation_days": 30,
                "secure_communication_only": True,
                "certificate_validation_enabled": True
            }
        }
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database-specific configuration"""
        config = self.load_config()
        return config.get('database', {})
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring-specific configuration"""
        config = self.load_config()
        return config.get('monitoring', {})
    
    def get_detection_config(self) -> Dict[str, Any]:
        """Get detection-specific configuration"""
        config = self.load_config()
        return config.get('detection', {})
    
    def get_response_config(self) -> Dict[str, Any]:
        """Get response-specific configuration"""
        config = self.load_config()
        return config.get('response', {})
    
    def update_section(self, section: str, updates: Dict[str, Any]) -> bool:
        """
        Update a specific configuration section
        
        Args:
            section: Configuration section name
            updates: Dictionary of updates to apply
            
        Returns:
            bool: True if update was successful
        """
        try:
            config = self.load_config()
            
            if section not in config:
                config[section] = {}
            
            # Deep merge updates
            config[section].update(updates)
            
            return self.save_config(config)
            
        except Exception as e:
            self.logger.error(f"Error updating configuration section {section}: {e}")
            return False