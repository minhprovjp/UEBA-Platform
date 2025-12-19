"""
Cryptographic Logger for UBA Self-Monitoring System

Provides tamper-evident logging with cryptographic integrity validation
for all self-monitoring activities and audit trails.
"""

import hashlib
import hmac
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import uuid

from .interfaces import IntegrityInterface, InfrastructureEvent


class CryptoLogger(IntegrityInterface):
    """Cryptographic logger with integrity validation"""
    
    def __init__(self, log_path: Optional[str] = None, secret_key: Optional[str] = None):
        """
        Initialize cryptographic logger
        
        Args:
            log_path: Path to log file. If None, uses default path.
            secret_key: Secret key for HMAC. If None, generates or loads from environment.
        """
        self.logger = logging.getLogger(__name__)
        self.log_path = log_path or self._get_default_log_path()
        self.secret_key = secret_key or self._get_or_create_secret_key()
        
        # Ensure log directory exists
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        
        # Initialize log file with header if it doesn't exist
        if not os.path.exists(self.log_path):
            self._initialize_log_file()
    
    def _get_default_log_path(self) -> str:
        """Get default log file path"""
        base_dir = Path(__file__).parent.parent.parent
        return str(base_dir / "logs" / "self_monitoring_audit.log")
    
    def _get_or_create_secret_key(self) -> str:
        """Get or create secret key for HMAC"""
        # Try to get from environment first
        key = os.environ.get('UBA_SELF_MONITORING_SECRET_KEY')
        if key:
            return key
        
        # Try to load from file
        key_file = Path(__file__).parent.parent.parent / ".self_monitoring_key"
        if key_file.exists():
            try:
                with open(key_file, 'r') as f:
                    return f.read().strip()
            except Exception as e:
                self.logger.warning(f"Could not read key file: {e}")
        
        # Generate new key
        key = hashlib.sha256(str(uuid.uuid4()).encode()).hexdigest()
        
        # Try to save key to file for persistence
        try:
            with open(key_file, 'w') as f:
                f.write(key)
            os.chmod(key_file, 0o600)  # Restrict permissions
            self.logger.info("Generated new secret key for crypto logging")
        except Exception as e:
            self.logger.warning(f"Could not save key file: {e}")
        
        return key
    
    def _initialize_log_file(self):
        """Initialize log file with header"""
        header = {
            "log_type": "uba_self_monitoring_audit",
            "version": "1.0.0",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "integrity_algorithm": "HMAC-SHA256",
            "format": "jsonl"
        }
        
        with open(self.log_path, 'w') as f:
            f.write(json.dumps(header) + '\n')
    
    def create_checksum(self, data: Any) -> str:
        """
        Create cryptographic checksum for data using HMAC-SHA256
        
        Args:
            data: Data to create checksum for
            
        Returns:
            str: Hexadecimal checksum
        """
        try:
            # Convert data to consistent string representation
            if isinstance(data, str):
                data_str = data
            else:
                # Handle enum serialization
                data_str = json.dumps(data, sort_keys=True, ensure_ascii=False, default=self._json_serializer)
            
            # Create HMAC-SHA256 checksum
            checksum = hmac.new(
                self.secret_key.encode('utf-8'),
                data_str.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            return checksum
            
        except Exception as e:
            self.logger.error(f"Error creating checksum: {e}")
            return ""
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for enum and datetime objects"""
        from enum import Enum
        from datetime import datetime
        
        if isinstance(obj, Enum):
            return obj.value
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        else:
            return str(obj)
    
    def verify_integrity(self, data: Any, checksum: str) -> bool:
        """
        Verify data integrity using checksum
        
        Args:
            data: Data to verify
            checksum: Expected checksum
            
        Returns:
            bool: True if integrity is valid
        """
        try:
            calculated_checksum = self.create_checksum(data)
            return hmac.compare_digest(calculated_checksum, checksum)
        except Exception as e:
            self.logger.error(f"Error verifying integrity: {e}")
            return False
    
    def detect_tampering(self, data_id: str) -> Tuple[bool, Optional[str]]:
        """
        Detect if data has been tampered with by checking audit trail
        
        Args:
            data_id: Identifier of data to check
            
        Returns:
            Tuple of (is_tampered, error_message)
        """
        try:
            # Read audit trail and verify checksums
            with open(self.log_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        entry = json.loads(line.strip())
                        
                        # Skip header line
                        if entry.get('log_type') == 'uba_self_monitoring_audit':
                            continue
                        
                        # Check if this entry relates to our data
                        if entry.get('data_id') == data_id:
                            # Verify entry integrity
                            stored_checksum = entry.get('integrity_hash')
                            if not stored_checksum:
                                return True, f"Missing integrity hash in audit entry at line {line_num}"
                            
                            # Create entry copy without checksum for verification
                            entry_copy = {k: v for k, v in entry.items() if k != 'integrity_hash'}
                            
                            if not self.verify_integrity(entry_copy, stored_checksum):
                                return True, f"Integrity verification failed for audit entry at line {line_num}"
                    
                    except json.JSONDecodeError:
                        return True, f"Corrupted audit entry at line {line_num}"
            
            return False, None
            
        except FileNotFoundError:
            return True, "Audit log file not found"
        except Exception as e:
            return True, f"Error checking tampering: {str(e)}"
    
    def create_audit_trail(self, event) -> str:
        """
        Create tamper-evident audit trail entry
        
        Args:
            event: Event to log (InfrastructureEvent, ConnectionEvent, etc.)
            
        Returns:
            str: Audit trail entry ID
        """
        try:
            entry_id = str(uuid.uuid4())
            timestamp = datetime.now(timezone.utc).isoformat()
            
            # Handle different event types
            if hasattr(event, 'source_ip'):
                # InfrastructureEvent
                audit_entry = {
                    "entry_id": entry_id,
                    "timestamp": timestamp,
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "source_ip": event.source_ip,
                    "user_account": event.user_account,
                    "target_component": event.target_component.value if hasattr(event.target_component, 'value') else str(event.target_component),
                    "action_details": event.action_details,
                    "risk_score": event.risk_score,
                    "data_id": event.event_id
                }
            elif hasattr(event, 'host'):
                # ConnectionEvent
                audit_entry = {
                    "entry_id": entry_id,
                    "timestamp": timestamp,
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "source_ip": event.host.split(':')[0] if ':' in event.host else event.host,
                    "user_account": event.user,
                    "target_component": "database",
                    "action_details": event.details,
                    "risk_score": event.risk_score,
                    "data_id": event.event_id
                }
            else:
                # Generic event
                audit_entry = {
                    "entry_id": entry_id,
                    "timestamp": timestamp,
                    "event_id": getattr(event, 'event_id', str(uuid.uuid4())),
                    "event_type": getattr(event, 'event_type', 'unknown'),
                    "source_ip": "unknown",
                    "user_account": getattr(event, 'user', 'unknown'),
                    "target_component": "unknown",
                    "action_details": getattr(event, 'details', {}),
                    "risk_score": getattr(event, 'risk_score', 0.0),
                    "data_id": getattr(event, 'event_id', entry_id)
                }
            
            # Create integrity hash
            integrity_hash = self.create_checksum(audit_entry)
            audit_entry["integrity_hash"] = integrity_hash
            
            # Write to audit log
            with open(self.log_path, 'a') as f:
                f.write(json.dumps(audit_entry, ensure_ascii=False, default=self._json_serializer) + '\n')
            
            return entry_id
            
        except Exception as e:
            self.logger.error(f"Error creating audit trail: {e}")
            return ""
    
    def log_config_access(self, action: str, config_path: str, success: bool, error: Optional[str] = None):
        """
        Log configuration access events
        
        Args:
            action: Type of action (LOAD, SAVE, UPDATE)
            config_path: Path to configuration file
            success: Whether the action was successful
            error: Error message if action failed
        """
        try:
            entry = {
                "entry_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event_type": "config_access",
                "action": action,
                "config_path": config_path,
                "success": success,
                "error": error,
                "process_id": os.getpid()
            }
            
            # Create integrity hash
            integrity_hash = self.create_checksum(entry)
            entry["integrity_hash"] = integrity_hash
            
            # Write to audit log
            with open(self.log_path, 'a') as f:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
                
        except Exception as e:
            self.logger.error(f"Error logging config access: {e}")
    
    def log_monitoring_event(self, event_type: str, component: str, details: Dict[str, Any], 
                           risk_score: float = 0.0):
        """
        Log monitoring events with cryptographic integrity
        
        Args:
            event_type: Type of monitoring event
            component: Component being monitored
            details: Event details
            risk_score: Risk score for the event
        """
        try:
            entry = {
                "entry_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event_type": event_type,
                "component": component,
                "details": details,
                "risk_score": risk_score,
                "process_id": os.getpid()
            }
            
            # Create integrity hash
            integrity_hash = self.create_checksum(entry)
            entry["integrity_hash"] = integrity_hash
            
            # Write to audit log
            with open(self.log_path, 'a') as f:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
                
        except Exception as e:
            self.logger.error(f"Error logging monitoring event: {e}")
    
    def log_threat_detection(self, threat_type: str, severity: str, indicators: Dict[str, Any],
                           confidence: float, affected_components: List[str]):
        """
        Log threat detection events
        
        Args:
            threat_type: Type of threat detected
            severity: Threat severity level
            indicators: Attack indicators
            confidence: Confidence score
            affected_components: List of affected components
        """
        try:
            entry = {
                "entry_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event_type": "threat_detection",
                "threat_type": threat_type,
                "severity": severity,
                "indicators": indicators,
                "confidence": confidence,
                "affected_components": affected_components,
                "process_id": os.getpid()
            }
            
            # Create integrity hash
            integrity_hash = self.create_checksum(entry)
            entry["integrity_hash"] = integrity_hash
            
            # Write to audit log
            with open(self.log_path, 'a') as f:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
                
        except Exception as e:
            self.logger.error(f"Error logging threat detection: {e}")
    
    def log_response_action(self, action_type: str, target: str, success: bool, 
                          error: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        """
        Log automated response actions
        
        Args:
            action_type: Type of response action
            target: Target of the action
            success: Whether the action was successful
            error: Error message if action failed
            details: Additional action details
        """
        try:
            entry = {
                "entry_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event_type": "response_action",
                "action_type": action_type,
                "target": target,
                "success": success,
                "error": error,
                "details": details or {},
                "process_id": os.getpid()
            }
            
            # Create integrity hash
            integrity_hash = self.create_checksum(entry)
            entry["integrity_hash"] = integrity_hash
            
            # Write to audit log
            with open(self.log_path, 'a') as f:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
                
        except Exception as e:
            self.logger.error(f"Error logging response action: {e}")
    
    def verify_log_integrity(self) -> Tuple[bool, List[str]]:
        """
        Verify integrity of entire audit log
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        try:
            with open(self.log_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        entry = json.loads(line.strip())
                        
                        # Skip header line
                        if entry.get('log_type') == 'uba_self_monitoring_audit':
                            continue
                        
                        # Verify entry integrity
                        stored_checksum = entry.get('integrity_hash')
                        if not stored_checksum:
                            errors.append(f"Missing integrity hash at line {line_num}")
                            continue
                        
                        # Create entry copy without checksum for verification
                        entry_copy = {k: v for k, v in entry.items() if k != 'integrity_hash'}
                        
                        if not self.verify_integrity(entry_copy, stored_checksum):
                            errors.append(f"Integrity verification failed at line {line_num}")
                    
                    except json.JSONDecodeError:
                        errors.append(f"Corrupted entry at line {line_num}")
            
        except FileNotFoundError:
            errors.append("Audit log file not found")
        except Exception as e:
            errors.append(f"Error verifying log integrity: {str(e)}")
        
        return len(errors) == 0, errors