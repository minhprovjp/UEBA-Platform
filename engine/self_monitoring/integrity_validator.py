"""
Integrity Violation Detection System for UBA Self-Monitoring

Provides comprehensive integrity validation for monitoring data, configuration files,
and audit trails with automatic restoration capabilities.
"""

import hashlib
import hmac
import json
import logging
import os
import shutil
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass
from pathlib import Path
import uuid
import threading
import time

try:
    from .interfaces import IntegrityInterface, InfrastructureEvent
    from .config_manager import SelfMonitoringConfig
    from .crypto_logger import CryptoLogger
except ImportError:
    # For direct execution or testing
    from interfaces import IntegrityInterface, InfrastructureEvent
    from config_manager import SelfMonitoringConfig
    from crypto_logger import CryptoLogger


@dataclass
class IntegrityViolation:
    """Integrity violation record"""
    violation_id: str
    timestamp: datetime
    violation_type: str
    affected_resource: str
    expected_checksum: str
    actual_checksum: str
    severity: str
    auto_restored: bool
    details: Dict[str, Any]


@dataclass
class ConfigurationBackup:
    """Configuration backup record"""
    backup_id: str
    timestamp: datetime
    config_path: str
    backup_path: str
    checksum: str
    is_verified: bool


class IntegrityValidator(IntegrityInterface):
    """Comprehensive integrity validation system"""
    
    def __init__(self, config_manager: Optional[SelfMonitoringConfig] = None):
        """
        Initialize integrity validator
        
        Args:
            config_manager: Configuration manager instance
        """
        self.config_manager = config_manager or SelfMonitoringConfig()
        self.crypto_logger = CryptoLogger()
        self.logger = logging.getLogger(__name__)
        
        # Validation state
        self._monitoring = False
        self._monitor_thread = None
        self._stop_event = threading.Event()
        
        # Integrity tracking
        self._tracked_files: Dict[str, str] = {}  # file_path -> checksum
        self._violations: List[IntegrityViolation] = []
        self._config_backups: List[ConfigurationBackup] = []
        
        # Database for integrity records
        self._integrity_db_path = self._get_integrity_db_path()
        self._integrity_db = None
        
        # Load configuration
        self._load_integrity_config()
        
        # Initialize integrity database
        self._initialize_integrity_database()
        
        # Create initial baselines
        self._create_initial_baselines()
    
    def _get_integrity_db_path(self) -> str:
        """Get path for integrity validation database"""
        base_dir = Path(__file__).parent.parent.parent
        return str(base_dir / "data" / "integrity_validation.db")
    
    def _load_integrity_config(self):
        """Load integrity validation configuration"""
        try:
            config = self.config_manager.load_config()
            self.integrity_config = config.get('integrity', {})
            self.monitoring_config = config.get('monitoring', {})
            
            # Set up file monitoring paths
            self._monitored_paths = [
                self.config_manager.config_path,
                self.crypto_logger.log_path,
                str(Path(__file__).parent.parent.parent / "self_monitoring_config.json"),
                str(Path(__file__).parent.parent.parent / "logs" / "self_monitoring_audit.log")
            ]
            
            self.logger.info("Integrity validation configuration loaded")
            
        except Exception as e:
            self.logger.error(f"Error loading integrity configuration: {e}")
            # Use safe defaults
            self.integrity_config = {
                'enabled': True,
                'verification_interval_seconds': 300,
                'auto_restore_enabled': True
            }
            self._monitored_paths = []
    
    def _initialize_integrity_database(self):
        """Initialize integrity validation database"""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self._integrity_db_path), exist_ok=True)
            
            # Connect to SQLite database
            self._integrity_db = sqlite3.connect(
                self._integrity_db_path,
                check_same_thread=False,
                timeout=30.0
            )
            self._integrity_db.row_factory = sqlite3.Row
            
            # Create tables
            self._create_integrity_tables()
            
            self.logger.info(f"Integrity database initialized at {self._integrity_db_path}")
            
        except Exception as e:
            self.logger.error(f"Error initializing integrity database: {e}")
            self._integrity_db = None
    
    def _create_integrity_tables(self):
        """Create integrity validation database tables"""
        try:
            cursor = self._integrity_db.cursor()
            
            # File baselines table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_baselines (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path TEXT UNIQUE NOT NULL,
                    checksum TEXT NOT NULL,
                    file_size INTEGER,
                    last_modified TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Integrity violations table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS integrity_violations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    violation_id TEXT UNIQUE NOT NULL,
                    timestamp TEXT NOT NULL,
                    violation_type TEXT NOT NULL,
                    affected_resource TEXT NOT NULL,
                    expected_checksum TEXT,
                    actual_checksum TEXT,
                    severity TEXT,
                    auto_restored INTEGER DEFAULT 0,
                    details TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Configuration backups table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS config_backups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    backup_id TEXT UNIQUE NOT NULL,
                    timestamp TEXT NOT NULL,
                    config_path TEXT NOT NULL,
                    backup_path TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    is_verified INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Audit log entries table (for uba_persistent_log monitoring)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_log_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entry_id TEXT UNIQUE NOT NULL,
                    timestamp TEXT NOT NULL,
                    entry_checksum TEXT NOT NULL,
                    entry_content TEXT,
                    is_verified INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_violations_timestamp ON integrity_violations(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_backups_timestamp ON config_backups(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_entries_timestamp ON audit_log_entries(timestamp)")
            
            self._integrity_db.commit()
            
        except Exception as e:
            self.logger.error(f"Error creating integrity tables: {e}")
            if self._integrity_db:
                self._integrity_db.rollback()
    
    def _create_initial_baselines(self):
        """Create initial integrity baselines for monitored files"""
        try:
            for file_path in self._monitored_paths:
                if os.path.exists(file_path):
                    self._create_file_baseline(file_path)
            
        except Exception as e:
            self.logger.error(f"Error creating initial baselines: {e}")
    
    def _create_file_baseline(self, file_path: str) -> bool:
        """Create integrity baseline for a file"""
        try:
            if not os.path.exists(file_path):
                return False
            
            # Calculate file checksum
            checksum = self._calculate_file_checksum(file_path)
            if not checksum:
                return False
            
            # Get file metadata
            stat = os.stat(file_path)
            file_size = stat.st_size
            last_modified = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()
            
            # Store baseline in database
            if self._integrity_db:
                cursor = self._integrity_db.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO file_baselines 
                    (file_path, checksum, file_size, last_modified, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    file_path,
                    checksum,
                    file_size,
                    last_modified,
                    datetime.now(timezone.utc).isoformat()
                ))
                self._integrity_db.commit()
            
            # Update tracking
            self._tracked_files[file_path] = checksum
            
            self.logger.debug(f"Created baseline for {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating baseline for {file_path}: {e}")
            return False
    
    def _calculate_file_checksum(self, file_path: str) -> str:
        """Calculate SHA-256 checksum for a file"""
        try:
            hash_sha256 = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            self.logger.error(f"Error calculating checksum for {file_path}: {e}")
            return ""
    
    def create_checksum(self, data: Any) -> str:
        """Create cryptographic checksum for data using HMAC-SHA256"""
        return self.crypto_logger.create_checksum(data)
    
    def verify_integrity(self, data: Any, checksum: str) -> bool:
        """Verify data integrity using checksum"""
        return self.crypto_logger.verify_integrity(data, checksum)
    
    def detect_tampering(self, data_id: str) -> Tuple[bool, Optional[str]]:
        """Detect if data has been tampered with"""
        try:
            # Check if this is a file path
            if os.path.exists(data_id):
                return self._detect_file_tampering(data_id)
            
            # Check audit log tampering
            return self.crypto_logger.detect_tampering(data_id)
            
        except Exception as e:
            return True, f"Error detecting tampering: {str(e)}"
    
    def _detect_file_tampering(self, file_path: str) -> Tuple[bool, Optional[str]]:
        """Detect file tampering by comparing with baseline"""
        try:
            if not os.path.exists(file_path):
                return True, "File no longer exists"
            
            # Get baseline checksum
            baseline_checksum = None
            if self._integrity_db:
                cursor = self._integrity_db.cursor()
                cursor.execute(
                    "SELECT checksum FROM file_baselines WHERE file_path = ?",
                    (file_path,)
                )
                result = cursor.fetchone()
                if result:
                    baseline_checksum = result[0]
            
            if not baseline_checksum:
                # No baseline exists, create one
                self._create_file_baseline(file_path)
                return False, None
            
            # Calculate current checksum
            current_checksum = self._calculate_file_checksum(file_path)
            if not current_checksum:
                return True, "Could not calculate current checksum"
            
            # Compare checksums
            if current_checksum != baseline_checksum:
                return True, f"Checksum mismatch: expected {baseline_checksum}, got {current_checksum}"
            
            return False, None
            
        except Exception as e:
            return True, f"Error detecting file tampering: {str(e)}"
    
    def create_audit_trail(self, event: InfrastructureEvent) -> str:
        """Create tamper-evident audit trail entry"""
        return self.crypto_logger.create_audit_trail(event)
    
    def start_monitoring(self) -> bool:
        """Start integrity monitoring"""
        try:
            if self._monitoring:
                self.logger.warning("Integrity monitoring is already running")
                return True
            
            # Check if integrity monitoring is enabled
            if not self.integrity_config.get('enabled', True):
                self.logger.info("Integrity monitoring is disabled in configuration")
                return False
            
            # Start monitoring thread
            self._monitoring = True
            self._stop_event.clear()
            self._monitor_thread = threading.Thread(target=self._integrity_monitoring_loop, daemon=True)
            self._monitor_thread.start()
            
            self.logger.info("Integrity monitoring started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting integrity monitoring: {e}")
            self._monitoring = False
            return False
    
    def stop_monitoring(self) -> bool:
        """Stop integrity monitoring"""
        try:
            if not self._monitoring:
                self.logger.warning("Integrity monitoring is not running")
                return True
            
            # Signal stop and wait for thread
            self._stop_event.set()
            self._monitoring = False
            
            if self._monitor_thread and self._monitor_thread.is_alive():
                self._monitor_thread.join(timeout=10)
            
            # Close integrity database
            if self._integrity_db:
                self._integrity_db.close()
                self._integrity_db = None
            
            self.logger.info("Integrity monitoring stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping integrity monitoring: {e}")
            return False
    
    def _integrity_monitoring_loop(self):
        """Main integrity monitoring loop"""
        verification_interval = self.integrity_config.get('verification_interval_seconds', 300)
        
        while not self._stop_event.is_set():
            try:
                # Verify file integrity
                self._verify_monitored_files()
                
                # Check uba_persistent_log integrity
                self._verify_uba_persistent_log()
                
                # Verify configuration integrity
                self._verify_configuration_integrity()
                
                # Clean up old records
                self._cleanup_old_records()
                
            except Exception as e:
                self.logger.error(f"Error in integrity monitoring loop: {e}")
            
            # Wait for next iteration
            self._stop_event.wait(verification_interval)
    
    def _verify_monitored_files(self):
        """Verify integrity of all monitored files"""
        try:
            for file_path in self._monitored_paths:
                if os.path.exists(file_path):
                    is_tampered, error_msg = self._detect_file_tampering(file_path)
                    
                    if is_tampered:
                        self._handle_integrity_violation(
                            violation_type="file_tampering",
                            affected_resource=file_path,
                            details={"error": error_msg}
                        )
            
        except Exception as e:
            self.logger.error(f"Error verifying monitored files: {e}")
    
    def _verify_uba_persistent_log(self):
        """Verify integrity of uba_persistent_log entries"""
        try:
            # This would connect to the main UBA database and verify log entries
            # For now, we'll verify the crypto logger's audit trail
            is_valid, errors = self.crypto_logger.verify_log_integrity()
            
            if not is_valid:
                for error in errors:
                    self._handle_integrity_violation(
                        violation_type="audit_log_tampering",
                        affected_resource="uba_persistent_log",
                        details={"error": error}
                    )
            
        except Exception as e:
            self.logger.error(f"Error verifying uba_persistent_log: {e}")
    
    def _verify_configuration_integrity(self):
        """Verify integrity of configuration files"""
        try:
            config_path = self.config_manager.config_path
            
            if os.path.exists(config_path):
                is_tampered, error_msg = self._detect_file_tampering(config_path)
                
                if is_tampered:
                    self._handle_integrity_violation(
                        violation_type="config_tampering",
                        affected_resource=config_path,
                        details={"error": error_msg}
                    )
                    
                    # Attempt automatic restoration
                    if self.integrity_config.get('auto_restore_enabled', True):
                        self._restore_configuration()
            
        except Exception as e:
            self.logger.error(f"Error verifying configuration integrity: {e}")
    
    def _handle_integrity_violation(self, violation_type: str, affected_resource: str, 
                                  details: Dict[str, Any]):
        """Handle detected integrity violation"""
        try:
            violation = IntegrityViolation(
                violation_id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                violation_type=violation_type,
                affected_resource=affected_resource,
                expected_checksum=self._tracked_files.get(affected_resource, ""),
                actual_checksum=self._calculate_file_checksum(affected_resource) if os.path.exists(affected_resource) else "",
                severity="HIGH" if "config" in violation_type or "audit_log" in violation_type else "MEDIUM",
                auto_restored=False,
                details=details
            )
            
            # Store violation
            self._violations.append(violation)
            
            # Store in database
            if self._integrity_db:
                cursor = self._integrity_db.cursor()
                cursor.execute("""
                    INSERT INTO integrity_violations 
                    (violation_id, timestamp, violation_type, affected_resource,
                     expected_checksum, actual_checksum, severity, details)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    violation.violation_id,
                    violation.timestamp.isoformat(),
                    violation.violation_type,
                    violation.affected_resource,
                    violation.expected_checksum,
                    violation.actual_checksum,
                    violation.severity,
                    json.dumps(violation.details)
                ))
                self._integrity_db.commit()
            
            # Log violation
            self.crypto_logger.log_monitoring_event(
                f"integrity_violation_{violation_type}",
                affected_resource,
                violation.details,
                risk_score=0.9 if violation.severity == "HIGH" else 0.7
            )
            
            self.logger.warning(f"Integrity violation detected: {violation_type} in {affected_resource}")
            
        except Exception as e:
            self.logger.error(f"Error handling integrity violation: {e}")
    
    def create_configuration_backup(self) -> bool:
        """Create backup of current configuration"""
        try:
            config_path = self.config_manager.config_path
            
            if not os.path.exists(config_path):
                return False
            
            # Create backup directory
            backup_dir = Path(config_path).parent / "backups"
            backup_dir.mkdir(exist_ok=True)
            
            # Generate backup filename
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            backup_filename = f"config_backup_{timestamp}.json"
            backup_path = backup_dir / backup_filename
            
            # Copy configuration file
            shutil.copy2(config_path, backup_path)
            
            # Calculate checksum
            checksum = self._calculate_file_checksum(str(backup_path))
            
            # Create backup record
            backup = ConfigurationBackup(
                backup_id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                config_path=config_path,
                backup_path=str(backup_path),
                checksum=checksum,
                is_verified=True
            )
            
            self._config_backups.append(backup)
            
            # Store in database
            if self._integrity_db:
                cursor = self._integrity_db.cursor()
                cursor.execute("""
                    INSERT INTO config_backups 
                    (backup_id, timestamp, config_path, backup_path, checksum, is_verified)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    backup.backup_id,
                    backup.timestamp.isoformat(),
                    backup.config_path,
                    backup.backup_path,
                    backup.checksum,
                    1 if backup.is_verified else 0
                ))
                self._integrity_db.commit()
            
            self.logger.info(f"Configuration backup created: {backup_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating configuration backup: {e}")
            return False
    
    def _restore_configuration(self) -> bool:
        """Restore configuration from most recent verified backup"""
        try:
            # Find most recent verified backup
            if not self._config_backups:
                # Load from database
                if self._integrity_db:
                    cursor = self._integrity_db.cursor()
                    cursor.execute("""
                        SELECT backup_id, timestamp, config_path, backup_path, checksum, is_verified
                        FROM config_backups 
                        WHERE is_verified = 1 
                        ORDER BY timestamp DESC 
                        LIMIT 1
                    """)
                    result = cursor.fetchone()
                    if not result:
                        return False
                    
                    backup_path = result[3]
                else:
                    return False
            else:
                # Use in-memory backups
                verified_backups = [b for b in self._config_backups if b.is_verified]
                if not verified_backups:
                    return False
                
                latest_backup = max(verified_backups, key=lambda b: b.timestamp)
                backup_path = latest_backup.backup_path
            
            # Verify backup integrity before restoration
            if not os.path.exists(backup_path):
                return False
            
            # Restore configuration
            config_path = self.config_manager.config_path
            shutil.copy2(backup_path, config_path)
            
            # Update baseline
            self._create_file_baseline(config_path)
            
            # Log restoration
            self.crypto_logger.log_response_action(
                "configuration_restored",
                config_path,
                success=True,
                details={"backup_source": backup_path}
            )
            
            self.logger.info(f"Configuration restored from backup: {backup_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error restoring configuration: {e}")
            return False
    
    def _cleanup_old_records(self):
        """Clean up old integrity records"""
        try:
            # Keep records for 90 days
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=90)
            
            if self._integrity_db:
                cursor = self._integrity_db.cursor()
                
                # Clean up old violations
                cursor.execute(
                    "DELETE FROM integrity_violations WHERE timestamp < ?",
                    (cutoff_date.isoformat(),)
                )
                
                # Clean up old backups (keep at least 10 most recent)
                cursor.execute("""
                    DELETE FROM config_backups 
                    WHERE timestamp < ? 
                    AND backup_id NOT IN (
                        SELECT backup_id FROM config_backups 
                        ORDER BY timestamp DESC 
                        LIMIT 10
                    )
                """, (cutoff_date.isoformat(),))
                
                self._integrity_db.commit()
            
            # Clean up in-memory records
            self._violations = [v for v in self._violations if v.timestamp > cutoff_date]
            self._config_backups = [b for b in self._config_backups if b.timestamp > cutoff_date]
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old records: {e}")
    
    def get_integrity_statistics(self) -> Dict[str, Any]:
        """Get integrity validation statistics"""
        try:
            stats = {
                "monitoring_active": self._monitoring,
                "tracked_files_count": len(self._tracked_files),
                "violations_count": len(self._violations),
                "config_backups_count": len(self._config_backups),
                "monitored_paths": self._monitored_paths
            }
            
            # Add database statistics if available
            if self._integrity_db:
                cursor = self._integrity_db.cursor()
                
                # Count total violations
                cursor.execute("SELECT COUNT(*) FROM integrity_violations")
                stats["total_violations"] = cursor.fetchone()[0]
                
                # Count recent violations (last 24 hours)
                recent_cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
                cursor.execute(
                    "SELECT COUNT(*) FROM integrity_violations WHERE timestamp > ?",
                    (recent_cutoff,)
                )
                stats["recent_violations"] = cursor.fetchone()[0]
                
                # Count total backups
                cursor.execute("SELECT COUNT(*) FROM config_backups")
                stats["total_backups"] = cursor.fetchone()[0]
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting integrity statistics: {e}")
            return {}
    
    def is_healthy(self) -> bool:
        """Check if integrity validator is healthy"""
        try:
            # Check if monitoring is running
            if not self._monitoring:
                return False
            
            # Check if monitoring thread is alive
            if self._monitor_thread and not self._monitor_thread.is_alive():
                return False
            
            # Check database connection
            if not self._integrity_db:
                return False
            
            # Check for recent critical violations
            recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
            critical_violations = [
                v for v in self._violations 
                if v.timestamp > recent_cutoff and v.severity == "HIGH"
            ]
            
            # If there are unresolved critical violations, system is not healthy
            if len(critical_violations) > 5:
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking integrity validator health: {e}")
            return False