"""
Shadow Monitoring System for UBA Self-Monitoring

Provides independent monitoring infrastructure that operates in parallel to the primary
monitoring system, ensuring continuous security oversight even when primary systems
are compromised or tampered with.
"""

import logging
import threading
import time
import sqlite3
import json
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import uuid
import queue
from pathlib import Path

try:
    from .interfaces import (
        ShadowMonitoringInterface, 
        MonitoringInterface,
        InfrastructureEvent, 
        ThreatDetection,
        ComponentType,
        ThreatLevel
    )
    from .config_manager import SelfMonitoringConfig
    from .crypto_logger import CryptoLogger
    from .infrastructure_monitor import InfrastructureMonitor
except ImportError:
    # For direct execution or testing
    from interfaces import (
        ShadowMonitoringInterface, 
        MonitoringInterface,
        InfrastructureEvent, 
        ThreatDetection,
        ComponentType,
        ThreatLevel
    )
    from config_manager import SelfMonitoringConfig
    from crypto_logger import CryptoLogger
    from infrastructure_monitor import InfrastructureMonitor


@dataclass
class ShadowMonitorState:
    """Shadow monitor state tracking"""
    monitor_id: str
    last_heartbeat: datetime
    primary_system_status: str
    independent_detections: List[str]
    backup_alert_status: str
    integrity_violations: List[str]


@dataclass
class PrimarySystemHealth:
    """Primary system health status"""
    timestamp: datetime
    component: str
    is_healthy: bool
    response_time_ms: float
    error_message: Optional[str] = None


class ShadowMonitor(ShadowMonitoringInterface):
    """Independent shadow monitoring system"""
    
    def __init__(self, config_manager: Optional[SelfMonitoringConfig] = None):
        """
        Initialize shadow monitoring system
        
        Args:
            config_manager: Configuration manager instance
        """
        self.config_manager = config_manager or SelfMonitoringConfig()
        self.logger = logging.getLogger(__name__)
        self.crypto_logger = CryptoLogger()
        
        # Shadow monitoring state
        self.monitor_id = str(uuid.uuid4())
        self._monitoring = False
        self._monitor_thread = None
        self._stop_event = threading.Event()
        
        # Independent database connection
        self._shadow_db_path = self._get_shadow_db_path()
        self._shadow_db = None
        
        # Primary system monitoring
        self._primary_monitor: Optional[InfrastructureMonitor] = None
        self._primary_health_history: List[PrimarySystemHealth] = []
        self._last_primary_check = None
        
        # Event queues for independent processing
        self._shadow_event_queue = queue.Queue()
        self._threat_detection_queue = queue.Queue()
        
        # State tracking
        self._state = ShadowMonitorState(
            monitor_id=self.monitor_id,
            last_heartbeat=datetime.now(timezone.utc),
            primary_system_status="unknown",
            independent_detections=[],
            backup_alert_status="inactive",
            integrity_violations=[]
        )
        
        # Load configuration
        self._load_shadow_config()
        
        # Initialize shadow database
        self._initialize_shadow_database()
    
    def _get_shadow_db_path(self) -> str:
        """Get path for shadow monitoring database"""
        base_dir = Path(__file__).parent.parent.parent
        return str(base_dir / "data" / "shadow_monitoring.db")
    
    def _load_shadow_config(self):
        """Load shadow monitoring configuration"""
        try:
            config = self.config_manager.load_config()
            self.shadow_config = config.get('shadow_monitoring', {})
            self.monitoring_config = config.get('monitoring', {})
            self.detection_config = config.get('detection', {})
            
            self.logger.info("Shadow monitoring configuration loaded")
            
        except Exception as e:
            self.logger.error(f"Error loading shadow configuration: {e}")
            # Use safe defaults
            self.shadow_config = {
                'enabled': True,
                'heartbeat_interval_seconds': 60,
                'primary_health_check_interval_seconds': 30,
                'failover_timeout_seconds': 120
            }
            self.monitoring_config = {'enabled': True}
            self.detection_config = {'enabled': True}
    
    def _initialize_shadow_database(self):
        """Initialize independent shadow database"""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self._shadow_db_path), exist_ok=True)
            
            # Connect to SQLite database
            self._shadow_db = sqlite3.connect(
                self._shadow_db_path,
                check_same_thread=False,
                timeout=30.0
            )
            self._shadow_db.row_factory = sqlite3.Row
            
            # Create tables
            self._create_shadow_tables()
            
            self.logger.info(f"Shadow database initialized at {self._shadow_db_path}")
            
        except Exception as e:
            self.logger.error(f"Error initializing shadow database: {e}")
            self._shadow_db = None
    
    def _create_shadow_tables(self):
        """Create shadow monitoring database tables"""
        try:
            cursor = self._shadow_db.cursor()
            
            # Events table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS shadow_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT UNIQUE NOT NULL,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    source_ip TEXT,
                    user_account TEXT,
                    target_component TEXT,
                    action_details TEXT,
                    risk_score REAL,
                    integrity_hash TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Primary system health table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS primary_health (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    component TEXT NOT NULL,
                    is_healthy INTEGER NOT NULL,
                    response_time_ms REAL,
                    error_message TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Threat detections table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS shadow_threats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    detection_id TEXT UNIQUE NOT NULL,
                    timestamp TEXT NOT NULL,
                    threat_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    affected_components TEXT,
                    attack_indicators TEXT,
                    confidence_score REAL,
                    response_actions TEXT,
                    evidence_chain TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Shadow monitor state table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS shadow_state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    primary_system_status TEXT,
                    independent_detections TEXT,
                    backup_alert_status TEXT,
                    integrity_violations TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_timestamp ON shadow_events(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_health_timestamp ON primary_health(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_threats_timestamp ON shadow_threats(timestamp)")
            
            self._shadow_db.commit()
            
        except Exception as e:
            self.logger.error(f"Error creating shadow tables: {e}")
            if self._shadow_db:
                self._shadow_db.rollback()
    
    def start_monitoring(self, primary_monitor: Optional[InfrastructureMonitor] = None) -> bool:
        """
        Start shadow monitoring system
        
        Args:
            primary_monitor: Primary infrastructure monitor to watch
            
        Returns:
            bool: True if started successfully
        """
        try:
            if self._monitoring:
                self.logger.warning("Shadow monitoring is already running")
                return True
            
            # Check if shadow monitoring is enabled
            if not self.shadow_config.get('enabled', True):
                self.logger.info("Shadow monitoring is disabled in configuration")
                return False
            
            # Set primary monitor reference
            self._primary_monitor = primary_monitor
            
            # Start monitoring thread
            self._monitoring = True
            self._stop_event.clear()
            self._monitor_thread = threading.Thread(target=self._shadow_monitoring_loop, daemon=True)
            self._monitor_thread.start()
            
            # Log startup
            self.crypto_logger.log_monitoring_event(
                "shadow_monitor_started",
                "shadow_monitoring_system",
                {"monitor_id": self.monitor_id}
            )
            
            self.logger.info("Shadow monitoring started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting shadow monitoring: {e}")
            self._monitoring = False
            return False
    
    def stop_monitoring(self) -> bool:
        """Stop shadow monitoring system"""
        try:
            if not self._monitoring:
                self.logger.warning("Shadow monitoring is not running")
                return True
            
            # Signal stop and wait for thread
            self._stop_event.set()
            self._monitoring = False
            
            if self._monitor_thread and self._monitor_thread.is_alive():
                self._monitor_thread.join(timeout=10)
            
            # Close shadow database
            if self._shadow_db:
                self._shadow_db.close()
                self._shadow_db = None
            
            # Log shutdown
            self.crypto_logger.log_monitoring_event(
                "shadow_monitor_stopped",
                "shadow_monitoring_system",
                {"monitor_id": self.monitor_id}
            )
            
            self.logger.info("Shadow monitoring stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping shadow monitoring: {e}")
            return False
    
    def _shadow_monitoring_loop(self):
        """Main shadow monitoring loop"""
        heartbeat_interval = self.shadow_config.get('heartbeat_interval_seconds', 60)
        health_check_interval = self.shadow_config.get('primary_health_check_interval_seconds', 30)
        
        last_heartbeat = time.time()
        last_health_check = time.time()
        
        while not self._stop_event.is_set():
            try:
                current_time = time.time()
                
                # Perform heartbeat
                if current_time - last_heartbeat >= heartbeat_interval:
                    self._perform_heartbeat()
                    last_heartbeat = current_time
                
                # Check primary system health
                if current_time - last_health_check >= health_check_interval:
                    self._check_primary_system_health()
                    last_health_check = current_time
                
                # Process independent threat detection
                self._process_independent_detection()
                
                # Process shadow events
                self._process_shadow_events()
                
                # Check for integrity violations
                self._check_integrity_violations()
                
                # Update shadow state
                self._update_shadow_state()
                
            except Exception as e:
                self.logger.error(f"Error in shadow monitoring loop: {e}")
            
            # Wait for next iteration (shorter interval for responsiveness)
            self._stop_event.wait(5)
    
    def _perform_heartbeat(self):
        """Perform shadow monitor heartbeat"""
        try:
            self._state.last_heartbeat = datetime.now(timezone.utc)
            
            # Log heartbeat to shadow database
            if self._shadow_db:
                cursor = self._shadow_db.cursor()
                cursor.execute("""
                    INSERT INTO shadow_state 
                    (monitor_id, timestamp, primary_system_status, independent_detections, 
                     backup_alert_status, integrity_violations)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    self.monitor_id,
                    self._state.last_heartbeat.isoformat(),
                    self._state.primary_system_status,
                    json.dumps(self._state.independent_detections),
                    self._state.backup_alert_status,
                    json.dumps(self._state.integrity_violations)
                ))
                self._shadow_db.commit()
            
            self.logger.debug("Shadow monitor heartbeat performed")
            
        except Exception as e:
            self.logger.error(f"Error performing heartbeat: {e}")
    
    def monitor_primary_system(self) -> bool:
        """Monitor the health of primary monitoring systems"""
        return self._check_primary_system_health()
    
    def _check_primary_system_health(self) -> bool:
        """Check primary system health"""
        try:
            if not self._primary_monitor:
                self._state.primary_system_status = "no_primary_monitor"
                self.logger.warning("No primary monitor set for shadow monitoring")
                return True  # Return True to allow system to continue
            
            start_time = time.time()
            
            # Check if primary monitor is healthy
            is_healthy = self._primary_monitor.is_healthy()
            
            response_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            
            # Record health status
            health_status = PrimarySystemHealth(
                timestamp=datetime.now(timezone.utc),
                component="primary_infrastructure_monitor",
                is_healthy=is_healthy,
                response_time_ms=response_time,
                error_message=None if is_healthy else "Health check failed"
            )
            
            self._primary_health_history.append(health_status)
            
            # Keep only recent history (last 100 entries)
            if len(self._primary_health_history) > 100:
                self._primary_health_history = self._primary_health_history[-100:]
            
            # Store in shadow database
            if self._shadow_db:
                cursor = self._shadow_db.cursor()
                cursor.execute("""
                    INSERT INTO primary_health 
                    (timestamp, component, is_healthy, response_time_ms, error_message)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    health_status.timestamp.isoformat(),
                    health_status.component,
                    1 if health_status.is_healthy else 0,
                    health_status.response_time_ms,
                    health_status.error_message
                ))
                self._shadow_db.commit()
            
            # Update primary system status
            if is_healthy:
                self._state.primary_system_status = "healthy"
            else:
                self._state.primary_system_status = "unhealthy"
                
                # Log primary system failure
                self.crypto_logger.log_monitoring_event(
                    "primary_system_unhealthy",
                    "primary_infrastructure_monitor",
                    {
                        "response_time_ms": response_time,
                        "error": health_status.error_message
                    },
                    risk_score=0.8
                )
            
            return is_healthy
            
        except Exception as e:
            self.logger.error(f"Error checking primary system health: {e}")
            self._state.primary_system_status = "check_failed"
            return False
    
    def detect_primary_compromise(self) -> List[ThreatDetection]:
        """Detect if primary monitoring has been compromised"""
        threats = []
        
        try:
            # Analyze primary system health patterns
            if len(self._primary_health_history) >= 5:
                recent_health = self._primary_health_history[-5:]
                
                # Check for sudden health degradation
                healthy_count = sum(1 for h in recent_health if h.is_healthy)
                if healthy_count <= 1:  # Most recent checks failed
                    threat = ThreatDetection(
                        detection_id=str(uuid.uuid4()),
                        timestamp=datetime.now(timezone.utc),
                        threat_type="primary_system_compromise",
                        severity=ThreatLevel.HIGH,
                        affected_components=[ComponentType.MONITORING_SERVICE],
                        attack_indicators={
                            "consecutive_health_failures": 5 - healthy_count,
                            "pattern": "sudden_degradation"
                        },
                        confidence_score=0.8,
                        response_actions=["activate_backup_monitoring", "investigate_primary_system"],
                        evidence_chain=[h.timestamp.isoformat() for h in recent_health]
                    )
                    threats.append(threat)
                
                # Check for unusual response times
                avg_response_time = sum(h.response_time_ms for h in recent_health) / len(recent_health)
                if avg_response_time > 5000:  # 5 seconds
                    threat = ThreatDetection(
                        detection_id=str(uuid.uuid4()),
                        timestamp=datetime.now(timezone.utc),
                        threat_type="primary_system_performance_degradation",
                        severity=ThreatLevel.MEDIUM,
                        affected_components=[ComponentType.MONITORING_SERVICE],
                        attack_indicators={
                            "average_response_time_ms": avg_response_time,
                            "threshold_ms": 5000
                        },
                        confidence_score=0.6,
                        response_actions=["investigate_performance", "check_resource_usage"],
                        evidence_chain=[h.timestamp.isoformat() for h in recent_health]
                    )
                    threats.append(threat)
            
            # Store detected threats
            for threat in threats:
                self._store_threat_detection(threat)
                self._state.independent_detections.append(threat.detection_id)
            
        except Exception as e:
            self.logger.error(f"Error detecting primary compromise: {e}")
        
        return threats
    
    def _store_threat_detection(self, threat: ThreatDetection):
        """Store threat detection in shadow database"""
        try:
            if self._shadow_db:
                cursor = self._shadow_db.cursor()
                cursor.execute("""
                    INSERT INTO shadow_threats 
                    (detection_id, timestamp, threat_type, severity, affected_components,
                     attack_indicators, confidence_score, response_actions, evidence_chain)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    threat.detection_id,
                    threat.timestamp.isoformat(),
                    threat.threat_type,
                    threat.severity.value,
                    json.dumps([c.value for c in threat.affected_components]),
                    json.dumps(threat.attack_indicators),
                    threat.confidence_score,
                    json.dumps(threat.response_actions),
                    json.dumps(threat.evidence_chain)
                ))
                self._shadow_db.commit()
                
        except Exception as e:
            self.logger.error(f"Error storing threat detection: {e}")
    
    def activate_backup_monitoring(self) -> bool:
        """Activate backup monitoring when primary fails"""
        try:
            self._state.backup_alert_status = "active"
            
            # Log backup activation
            self.crypto_logger.log_monitoring_event(
                "backup_monitoring_activated",
                "shadow_monitoring_system",
                {
                    "monitor_id": self.monitor_id,
                    "primary_status": self._state.primary_system_status,
                    "activation_reason": "primary_system_failure"
                },
                risk_score=0.9
            )
            
            # In a full implementation, this would:
            # 1. Start independent monitoring processes
            # 2. Activate alternative data collection
            # 3. Switch to backup alert channels
            # 4. Notify administrators
            
            self.logger.warning("Backup monitoring activated due to primary system failure")
            return True
            
        except Exception as e:
            self.logger.error(f"Error activating backup monitoring: {e}")
            return False
    
    def _process_independent_detection(self):
        """Process independent threat detection"""
        try:
            # Detect primary system compromise
            threats = self.detect_primary_compromise()
            
            # Process any detected threats
            for threat in threats:
                self._threat_detection_queue.put(threat)
                
                # If critical threat, activate backup monitoring
                if threat.severity in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                    self.activate_backup_monitoring()
            
        except Exception as e:
            self.logger.error(f"Error in independent detection: {e}")
    
    def _process_shadow_events(self):
        """Process events in shadow event queue"""
        try:
            events_processed = 0
            max_events = 100
            
            while not self._shadow_event_queue.empty() and events_processed < max_events:
                try:
                    event = self._shadow_event_queue.get_nowait()
                    
                    # Store event in shadow database
                    if self._shadow_db:
                        cursor = self._shadow_db.cursor()
                        cursor.execute("""
                            INSERT INTO shadow_events 
                            (event_id, timestamp, event_type, source_ip, user_account,
                             target_component, action_details, risk_score, integrity_hash)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            event.event_id,
                            event.timestamp.isoformat(),
                            event.event_type,
                            event.source_ip,
                            event.user_account,
                            event.target_component.value,
                            json.dumps(event.action_details),
                            event.risk_score,
                            event.integrity_hash
                        ))
                        self._shadow_db.commit()
                    
                    events_processed += 1
                    
                except queue.Empty:
                    break
                except Exception as e:
                    self.logger.error(f"Error processing shadow event: {e}")
            
        except Exception as e:
            self.logger.error(f"Error processing shadow events: {e}")
    
    def _check_integrity_violations(self):
        """Check for integrity violations in monitoring data"""
        try:
            # Verify crypto logger integrity
            is_valid, errors = self.crypto_logger.verify_log_integrity()
            
            if not is_valid:
                # Record integrity violations
                for error in errors:
                    if error not in self._state.integrity_violations:
                        self._state.integrity_violations.append(error)
                        
                        # Log integrity violation
                        self.crypto_logger.log_monitoring_event(
                            "integrity_violation_detected",
                            "audit_log",
                            {"error": error},
                            risk_score=0.9
                        )
            
        except Exception as e:
            self.logger.error(f"Error checking integrity violations: {e}")
    
    def _update_shadow_state(self):
        """Update shadow monitoring state"""
        try:
            # Keep only recent detections and violations (last 50)
            if len(self._state.independent_detections) > 50:
                self._state.independent_detections = self._state.independent_detections[-50:]
            
            if len(self._state.integrity_violations) > 50:
                self._state.integrity_violations = self._state.integrity_violations[-50:]
            
        except Exception as e:
            self.logger.error(f"Error updating shadow state: {e}")
    
    def get_shadow_statistics(self) -> Dict[str, Any]:
        """Get shadow monitoring statistics"""
        try:
            stats = {
                "monitor_id": self.monitor_id,
                "monitoring_active": self._monitoring,
                "primary_system_status": self._state.primary_system_status,
                "backup_alert_status": self._state.backup_alert_status,
                "last_heartbeat": self._state.last_heartbeat.isoformat(),
                "independent_detections_count": len(self._state.independent_detections),
                "integrity_violations_count": len(self._state.integrity_violations),
                "primary_health_history_count": len(self._primary_health_history),
                "shadow_events_queued": self._shadow_event_queue.qsize(),
                "threat_detections_queued": self._threat_detection_queue.qsize()
            }
            
            # Add database statistics if available
            if self._shadow_db:
                cursor = self._shadow_db.cursor()
                
                # Count events
                cursor.execute("SELECT COUNT(*) FROM shadow_events")
                stats["total_shadow_events"] = cursor.fetchone()[0]
                
                # Count threats
                cursor.execute("SELECT COUNT(*) FROM shadow_threats")
                stats["total_threat_detections"] = cursor.fetchone()[0]
                
                # Count health records
                cursor.execute("SELECT COUNT(*) FROM primary_health")
                stats["total_health_records"] = cursor.fetchone()[0]
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting shadow statistics: {e}")
            return {}
    
    def is_healthy(self) -> bool:
        """Check if shadow monitoring is healthy"""
        try:
            # Check if monitoring is running
            if not self._monitoring:
                return False
            
            # Check if monitoring thread is alive
            if self._monitor_thread and not self._monitor_thread.is_alive():
                return False
            
            # Check database connection
            if not self._shadow_db:
                return False
            
            # Check recent heartbeat
            if self._state.last_heartbeat:
                time_since_heartbeat = datetime.now(timezone.utc) - self._state.last_heartbeat
                if time_since_heartbeat.total_seconds() > 300:  # 5 minutes
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking shadow monitor health: {e}")
            return False