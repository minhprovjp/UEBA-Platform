"""
Database Connection Interceptor for UBA Self-Monitoring System

This module provides connection interception capabilities to monitor all database
connections to uba_db, capture authentication events, and analyze query patterns
for detecting malicious activities.
"""

import logging
import threading
import time
import re
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Set, Callable, Tuple
from dataclasses import dataclass
import mysql.connector
from mysql.connector import Error as MySQLError
import queue
import uuid
import weakref

try:
    from .interfaces import (
        InfrastructureEvent, 
        ComponentType,
        ThreatLevel
    )
    from .crypto_logger import CryptoLogger
except ImportError:
    # For direct execution or testing
    from interfaces import (
        InfrastructureEvent, 
        ComponentType,
        ThreatLevel
    )
    from crypto_logger import CryptoLogger


@dataclass
class ConnectionEvent:
    """Database connection event information"""
    event_id: str
    timestamp: datetime
    connection_id: int
    user: str
    host: str
    database: str
    event_type: str  # 'connect', 'disconnect', 'query', 'auth_failure'
    details: Dict[str, Any]
    risk_score: float


@dataclass
class QueryEvent:
    """Database query event information"""
    event_id: str
    timestamp: datetime
    connection_id: int
    user: str
    host: str
    database: str
    query: str
    query_type: str
    execution_time: float
    rows_affected: int
    risk_score: float
    matched_patterns: List[str]


class DatabaseConnectionInterceptor:
    """
    Database connection interceptor for monitoring uba_db access
    
    This class provides comprehensive monitoring of database connections,
    authentication events, and query patterns to detect malicious activities
    targeting the UBA infrastructure.
    """
    
    def __init__(self, crypto_logger: Optional[CryptoLogger] = None):
        """
        Initialize the database connection interceptor
        
        Args:
            crypto_logger: Cryptographic logger for secure audit trails
        """
        self.crypto_logger = crypto_logger or CryptoLogger()
        self.logger = logging.getLogger(__name__)
        
        # Monitoring state
        self._active = False
        self._monitor_thread = None
        self._stop_event = threading.Event()
        
        # Event queues
        self._connection_events = queue.Queue()
        self._query_events = queue.Queue()
        
        # Connection tracking
        self._active_connections: Dict[int, ConnectionEvent] = {}
        self._connection_history: List[ConnectionEvent] = []
        self._query_history: List[QueryEvent] = []
        
        # Pattern detection
        self._malicious_patterns = self._load_malicious_patterns()
        self._reconnaissance_patterns = self._load_reconnaissance_patterns()
        self._privilege_escalation_patterns = self._load_privilege_escalation_patterns()
        
        # Authentication monitoring
        self._failed_auth_attempts: Dict[str, List[datetime]] = {}
        self._suspicious_auth_patterns: Set[str] = set()
        
        # Database connection for monitoring
        self._monitor_connection = None
        
        # Callbacks for event handling
        self._event_callbacks: List[Callable[[ConnectionEvent], None]] = []
        self._query_callbacks: List[Callable[[QueryEvent], None]] = []
    
    def _load_malicious_patterns(self) -> List[str]:
        """Load malicious query patterns for detection"""
        return [
            # SQL Injection patterns
            r"(?i)union\s+select",
            r"(?i)or\s+1\s*=\s*1",
            r"(?i)and\s+1\s*=\s*1",
            r"(?i)'\s*or\s*'1'\s*=\s*'1",
            r"(?i);\s*drop\s+table",
            r"(?i);\s*delete\s+from",
            
            # Information gathering
            r"(?i)select\s+.*\s+from\s+information_schema",
            r"(?i)show\s+databases",
            r"(?i)show\s+tables",
            r"(?i)describe\s+",
            r"(?i)show\s+columns",
            
            # User and privilege queries
            r"(?i)select\s+.*\s+from\s+mysql\.user",
            r"(?i)show\s+grants",
            r"(?i)select\s+user\s*\(\s*\)",
            r"(?i)select\s+current_user",
            
            # Performance schema access
            r"(?i)select\s+.*\s+from\s+performance_schema",
            r"(?i)show\s+engine\s+innodb\s+status",
            
            # Dangerous functions
            r"(?i)load_file\s*\(",
            r"(?i)into\s+outfile",
            r"(?i)into\s+dumpfile",
            r"(?i)benchmark\s*\(",
            r"(?i)sleep\s*\(",
        ]
    
    def _load_reconnaissance_patterns(self) -> List[str]:
        """Load reconnaissance query patterns"""
        return [
            # Database enumeration
            r"(?i)select\s+schema_name\s+from\s+information_schema\.schemata",
            r"(?i)select\s+table_name\s+from\s+information_schema\.tables",
            r"(?i)select\s+column_name\s+from\s+information_schema\.columns",
            
            # Version and configuration
            r"(?i)select\s+version\s*\(\s*\)",
            r"(?i)select\s+@@version",
            r"(?i)show\s+variables",
            r"(?i)show\s+status",
            
            # User enumeration
            r"(?i)select\s+host,\s*user\s+from\s+mysql\.user",
            r"(?i)select\s+grantee\s+from\s+information_schema\.user_privileges",
            
            # Process monitoring
            r"(?i)show\s+processlist",
            r"(?i)show\s+full\s+processlist",
        ]
    
    def _load_privilege_escalation_patterns(self) -> List[str]:
        """Load privilege escalation patterns"""
        return [
            # User creation and modification
            r"(?i)create\s+user",
            r"(?i)drop\s+user",
            r"(?i)alter\s+user",
            r"(?i)rename\s+user",
            
            # Privilege grants
            r"(?i)grant\s+.*\s+to",
            r"(?i)revoke\s+.*\s+from",
            r"(?i)grant\s+all\s+privileges",
            r"(?i)grant\s+.*\s+with\s+grant\s+option",
            
            # Database and table operations
            r"(?i)create\s+database",
            r"(?i)drop\s+database",
            r"(?i)alter\s+database",
            
            # System operations
            r"(?i)set\s+global",
            r"(?i)flush\s+privileges",
            r"(?i)reset\s+master",
            r"(?i)shutdown",
        ]
    
    def start_monitoring(self, db_config: Dict[str, Any]) -> bool:
        """
        Start database connection monitoring
        
        Args:
            db_config: Database configuration for monitoring connection
            
        Returns:
            True if monitoring started successfully, False otherwise
        """
        try:
            if self._active:
                self.logger.warning("Connection interceptor is already active")
                return True
            
            # Establish monitoring connection
            self._monitor_connection = mysql.connector.connect(
                host=db_config.get('host', 'localhost'),
                port=db_config.get('port', 3306),
                database=db_config.get('database', 'uba_db'),
                user=db_config.get('user', 'uba_user'),
                password=db_config.get('password', ''),
                connection_timeout=db_config.get('connection_timeout_seconds', 30),
                autocommit=True
            )
            
            # Start monitoring thread
            self._active = True
            self._stop_event.clear()
            self._monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self._monitor_thread.start()
            
            self.logger.info("Database connection interceptor started")
            return True
            
        except MySQLError as e:
            self.logger.error(f"Error starting connection interceptor: {e}")
            self._active = False
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error starting interceptor: {e}")
            self._active = False
            return False
    
    def stop_monitoring(self) -> bool:
        """
        Stop database connection monitoring
        
        Returns:
            True if monitoring stopped successfully, False otherwise
        """
        try:
            if not self._active:
                self.logger.warning("Connection interceptor is not active")
                return True
            
            # Signal stop and wait for thread
            self._stop_event.set()
            self._active = False
            
            if self._monitor_thread and self._monitor_thread.is_alive():
                self._monitor_thread.join(timeout=10)
            
            # Close monitoring connection
            if self._monitor_connection and self._monitor_connection.is_connected():
                self._monitor_connection.close()
            
            self.logger.info("Database connection interceptor stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping connection interceptor: {e}")
            return False
    
    def _monitoring_loop(self):
        """Main monitoring loop for connection interception"""
        while not self._stop_event.is_set():
            try:
                # Monitor active connections
                self._monitor_active_connections()
                
                # Monitor authentication events
                self._monitor_authentication_events()
                
                # Monitor query patterns
                self._monitor_query_patterns()
                
                # Process queued events
                self._process_connection_events()
                self._process_query_events()
                
                # Clean up old events
                self._cleanup_old_events()
                
            except Exception as e:
                self.logger.error(f"Error in connection monitoring loop: {e}")
            
            # Wait before next iteration
            self._stop_event.wait(5)  # 5 second intervals
    
    def _monitor_active_connections(self):
        """Monitor currently active database connections"""
        try:
            if not self._monitor_connection or not self._monitor_connection.is_connected():
                return
            
            cursor = self._monitor_connection.cursor(dictionary=True)
            
            # Get current process list
            cursor.execute("SHOW FULL PROCESSLIST")
            current_processes = cursor.fetchall()
            
            current_connection_ids = set()
            
            for process in current_processes:
                conn_id = process['Id']
                current_connection_ids.add(conn_id)
                
                # Check for new connections
                if conn_id not in self._active_connections:
                    self._handle_new_connection(process)
                else:
                    # Update existing connection info
                    self._update_connection_info(conn_id, process)
            
            # Detect closed connections
            closed_connections = set(self._active_connections.keys()) - current_connection_ids
            for conn_id in closed_connections:
                self._handle_connection_close(conn_id)
            
            cursor.close()
            
        except MySQLError as e:
            self.logger.error(f"Error monitoring active connections: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error monitoring connections: {e}")
    
    def _handle_new_connection(self, process_info: Dict[str, Any]):
        """Handle new database connection"""
        try:
            conn_id = process_info['Id']
            user = process_info.get('User', '')
            host = process_info.get('Host', '')
            database = process_info.get('db', '')
            
            # Calculate risk score for connection
            risk_score = self._calculate_connection_risk(process_info)
            
            # Create connection event
            event = ConnectionEvent(
                event_id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                connection_id=conn_id,
                user=user,
                host=host,
                database=database,
                event_type='connect',
                details={
                    'command': process_info.get('Command', ''),
                    'time': process_info.get('Time', 0),
                    'state': process_info.get('State', ''),
                    'info': process_info.get('Info', '')
                },
                risk_score=risk_score
            )
            
            # Store active connection
            self._active_connections[conn_id] = event
            
            # Queue event for processing
            self._connection_events.put(event)
            
            # Log high-risk connections immediately
            if risk_score >= 0.7:
                self.crypto_logger.log_monitoring_event(
                    "high_risk_connection",
                    "uba_db",
                    {
                        "connection_id": conn_id,
                        "user": user,
                        "host": host,
                        "database": database,
                        "risk_score": risk_score
                    },
                    risk_score
                )
            
            # Check for uba_user account anomalies
            if user == 'uba_user':
                self._analyze_uba_user_connection(event)
            
        except Exception as e:
            self.logger.error(f"Error handling new connection: {e}")
    
    def _calculate_connection_risk(self, process_info: Dict[str, Any]) -> float:
        """Calculate risk score for database connection"""
        risk_score = 0.0
        
        try:
            user = process_info.get('User', '')
            host = process_info.get('Host', '')
            database = process_info.get('db', '')
            command = process_info.get('Command', '')
            
            # Check for unauthorized users
            authorized_users = ['uba_user', 'root', 'mysql.sys', 'mysql.session']
            if user not in authorized_users:
                risk_score += 0.5
            
            # Check for remote connections
            local_hosts = ['localhost', '127.0.0.1', '::1']
            connection_host = host.split(':')[0] if ':' in host else host
            if connection_host not in local_hosts:
                risk_score += 0.3
            
            # Check for access to sensitive databases
            if database in ['mysql', 'information_schema', 'performance_schema']:
                risk_score += 0.4
            
            # Check for administrative commands
            admin_commands = ['Admin', 'Binlog Dump', 'Change user', 'Create DB', 'Drop DB']
            if command in admin_commands:
                risk_score += 0.3
            
            # Check for multiple concurrent sessions from same user
            same_user_connections = [
                conn for conn in self._active_connections.values()
                if conn.user == user
            ]
            if len(same_user_connections) >= 3:
                risk_score += 0.4
            
            return min(risk_score, 1.0)
            
        except Exception as e:
            self.logger.error(f"Error calculating connection risk: {e}")
            return 0.5  # Default moderate risk
    
    def _analyze_uba_user_connection(self, event: ConnectionEvent):
        """Analyze uba_user connection for anomalies"""
        try:
            anomaly_indicators = []
            additional_risk = 0.0
            
            # Check connection source
            if event.host not in ['localhost', '127.0.0.1', '::1']:
                anomaly_indicators.append("remote_connection")
                additional_risk += 0.4
            
            # Check for unusual command types
            command = event.details.get('command', '')
            if command in ['Admin', 'Binlog Dump', 'Create DB', 'Drop DB']:
                anomaly_indicators.append(f"admin_command:{command}")
                additional_risk += 0.5
            
            # Check for long-running connections
            connection_time = event.details.get('time', 0)
            if connection_time > 3600:  # 1 hour
                anomaly_indicators.append("long_running_connection")
                additional_risk += 0.3
            
            # Check for multiple concurrent uba_user sessions
            uba_connections = [
                conn for conn in self._active_connections.values()
                if conn.user == 'uba_user'
            ]
            if len(uba_connections) > 2:
                anomaly_indicators.append(f"multiple_sessions:{len(uba_connections)}")
                additional_risk += 0.4
            
            # If anomalies detected, create high-priority event
            if anomaly_indicators:
                updated_risk = min(event.risk_score + additional_risk, 1.0)
                
                anomaly_event = InfrastructureEvent(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.now(timezone.utc),
                    event_type="uba_user_anomaly",
                    source_ip=event.host.split(':')[0] if ':' in event.host else event.host,
                    user_account=event.user,
                    target_component=ComponentType.USER_ACCOUNT,
                    action_details={
                        "connection_id": event.connection_id,
                        "anomaly_indicators": anomaly_indicators,
                        "original_risk_score": event.risk_score,
                        "updated_risk_score": updated_risk,
                        "connection_details": event.details
                    },
                    risk_score=updated_risk,
                    integrity_hash=""
                )
                
                anomaly_event.integrity_hash = self.crypto_logger.create_checksum(anomaly_event.__dict__)
                
                # Notify callbacks
                for callback in self._event_callbacks:
                    try:
                        callback(event)
                    except Exception as e:
                        self.logger.error(f"Error in event callback: {e}")
            
        except Exception as e:
            self.logger.error(f"Error analyzing uba_user connection: {e}")
    
    def _monitor_authentication_events(self):
        """Monitor authentication events and failures"""
        try:
            if not self._monitor_connection or not self._monitor_connection.is_connected():
                return
            
            cursor = self._monitor_connection.cursor(dictionary=True)
            
            # Check for recent authentication failures in error log
            # Note: This requires access to MySQL error log or audit plugin
            # For now, we'll monitor connection patterns that might indicate auth failures
            
            # Monitor failed connection attempts by tracking short-lived connections
            current_time = datetime.now(timezone.utc)
            
            for conn_id, event in list(self._active_connections.items()):
                connection_duration = (current_time - event.timestamp).total_seconds()
                
                # Connections that close very quickly might indicate auth failures
                if connection_duration < 1 and event.details.get('state') == 'login':
                    self._handle_potential_auth_failure(event)
            
            cursor.close()
            
        except Exception as e:
            self.logger.error(f"Error monitoring authentication events: {e}")
    
    def _handle_potential_auth_failure(self, event: ConnectionEvent):
        """Handle potential authentication failure"""
        try:
            host_key = event.host.split(':')[0] if ':' in event.host else event.host
            
            # Track failed attempts by host
            if host_key not in self._failed_auth_attempts:
                self._failed_auth_attempts[host_key] = []
            
            self._failed_auth_attempts[host_key].append(event.timestamp)
            
            # Clean up old attempts (keep only last hour)
            cutoff_time = datetime.now(timezone.utc).timestamp() - 3600
            self._failed_auth_attempts[host_key] = [
                attempt for attempt in self._failed_auth_attempts[host_key]
                if attempt.timestamp() > cutoff_time
            ]
            
            # Check for brute force patterns
            recent_failures = len(self._failed_auth_attempts[host_key])
            if recent_failures >= 5:  # 5 failures in an hour
                self._handle_brute_force_detection(host_key, recent_failures)
            
        except Exception as e:
            self.logger.error(f"Error handling potential auth failure: {e}")
    
    def _handle_brute_force_detection(self, host: str, failure_count: int):
        """Handle brute force attack detection"""
        try:
            # Create high-priority security event
            brute_force_event = InfrastructureEvent(
                event_id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                event_type="brute_force_attack",
                source_ip=host,
                user_account="unknown",
                target_component=ComponentType.DATABASE,
                action_details={
                    "failure_count": failure_count,
                    "time_window": "1_hour",
                    "attack_type": "authentication_brute_force"
                },
                risk_score=0.9,
                integrity_hash=""
            )
            
            brute_force_event.integrity_hash = self.crypto_logger.create_checksum(brute_force_event.__dict__)
            
            # Log critical security event
            self.crypto_logger.log_monitoring_event(
                "brute_force_attack_detected",
                "uba_db",
                {
                    "source_host": host,
                    "failure_count": failure_count,
                    "detection_time": brute_force_event.timestamp.isoformat()
                },
                0.9
            )
            
        except Exception as e:
            self.logger.error(f"Error handling brute force detection: {e}")
    
    def _monitor_query_patterns(self):
        """Monitor query patterns for malicious activities"""
        try:
            if not self._monitor_connection or not self._monitor_connection.is_connected():
                return
            
            cursor = self._monitor_connection.cursor(dictionary=True)
            
            # Monitor general log for query patterns (if enabled)
            try:
                cursor.execute("""
                    SELECT event_time, user_host, thread_id, server_id, command_type, argument
                    FROM mysql.general_log 
                    WHERE event_time >= DATE_SUB(NOW(), INTERVAL 30 SECOND)
                    AND command_type IN ('Query', 'Execute', 'Prepare')
                    ORDER BY event_time DESC
                    LIMIT 100
                """)
                
                recent_queries = cursor.fetchall()
                
                for query_info in recent_queries:
                    self._analyze_query_for_threats(query_info)
                    
            except MySQLError as e:
                # General log might not be enabled or accessible
                self.logger.debug(f"Cannot access general log for query monitoring: {e}")
            
            cursor.close()
            
        except Exception as e:
            self.logger.error(f"Error monitoring query patterns: {e}")
    
    def _analyze_query_for_threats(self, query_info: Dict[str, Any]):
        """Analyze individual query for threat patterns"""
        try:
            query = query_info.get('argument', '').strip()
            if not query or len(query) < 10:  # Skip very short queries
                return
            
            user_host = query_info.get('user_host', '')
            thread_id = query_info.get('thread_id', 0)
            
            # Extract user and host
            user = user_host.split('[')[0] if '[' in user_host else user_host.split('@')[0]
            host = user_host.split('@')[1] if '@' in user_host else 'unknown'
            
            # Analyze query against threat patterns
            risk_score = 0.0
            matched_patterns = []
            threat_categories = []
            
            # Check malicious patterns
            for pattern in self._malicious_patterns:
                if re.search(pattern, query):
                    risk_score += 0.3
                    matched_patterns.append(pattern)
                    threat_categories.append("malicious_query")
            
            # Check reconnaissance patterns
            for pattern in self._reconnaissance_patterns:
                if re.search(pattern, query):
                    risk_score += 0.4
                    matched_patterns.append(pattern)
                    threat_categories.append("reconnaissance")
            
            # Check privilege escalation patterns
            for pattern in self._privilege_escalation_patterns:
                if re.search(pattern, query):
                    risk_score += 0.6
                    matched_patterns.append(pattern)
                    threat_categories.append("privilege_escalation")
            
            # Additional risk factors
            if 'uba_persistent_log' in query.lower():
                risk_score += 0.5
                threat_categories.append("audit_log_tampering")
            
            if 'performance_schema' in query.lower():
                risk_score += 0.4
                threat_categories.append("performance_schema_access")
            
            # Create query event if suspicious
            if risk_score >= 0.3 or matched_patterns:
                query_event = QueryEvent(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.now(timezone.utc),
                    connection_id=thread_id,
                    user=user,
                    host=host,
                    database=query_info.get('db', ''),
                    query=query[:1000],  # Truncate very long queries
                    query_type='suspicious',
                    execution_time=0.0,  # Would need performance schema for this
                    rows_affected=0,     # Would need query result for this
                    risk_score=min(risk_score, 1.0),
                    matched_patterns=matched_patterns
                )
                
                self._query_events.put(query_event)
                
                # Create infrastructure event for high-risk queries
                if risk_score >= 0.6:
                    infra_event = InfrastructureEvent(
                        event_id=str(uuid.uuid4()),
                        timestamp=datetime.now(timezone.utc),
                        event_type="high_risk_query",
                        source_ip=host,
                        user_account=user,
                        target_component=ComponentType.DATABASE,
                        action_details={
                            "query_snippet": query[:200],
                            "threat_categories": threat_categories,
                            "matched_patterns": matched_patterns,
                            "connection_id": thread_id
                        },
                        risk_score=min(risk_score, 1.0),
                        integrity_hash=""
                    )
                    
                    infra_event.integrity_hash = self.crypto_logger.create_checksum(infra_event.__dict__)
                    
                    # Log critical query event
                    self.crypto_logger.log_monitoring_event(
                        "high_risk_query_detected",
                        "uba_db",
                        {
                            "user": user,
                            "host": host,
                            "query_snippet": query[:200],
                            "risk_score": risk_score,
                            "threat_categories": threat_categories
                        },
                        risk_score
                    )
            
        except Exception as e:
            self.logger.error(f"Error analyzing query for threats: {e}")
    
    def _update_connection_info(self, conn_id: int, process_info: Dict[str, Any]):
        """Update existing connection information"""
        try:
            if conn_id in self._active_connections:
                event = self._active_connections[conn_id]
                event.details.update({
                    'command': process_info.get('Command', ''),
                    'time': process_info.get('Time', 0),
                    'state': process_info.get('State', ''),
                    'info': process_info.get('Info', '')
                })
        except Exception as e:
            self.logger.error(f"Error updating connection info: {e}")
    
    def _handle_connection_close(self, conn_id: int):
        """Handle database connection closure"""
        try:
            if conn_id in self._active_connections:
                event = self._active_connections[conn_id]
                
                # Create disconnect event
                disconnect_event = ConnectionEvent(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.now(timezone.utc),
                    connection_id=conn_id,
                    user=event.user,
                    host=event.host,
                    database=event.database,
                    event_type='disconnect',
                    details={
                        'duration': (datetime.now(timezone.utc) - event.timestamp).total_seconds(),
                        'original_event_id': event.event_id
                    },
                    risk_score=0.0
                )
                
                # Move to history and remove from active
                self._connection_history.append(event)
                del self._active_connections[conn_id]
                
                # Queue disconnect event
                self._connection_events.put(disconnect_event)
                
        except Exception as e:
            self.logger.error(f"Error handling connection close: {e}")
    
    def _process_connection_events(self):
        """Process queued connection events"""
        try:
            events_processed = 0
            max_events = 100
            
            while not self._connection_events.empty() and events_processed < max_events:
                try:
                    event = self._connection_events.get_nowait()
                    
                    # Create audit trail
                    self.crypto_logger.create_audit_trail(event)
                    
                    # Notify callbacks
                    for callback in self._event_callbacks:
                        try:
                            callback(event)
                        except Exception as e:
                            self.logger.error(f"Error in connection event callback: {e}")
                    
                    events_processed += 1
                    
                except queue.Empty:
                    break
                except Exception as e:
                    self.logger.error(f"Error processing connection event: {e}")
            
        except Exception as e:
            self.logger.error(f"Error processing connection events: {e}")
    
    def _process_query_events(self):
        """Process queued query events"""
        try:
            events_processed = 0
            max_events = 100
            
            while not self._query_events.empty() and events_processed < max_events:
                try:
                    event = self._query_events.get_nowait()
                    
                    # Add to query history
                    self._query_history.append(event)
                    
                    # Notify callbacks
                    for callback in self._query_callbacks:
                        try:
                            callback(event)
                        except Exception as e:
                            self.logger.error(f"Error in query event callback: {e}")
                    
                    events_processed += 1
                    
                except queue.Empty:
                    break
                except Exception as e:
                    self.logger.error(f"Error processing query event: {e}")
            
        except Exception as e:
            self.logger.error(f"Error processing query events: {e}")
    
    def _cleanup_old_events(self):
        """Clean up old events to prevent memory leaks"""
        try:
            current_time = datetime.now(timezone.utc)
            cutoff_time = current_time.timestamp() - 86400  # 24 hours
            
            # Clean connection history
            self._connection_history = [
                event for event in self._connection_history
                if event.timestamp.timestamp() > cutoff_time
            ]
            
            # Clean query history
            self._query_history = [
                event for event in self._query_history
                if event.timestamp.timestamp() > cutoff_time
            ]
            
            # Clean failed auth attempts
            for host in list(self._failed_auth_attempts.keys()):
                self._failed_auth_attempts[host] = [
                    attempt for attempt in self._failed_auth_attempts[host]
                    if attempt.timestamp() > cutoff_time
                ]
                
                # Remove empty entries
                if not self._failed_auth_attempts[host]:
                    del self._failed_auth_attempts[host]
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old events: {e}")
    
    def add_event_callback(self, callback: Callable[[ConnectionEvent], None]):
        """Add callback for connection events"""
        self._event_callbacks.append(callback)
    
    def add_query_callback(self, callback: Callable[[QueryEvent], None]):
        """Add callback for query events"""
        self._query_callbacks.append(callback)
    
    def get_active_connections(self) -> Dict[int, ConnectionEvent]:
        """Get currently active connections"""
        return self._active_connections.copy()
    
    def get_connection_history(self, limit: int = 100) -> List[ConnectionEvent]:
        """Get connection history"""
        return self._connection_history[-limit:]
    
    def get_query_history(self, limit: int = 100) -> List[QueryEvent]:
        """Get query history"""
        return self._query_history[-limit:]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get interceptor statistics"""
        return {
            "active": self._active,
            "active_connections": len(self._active_connections),
            "connection_events_queued": self._connection_events.qsize(),
            "query_events_queued": self._query_events.qsize(),
            "connection_history_size": len(self._connection_history),
            "query_history_size": len(self._query_history),
            "failed_auth_hosts": len(self._failed_auth_attempts),
            "malicious_patterns_loaded": len(self._malicious_patterns),
            "reconnaissance_patterns_loaded": len(self._reconnaissance_patterns),
            "privilege_escalation_patterns_loaded": len(self._privilege_escalation_patterns)
        }
    
    def is_healthy(self) -> bool:
        """Check if the interceptor is healthy"""
        try:
            if not self._active:
                return False
            
            if not self._monitor_connection or not self._monitor_connection.is_connected():
                return False
            
            if self._monitor_thread and not self._monitor_thread.is_alive():
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking interceptor health: {e}")
            return False