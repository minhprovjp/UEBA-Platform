"""
Infrastructure Monitor for UBA Self-Monitoring System

Monitors all interactions with critical UBA infrastructure components including
database connections, query patterns, and service account activities.
"""

import logging
import threading
import time
import re
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass
import mysql.connector
from mysql.connector import Error as MySQLError
import queue
import uuid

try:
    from .interfaces import (
        MonitoringInterface, 
        InfrastructureEvent, 
        ComponentType,
        ThreatLevel
    )
    from .config_manager import SelfMonitoringConfig
    from .crypto_logger import CryptoLogger
    from .db_connection_interceptor import DatabaseConnectionInterceptor, ConnectionEvent, QueryEvent
    from .performance_schema_monitor import PerformanceSchemaMonitor, PerformanceSchemaAccess
except ImportError:
    # For direct execution or testing
    from interfaces import (
        MonitoringInterface, 
        InfrastructureEvent, 
        ComponentType,
        ThreatLevel
    )
    from config_manager import SelfMonitoringConfig
    from crypto_logger import CryptoLogger
    from db_connection_interceptor import DatabaseConnectionInterceptor, ConnectionEvent, QueryEvent
    from performance_schema_monitor import PerformanceSchemaMonitor, PerformanceSchemaAccess


@dataclass
class ConnectionInfo:
    """Database connection information"""
    connection_id: int
    user: str
    host: str
    database: str
    command: str
    time: int
    state: str
    info: Optional[str] = None


class InfrastructureMonitor(MonitoringInterface):
    """Infrastructure monitor for UBA components"""
    
    def __init__(self, config_manager: Optional[SelfMonitoringConfig] = None):
        """
        Initialize infrastructure monitor
        
        Args:
            config_manager: Configuration manager instance
        """
        self.config_manager = config_manager or SelfMonitoringConfig()
        self.crypto_logger = CryptoLogger()
        self.logger = logging.getLogger(__name__)
        
        # Monitoring state
        self._monitoring = False
        self._monitor_thread = None
        self._event_queue = queue.Queue()
        self._stop_event = threading.Event()
        
        # Database connection for monitoring
        self._db_connection = None
        self._last_connection_check = None
        
        # Database connection interceptor
        self._db_interceptor = DatabaseConnectionInterceptor(self.crypto_logger)
        
        # Performance schema monitor
        self._perf_schema_monitor = PerformanceSchemaMonitor(self.crypto_logger)
        
        # Tracking state
        self._tracked_connections: Dict[int, ConnectionInfo] = {}
        self._suspicious_patterns: Set[str] = set()
        self._baseline_established = False
        
        # Load configuration
        self._load_monitoring_config()
        
        # Set up interceptor callbacks
        self._setup_interceptor_callbacks()
    
    def _get_database_connection(self):
        """Get database connection for monitoring"""
        try:
            if self._db_connection is None or not self._db_connection.is_connected():
                # Create new connection
                db_config = self.db_config
                self._db_connection = mysql.connector.connect(
                    host=db_config.get('host', 'localhost'),
                    port=db_config.get('port', 3306),
                    database=db_config.get('database', 'uba_db'),
                    user=db_config.get('user', 'uba_user'),
                    password=db_config.get('password', ''),
                    connection_timeout=db_config.get('connection_timeout_seconds', 30),
                    autocommit=True
                )
            
            return self._db_connection
            
        except MySQLError as e:
            self.logger.error(f"Error connecting to database: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to database: {e}")
            return None
    
    def _load_monitoring_config(self):
        """Load monitoring configuration"""
        try:
            config = self.config_manager.load_config()
            self.monitoring_config = config.get('monitoring', {})
            self.detection_config = config.get('detection', {})
            self.db_config = config.get('database', {})
            
            # Load suspicious patterns
            patterns = self.detection_config.get('patterns', {})
            self._suspicious_patterns.update(patterns.get('malicious_queries', []))
            self._suspicious_patterns.update(patterns.get('reconnaissance_indicators', []))
            
            self.logger.info("Monitoring configuration loaded successfully")
            
        except Exception as e:
            self.logger.error(f"Error loading monitoring configuration: {e}")
            # Use minimal safe defaults
            self.monitoring_config = {'enabled': True, 'interval_seconds': 30}
            self.detection_config = {'enabled': True}
            self.db_config = {}
    
    def _setup_interceptor_callbacks(self):
        """Set up callbacks for database connection interceptor"""
        try:
            # Add callback for connection events
            self._db_interceptor.add_event_callback(self._handle_interceptor_connection_event)
            
            # Add callback for query events
            self._db_interceptor.add_query_callback(self._handle_interceptor_query_event)
            
        except Exception as e:
            self.logger.error(f"Error setting up interceptor callbacks: {e}")
    
    def _handle_interceptor_connection_event(self, event: ConnectionEvent):
        """Handle connection event from interceptor"""
        try:
            # Convert to infrastructure event
            infra_event = InfrastructureEvent(
                event_id=event.event_id,
                timestamp=event.timestamp,
                event_type=f"db_{event.event_type}",
                source_ip=event.host.split(':')[0] if ':' in event.host else event.host,
                user_account=event.user,
                target_component=ComponentType.DATABASE,
                action_details={
                    "connection_id": event.connection_id,
                    "database": event.database,
                    "event_details": event.details
                },
                risk_score=event.risk_score,
                integrity_hash=""
            )
            
            infra_event.integrity_hash = self.crypto_logger.create_checksum(infra_event.__dict__)
            
            # Queue for processing
            self._event_queue.put(infra_event)
            
        except Exception as e:
            self.logger.error(f"Error handling interceptor connection event: {e}")
    
    def _handle_interceptor_query_event(self, event: QueryEvent):
        """Handle query event from interceptor"""
        try:
            # Convert to infrastructure event
            infra_event = InfrastructureEvent(
                event_id=event.event_id,
                timestamp=event.timestamp,
                event_type="suspicious_query",
                source_ip=event.host.split(':')[0] if ':' in event.host else event.host,
                user_account=event.user,
                target_component=ComponentType.DATABASE,
                action_details={
                    "connection_id": event.connection_id,
                    "database": event.database,
                    "query_snippet": event.query[:200],
                    "query_type": event.query_type,
                    "matched_patterns": event.matched_patterns,
                    "execution_time": event.execution_time,
                    "rows_affected": event.rows_affected
                },
                risk_score=event.risk_score,
                integrity_hash=""
            )
            
            infra_event.integrity_hash = self.crypto_logger.create_checksum(infra_event.__dict__)
            
            # Queue for processing
            self._event_queue.put(infra_event)
            
        except Exception as e:
            self.logger.error(f"Error handling interceptor query event: {e}")
    
    def start_monitoring(self) -> bool:
        """Start the monitoring process"""
        try:
            if self._monitoring:
                self.logger.warning("Monitoring is already running")
                return True
            
            # Check if monitoring is enabled
            if not self.monitoring_config.get('enabled', True):
                self.logger.info("Monitoring is disabled in configuration")
                return False
            
            # Start database connection interceptor
            interceptor_started = self._db_interceptor.start_monitoring(self.db_config)
            if not interceptor_started:
                self.logger.warning("Failed to start database connection interceptor")
            
            # Start performance schema monitor
            perf_monitor_started = self._perf_schema_monitor.start_monitoring(self.db_config)
            if not perf_monitor_started:
                self.logger.warning("Failed to start performance schema monitor")
            
            # Start monitoring thread
            self._monitoring = True
            self._stop_event.clear()
            self._monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self._monitor_thread.start()
            
            self.logger.info("Infrastructure monitoring started")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting monitoring: {e}")
            self._monitoring = False
            return False
    
    def stop_monitoring(self) -> bool:
        """Stop the monitoring process"""
        try:
            if not self._monitoring:
                self.logger.warning("Monitoring is not running")
                return True
            
            # Signal stop and wait for thread
            self._stop_event.set()
            self._monitoring = False
            
            if self._monitor_thread and self._monitor_thread.is_alive():
                self._monitor_thread.join(timeout=10)
            
            # Stop database connection interceptor
            self._db_interceptor.stop_monitoring()
            
            # Stop performance schema monitor
            self._perf_schema_monitor.stop_monitoring()
            
            self.logger.info("Infrastructure monitoring stopped")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping monitoring: {e}")
            return False
    
    def _monitoring_loop(self):
        """Main monitoring loop"""
        interval = self.monitoring_config.get('interval_seconds', 30)
        
        while not self._stop_event.is_set():
            try:
                # Core monitoring operations
                # print("DEBUG: Monitor iteration start")
                self._monitor_database_connections()
                self._monitor_query_patterns()
                self._monitor_performance_schema_access()
                self._monitor_uba_user_activities()
                self._process_monitoring_events()
                # print("DEBUG: Monitor iteration end")
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
            
            # Wait for next iteration
            self._stop_event.wait(interval)
    
    def _process_monitoring_events(self):
        """Process queued monitoring events"""
        try:
            events_processed = 0
            max_events = self.monitoring_config.get('max_events_per_batch', 1000)
            
            while not self._event_queue.empty() and events_processed < max_events:
                try:
                    event = self._event_queue.get_nowait()
                    
                    # Create audit trail
                    self.crypto_logger.create_audit_trail(event)
                    
                    events_processed += 1
                    
                except queue.Empty:
                    break
                except Exception as e:
                    self.logger.error(f"Error processing event: {e}")
            
            if events_processed > 0:
                self.logger.debug(f"Processed {events_processed} monitoring events")
                
        except Exception as e:
            self.logger.error(f"Error processing monitoring events: {e}")
    
    def get_events(self, start_time: datetime, end_time: datetime) -> List[InfrastructureEvent]:
        """
        Retrieve events within a time range
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            List of infrastructure events
        """
        events = []
        
        try:
            self.logger.debug(f"Retrieving events from {start_time} to {end_time}")
            
        except Exception as e:
            self.logger.error(f"Error retrieving events: {e}")
        
        return events
    
    def is_healthy(self) -> bool:
        """Check if the monitoring component is healthy"""
        try:
            # Check if monitoring is running
            if not self._monitoring:
                return False
            
            # Check if monitoring thread is alive
            if self._monitor_thread and not self._monitor_thread.is_alive():
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking health: {e}")
            return False
    
    def get_monitoring_statistics(self) -> Dict[str, Any]:
        """Get monitoring statistics"""
        try:
            return {
                "monitoring_active": self._monitoring,
                "tracked_connections": len(self._tracked_connections),
                "events_queued": self._event_queue.qsize(),
                "suspicious_patterns_loaded": len(self._suspicious_patterns),
                "baseline_established": self._baseline_established
            }
        except Exception as e:
            self.logger.error(f"Error getting statistics: {e}")
            
            connection = self._get_database_connection()
            if connection is None:
                return
            
            cursor = connection.cursor(dictionary=True)
            
            # Get current connections
            cursor.execute("SHOW PROCESSLIST")
            current_connections = cursor.fetchall()
            
            # Track new connections and detect anomalies
            current_connection_ids = set()
            
            for conn_info in current_connections:
                conn_id = conn_info['Id']
                current_connection_ids.add(conn_id)
                
                # Create connection info object
                connection_obj = ConnectionInfo(
                    connection_id=conn_id,
                    user=conn_info.get('User', ''),
                    host=conn_info.get('Host', ''),
                    database=conn_info.get('db', ''),
                    command=conn_info.get('Command', ''),
                    time=conn_info.get('Time', 0),
                    state=conn_info.get('State', ''),
                    info=conn_info.get('Info', '')
                )
                
                # Check for new connections
                if conn_id not in self._tracked_connections:
                    self._handle_new_connection(connection_obj)
                
                # Update tracking
                self._tracked_connections[conn_id] = connection_obj
            
            # Detect closed connections
            closed_connections = set(self._tracked_connections.keys()) - current_connection_ids
            for conn_id in closed_connections:
                self._handle_closed_connection(self._tracked_connections[conn_id])
                del self._tracked_connections[conn_id]
            
            cursor.close()
            
        except MySQLError as e:
            self.logger.error(f"Error monitoring database connections: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error monitoring connections: {e}")
    
    def _handle_new_connection(self, connection: ConnectionInfo):
        """Handle new database connection"""
        try:
            # Calculate risk score based on connection attributes
            risk_score = self._calculate_connection_risk(connection)
            
            # Create infrastructure event
            event = InfrastructureEvent(
                event_id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                event_type="database_connection",
                source_ip=connection.host.split(':')[0] if ':' in connection.host else connection.host,
                user_account=connection.user,
                target_component=ComponentType.DATABASE,
                action_details={
                    "connection_id": connection.connection_id,
                    "database": connection.database,
                    "command": connection.command,
                    "state": connection.state
                },
                risk_score=risk_score,
                integrity_hash=""
            )
            
            # Create integrity hash
            event.integrity_hash = self.crypto_logger.create_checksum(event.__dict__)
            
            # Queue event for processing
            self._event_queue.put(event)
            
            # Log high-risk connections immediately
            if risk_score >= 0.7:
                self.crypto_logger.log_monitoring_event(
                    "high_risk_connection",
                    "uba_db",
                    event.action_details,
                    risk_score
                )
            
        except Exception as e:
            self.logger.error(f"Error handling new connection: {e}")
    
    def _calculate_connection_risk(self, connection: ConnectionInfo) -> float:
        """Calculate risk score for database connection"""
        risk_score = 0.0
        
        try:
            # Check for unauthorized users
            authorized_users = ['uba_user', 'root', 'mysql.sys']
            if connection.user not in authorized_users:
                risk_score += 0.5
            
            # Check for suspicious hosts
            if connection.host not in ['localhost', '127.0.0.1', '::1']:
                risk_score += 0.3
            
            # Check for multiple concurrent sessions from same user
            same_user_connections = [
                conn for conn in self._tracked_connections.values()
                if conn.user == connection.user
            ]
            
            concurrent_limit = self.detection_config.get('thresholds', {}).get('concurrent_session_limit', 2)
            if len(same_user_connections) >= concurrent_limit:
                risk_score += 0.4
            
            # Check for administrative commands
            admin_commands = ['Admin', 'Binlog Dump', 'Change user', 'Create DB', 'Drop DB']
            if connection.command in admin_commands:
                risk_score += 0.3
            
            return min(risk_score, 1.0)
            
        except Exception as e:
            self.logger.error(f"Error calculating connection risk: {e}")
            return 0.5  # Default moderate risk
    
    def _handle_closed_connection(self, connection: ConnectionInfo):
        """Handle closed database connection"""
        try:
            # Log connection closure for audit trail
            self.crypto_logger.log_monitoring_event(
                "database_disconnection",
                "uba_db",
                {
                    "connection_id": connection.connection_id,
                    "user": connection.user,
                    "host": connection.host,
                    "duration": connection.time
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error handling closed connection: {e}")
    
    def _monitor_database_connections(self):
        """Monitor database connections for unauthorized access and anomalies"""
        try:
            connection = self._get_database_connection()
            if connection is None:
                return
            
            cursor = connection.cursor(dictionary=True)
            
            # Get current connections
            cursor.execute("SHOW PROCESSLIST")
            current_connections = cursor.fetchall()
            
            # Track new connections and detect anomalies
            current_connection_ids = set()
            
            for conn_info in current_connections:
                conn_id = conn_info['Id']
                current_connection_ids.add(conn_id)
                
                # Create connection info object
                connection_obj = ConnectionInfo(
                    connection_id=conn_id,
                    user=conn_info.get('User', ''),
                    host=conn_info.get('Host', ''),
                    database=conn_info.get('db', ''),
                    command=conn_info.get('Command', ''),
                    time=conn_info.get('Time', 0),
                    state=conn_info.get('State', ''),
                    info=conn_info.get('Info', '')
                )
                
                # Check for new connections
                if conn_id not in self._tracked_connections:
                    self._handle_new_connection(connection_obj)
                
                # Update tracking
                self._tracked_connections[conn_id] = connection_obj
            
            # Detect closed connections
            closed_connections = set(self._tracked_connections.keys()) - current_connection_ids
            for conn_id in closed_connections:
                self._handle_closed_connection(self._tracked_connections[conn_id])
                del self._tracked_connections[conn_id]
            
            cursor.close()
            
        except MySQLError as e:
            self.logger.error(f"Error monitoring database connections: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error monitoring connections: {e}")
    
    def _monitor_query_patterns(self):
        """Monitor query patterns for malicious activities"""
        try:
            connection = self._get_database_connection()
            if connection is None:
                return
            
            cursor = connection.cursor(dictionary=True)
            
            # Monitor general log for query patterns (if enabled)
            # Note: This requires general_log to be enabled in MySQL
            try:
                cursor.execute("""
                    SELECT event_time, user_host, thread_id, server_id, command_type, argument
                    FROM mysql.general_log 
                    WHERE event_time >= DATE_SUB(NOW(), INTERVAL 1 MINUTE)
                    AND command_type IN ('Query', 'Execute')
                    AND 'UBA_EVENT' = 'UBA_EVENT'
                    ORDER BY event_time DESC
                    LIMIT 1000
                """)
                
                recent_queries = cursor.fetchall()
                
                for query_info in recent_queries:
                    self._analyze_query_pattern(query_info)
                    
            except MySQLError as e:
                # General log might not be enabled or accessible
                self.logger.debug(f"Cannot access general log: {e}")
            
            cursor.close()
            
        except Exception as e:
            self.logger.error(f"Error monitoring query patterns: {e}")
    
    def _analyze_query_pattern(self, query_info: Dict[str, Any]):
        """Analyze individual query for suspicious patterns"""
        try:
            query = query_info.get('argument', '').strip()
            if not query:
                return
            
            user_host = query_info.get('user_host', '')
            user = user_host.split('[')[0] if '[' in user_host else user_host
            
            # Check against suspicious patterns
            risk_score = 0.0
            matched_patterns = []
            
            for pattern in self._suspicious_patterns:
                if re.search(pattern, query, re.IGNORECASE):
                    risk_score += 0.3
                    matched_patterns.append(pattern)
            
            # Check for privilege escalation keywords
            privilege_keywords = self.detection_config.get('thresholds', {}).get('privilege_escalation_keywords', [])
            for keyword in privilege_keywords:
                if keyword.upper() in query.upper():
                    risk_score += 0.4
                    matched_patterns.append(f"privilege_keyword:{keyword}")
            
            # Check for information schema queries
            if 'information_schema' in query.lower():
                risk_score += 0.5
                matched_patterns.append("information_schema_access")
            
            # If suspicious, create event
            if risk_score >= 0.3:
                event = InfrastructureEvent(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.now(timezone.utc),
                    event_type="suspicious_query",
                    source_ip=user_host.split('@')[1] if '@' in user_host else 'unknown',
                    user_account=user,
                    target_component=ComponentType.DATABASE,
                    action_details={
                        "query": query[:500],  # Truncate long queries
                        "matched_patterns": matched_patterns,
                        "thread_id": query_info.get('thread_id')
                    },
                    risk_score=min(risk_score, 1.0),
                    integrity_hash=""
                )
                
                event.integrity_hash = self.crypto_logger.create_checksum(event.__dict__)
                self._event_queue.put(event)
            
        except Exception as e:
            self.logger.error(f"Error analyzing query pattern: {e}")
    
    def _monitor_performance_schema_access(self):
        """Monitor performance_schema access patterns"""
        try:
            connection = self._get_database_connection()
            if connection is None:
                return
            
            cursor = connection.cursor(dictionary=True)
            
            # Check for recent performance_schema queries
            # This is a simplified approach - in production, you'd want more sophisticated monitoring
            cursor.execute("""
                SELECT OBJECT_SCHEMA, OBJECT_NAME, COUNT_READ, COUNT_WRITE, COUNT_MISC
                FROM performance_schema.table_io_waits_summary_by_table
                WHERE OBJECT_SCHEMA = 'performance_schema'
                AND (COUNT_READ > 0 OR COUNT_WRITE > 0 OR COUNT_MISC > 0)
                AND 'UBA_EVENT' = 'UBA_EVENT'
                ORDER BY (COUNT_READ + COUNT_WRITE + COUNT_MISC) DESC
                LIMIT 50
            """)
            
            schema_access = cursor.fetchall()
            
            # Analyze access patterns
            for access_info in schema_access:
                self._analyze_performance_schema_access(access_info)
            
            cursor.close()
            
        except MySQLError as e:
            self.logger.debug(f"Cannot monitor performance schema: {e}")
        except Exception as e:
            self.logger.error(f"Error monitoring performance schema: {e}")
    
    def _analyze_performance_schema_access(self, access_info: Dict[str, Any]):
        """Analyze performance schema access for suspicious activity"""
        try:
            table_name = access_info.get('OBJECT_NAME', '')
            total_access = (access_info.get('COUNT_READ', 0) + 
                          access_info.get('COUNT_WRITE', 0) + 
                          access_info.get('COUNT_MISC', 0))
            
            # Check for suspicious table access
            sensitive_tables = [
                'users', 'user_variables_by_thread', 'accounts',
                'host_cache', 'session_variables', 'global_variables'
            ]
            
            risk_score = 0.0
            if table_name.lower() in [t.lower() for t in sensitive_tables]:
                risk_score += 0.6
            
            # High access count might indicate automated scanning
            if total_access > 100:
                risk_score += 0.3
            
            if risk_score >= 0.5:
                event = InfrastructureEvent(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.now(timezone.utc),
                    event_type="performance_schema_access",
                    source_ip="localhost",  # Performance schema access is typically local
                    user_account="system",
                    target_component=ComponentType.PERFORMANCE_SCHEMA,
                    action_details={
                        "table_name": table_name,
                        "access_count": total_access,
                        "read_count": access_info.get('COUNT_READ', 0),
                        "write_count": access_info.get('COUNT_WRITE', 0)
                    },
                    risk_score=min(risk_score, 1.0),
                    integrity_hash=""
                )
                
                event.integrity_hash = self.crypto_logger.create_checksum(event.__dict__)
                self._event_queue.put(event)
            
        except Exception as e:
            self.logger.error(f"Error analyzing performance schema access: {e}")
    
    def _monitor_uba_user_activities(self):
        """Monitor uba_user account activities for anomalies"""
        try:
            # Get uba_user connections
            uba_connections = [
                conn for conn in self._tracked_connections.values()
                if conn.user == 'uba_user'
            ]
            
            # Check for anomalous uba_user behavior
            for connection in uba_connections:
                risk_score = self._analyze_uba_user_behavior(connection)
                
                if risk_score >= 0.5:
                    event = InfrastructureEvent(
                        event_id=str(uuid.uuid4()),
                        timestamp=datetime.now(timezone.utc),
                        event_type="uba_user_anomaly",
                        source_ip=connection.host.split(':')[0] if ':' in connection.host else connection.host,
                        user_account=connection.user,
                        target_component=ComponentType.USER_ACCOUNT,
                        action_details={
                            "connection_id": connection.connection_id,
                            "command": connection.command,
                            "state": connection.state,
                            "duration": connection.time,
                            "anomaly_indicators": self._get_uba_user_anomaly_indicators(connection)
                        },
                        risk_score=risk_score,
                        integrity_hash=""
                    )
                    
                    event.integrity_hash = self.crypto_logger.create_checksum(event.__dict__)
                    self._event_queue.put(event)
            
        except Exception as e:
            self.logger.error(f"Error monitoring uba_user activities: {e}")
    
    def _analyze_uba_user_behavior(self, connection: ConnectionInfo) -> float:
        """Analyze uba_user behavior for anomalies"""
        risk_score = 0.0
        
        try:
            # Check for unusual connection sources
            if connection.host not in ['localhost', '127.0.0.1', '::1']:
                risk_score += 0.4
            
            # Check for administrative commands (uba_user should typically only do queries)
            admin_commands = ['Admin', 'Binlog Dump', 'Create DB', 'Drop DB', 'Shutdown']
            if connection.command in admin_commands:
                risk_score += 0.6
            
            # Check for long-running connections (might indicate compromise)
            if connection.time > 3600:  # 1 hour
                risk_score += 0.3
            
            # Check for multiple concurrent uba_user sessions
            uba_connections = [
                conn for conn in self._tracked_connections.values()
                if conn.user == 'uba_user'
            ]
            if len(uba_connections) > 2:
                risk_score += 0.4
            
            return min(risk_score, 1.0)
            
        except Exception as e:
            self.logger.error(f"Error analyzing uba_user behavior: {e}")
            return 0.5
    
    def _get_uba_user_anomaly_indicators(self, connection: ConnectionInfo) -> List[str]:
        """Get list of anomaly indicators for uba_user"""
        indicators = []
        
        try:
            if connection.host not in ['localhost', '127.0.0.1', '::1']:
                indicators.append("remote_connection")
            
            admin_commands = ['Admin', 'Binlog Dump', 'Create DB', 'Drop DB', 'Shutdown']
            if connection.command in admin_commands:
                indicators.append(f"admin_command:{connection.command}")
            
            if connection.time > 3600:
                indicators.append("long_running_connection")
            
            uba_connections = [
                conn for conn in self._tracked_connections.values()
                if conn.user == 'uba_user'
            ]
            if len(uba_connections) > 2:
                indicators.append(f"multiple_sessions:{len(uba_connections)}")
            
        except Exception as e:
            self.logger.error(f"Error getting anomaly indicators: {e}")
        
        return indicators
    
    def _process_monitoring_events(self):
        """Process queued monitoring events"""
        try:
            events_processed = 0
            max_events = self.monitoring_config.get('max_events_per_batch', 1000)
            
            while not self._event_queue.empty() and events_processed < max_events:
                try:
                    event = self._event_queue.get_nowait()
                    
                    # Create audit trail
                    self.crypto_logger.create_audit_trail(event)
                    
                    # Log high-risk events
                    if event.risk_score >= 0.7:
                        self.crypto_logger.log_monitoring_event(
                            f"high_risk_{event.event_type}",
                            event.target_component.value,
                            event.action_details,
                            event.risk_score
                        )
                    
                    events_processed += 1
                    
                except queue.Empty:
                    break
                except Exception as e:
                    self.logger.error(f"Error processing event: {e}")
            
            if events_processed > 0:
                self.logger.debug(f"Processed {events_processed} monitoring events")
                
        except Exception as e:
            self.logger.error(f"Error processing monitoring events: {e}")
    
    def get_events(self, start_time: datetime, end_time: datetime) -> List[InfrastructureEvent]:
        """
        Retrieve events within a time range
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            List of infrastructure events
        """
        # This is a simplified implementation
        # In production, you'd query a persistent event store
        events = []
        
        try:
            # For now, return empty list as events are processed in real-time
            # In a full implementation, this would query the audit log or event database
            self.logger.debug(f"Retrieving events from {start_time} to {end_time}")
            
        except Exception as e:
            self.logger.error(f"Error retrieving events: {e}")
        
        return events
    
    def is_healthy(self) -> bool:
        """Check if the monitoring component is healthy"""
        try:
            # Check if monitoring is running
            if not self._monitoring:
                return False
            
            # Check database connection
            connection = self._get_database_connection()
            if connection is None or not connection.is_connected():
                return False
            
            # Check if monitoring thread is alive
            if self._monitor_thread and not self._monitor_thread.is_alive():
                return False
            
            # Check interceptor health
            if not self._db_interceptor.is_healthy():
                return False
            
            # Check performance schema monitor health
            if not self._perf_schema_monitor.is_healthy():
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking health: {e}")
            return False
    
    def get_monitoring_statistics(self) -> Dict[str, Any]:
        """Get monitoring statistics"""
        try:
            interceptor_stats = self._db_interceptor.get_statistics()
            perf_schema_stats = self._perf_schema_monitor.get_access_statistics()
            
            return {
                "monitoring_active": self._monitoring,
                "tracked_connections": len(self._tracked_connections),
                "events_queued": self._event_queue.qsize(),
                "database_connected": (self._db_connection is not None and 
                                     self._db_connection.is_connected()),
                "suspicious_patterns_loaded": len(self._suspicious_patterns),
                "baseline_established": self._baseline_established,
                "interceptor_active": interceptor_stats.get("active", False),
                "interceptor_connections": interceptor_stats.get("active_connections", 0),
                "interceptor_connection_events": interceptor_stats.get("connection_events_queued", 0),
                "interceptor_query_events": interceptor_stats.get("query_events_queued", 0),
                "perf_schema_monitor_active": perf_schema_stats.get("active", False),
                "perf_schema_baseline_established": perf_schema_stats.get("baseline_established", False),
                "perf_schema_access_events": perf_schema_stats.get("access_events_queued", 0),
                "perf_schema_blocked_queries": perf_schema_stats.get("blocked_queries", 0)
            }
        except Exception as e:
            self.logger.error(f"Error getting statistics: {e}")
            return {}