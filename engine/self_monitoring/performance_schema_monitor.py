"""
Performance Schema Monitor for UBA Self-Monitoring System

This module provides specialized monitoring of MySQL performance_schema access patterns
to detect reconnaissance activities, credential extraction attempts, and other malicious
activities targeting the performance schema.
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
class PerformanceSchemaAccess:
    """Performance schema access event"""
    event_id: str
    timestamp: datetime
    user: str
    host: str
    table_name: str
    access_type: str  # 'read', 'write', 'metadata'
    query_pattern: str
    access_count: int
    risk_score: float
    threat_indicators: List[str]


@dataclass
class ReconnaissancePattern:
    """Reconnaissance pattern detection result"""
    pattern_id: str
    pattern_name: str
    description: str
    severity: ThreatLevel
    indicators: List[str]
    confidence: float


class PerformanceSchemaMonitor:
    """
    Monitor for MySQL performance_schema access patterns
    
    This class detects reconnaissance activities, credential extraction attempts,
    and other malicious activities targeting the performance schema tables.
    """
    
    def __init__(self, crypto_logger: Optional[CryptoLogger] = None):
        """
        Initialize performance schema monitor
        
        Args:
            crypto_logger: Cryptographic logger for secure audit trails
        """
        self.crypto_logger = crypto_logger or CryptoLogger()
        self.logger = logging.getLogger(__name__)
        
        # Monitoring state
        self._active = False
        self._monitor_thread = None
        self._stop_event = threading.Event()
        
        # Event tracking
        self._access_events = queue.Queue()
        self._access_history: List[PerformanceSchemaAccess] = []
        
        # Database connection for monitoring
        self._monitor_connection = None
        
        # Sensitive tables and patterns
        self._sensitive_tables = self._load_sensitive_tables()
        self._reconnaissance_patterns = self._load_reconnaissance_patterns()
        self._credential_extraction_patterns = self._load_credential_extraction_patterns()
        
        # Access tracking
        self._table_access_counts: Dict[str, Dict[str, int]] = {}  # table -> user -> count
        self._user_access_patterns: Dict[str, List[PerformanceSchemaAccess]] = {}
        self._blocked_queries: List[Dict[str, Any]] = []
        
        # Baseline learning
        self._baseline_period = 3600  # 1 hour baseline learning
        self._baseline_start_time = None
        self._baseline_established = False
        self._normal_access_patterns: Dict[str, Set[str]] = {}  # user -> set of normal tables
    
    def _load_sensitive_tables(self) -> Dict[str, ThreatLevel]:
        """Load sensitive performance schema tables and their threat levels"""
        return {
            # User and account information
            'accounts': ThreatLevel.HIGH,
            'users': ThreatLevel.HIGH,
            'user_variables_by_thread': ThreatLevel.CRITICAL,
            'session_variables': ThreatLevel.HIGH,
            'global_variables': ThreatLevel.CRITICAL,
            
            # Host and connection information
            'host_cache': ThreatLevel.HIGH,
            'hosts': ThreatLevel.MEDIUM,
            'socket_instances': ThreatLevel.MEDIUM,
            
            # Security and authentication
            'setup_actors': ThreatLevel.HIGH,
            'setup_objects': ThreatLevel.MEDIUM,
            
            # Process and thread information
            'threads': ThreatLevel.MEDIUM,
            'processlist': ThreatLevel.MEDIUM,
            
            # File and table access
            'file_instances': ThreatLevel.MEDIUM,
            'table_handles': ThreatLevel.LOW,
            'table_io_waits_summary_by_table': ThreatLevel.LOW,
            
            # Events and waits
            'events_waits_current': ThreatLevel.LOW,
            'events_waits_history': ThreatLevel.LOW,
            'events_statements_current': ThreatLevel.MEDIUM,
            'events_statements_history': ThreatLevel.MEDIUM,
            
            # Replication information
            'replication_connection_configuration': ThreatLevel.HIGH,
            'replication_connection_status': ThreatLevel.HIGH,

            # UBA Internal Tables (Self-Monitoring)
            'uba_persistent_log': ThreatLevel.CRITICAL,
            'uba_server_state': ThreatLevel.HIGH,
            'uba_ingest_metrics': ThreatLevel.MEDIUM,
        }
    
    def _load_reconnaissance_patterns(self) -> List[Dict[str, Any]]:
        """Load reconnaissance query patterns"""
        return [
            {
                'name': 'user_enumeration',
                'description': 'Enumerating database users and accounts',
                'patterns': [
                    r'(?i)select\s+.*\s+from\s+performance_schema\.accounts',
                    r'(?i)select\s+.*\s+from\s+performance_schema\.users',
                    r'(?i)select\s+user\s*,\s*host\s+from\s+performance_schema\.accounts',
                ],
                'severity': ThreatLevel.HIGH,
                'risk_score': 0.8
            },
            {
                'name': 'variable_extraction',
                'description': 'Extracting system and session variables',
                'patterns': [
                    r'(?i)select\s+.*\s+from\s+performance_schema\.global_variables',
                    r'(?i)select\s+.*\s+from\s+performance_schema\.session_variables',
                    r'(?i)select\s+.*variable_name.*variable_value.*from\s+performance_schema',
                ],
                'severity': ThreatLevel.CRITICAL,
                'risk_score': 0.9
            },
            {
                'name': 'host_reconnaissance',
                'description': 'Gathering host and network information',
                'patterns': [
                    r'(?i)select\s+.*\s+from\s+performance_schema\.host_cache',
                    r'(?i)select\s+.*\s+from\s+performance_schema\.hosts',
                    r'(?i)select\s+host\s*,.*from\s+performance_schema',
                ],
                'severity': ThreatLevel.HIGH,
                'risk_score': 0.7
            },
            {
                'name': 'process_monitoring',
                'description': 'Monitoring active processes and connections',
                'patterns': [
                    r'(?i)select\s+.*\s+from\s+performance_schema\.threads',
                    r'(?i)select\s+.*\s+from\s+performance_schema\.processlist',
                    r'(?i)select\s+.*processlist_id.*from\s+performance_schema',
                ],
                'severity': ThreatLevel.MEDIUM,
                'risk_score': 0.6
            },
            {
                'name': 'configuration_discovery',
                'description': 'Discovering database configuration and setup',
                'patterns': [
                    r'(?i)select\s+.*\s+from\s+performance_schema\.setup_',
                    r'(?i)show\s+tables\s+from\s+performance_schema',
                    r'(?i)describe\s+performance_schema\.',
                ],
                'severity': ThreatLevel.MEDIUM,
                'risk_score': 0.5
            },
            {
                'name': 'uba_reconnaissance',
                'description': 'Reconnaissance of UBA internal tables',
                'patterns': [
                    r'(?i)select\s+.*\s+from\s+uba_db\.',
                    r'(?i)show\s+tables\s+from\s+uba_db',
                    r'(?i)describe\s+uba_db\.',
                ],
                'severity': ThreatLevel.HIGH,
                'risk_score': 0.8
            }
        ]
    
    def _load_credential_extraction_patterns(self) -> List[str]:
        """Load patterns that indicate credential extraction attempts"""
        return [
            # Password-related variables
            r'(?i)variable_name\s*like\s*[\'"]%password%[\'"]',
            r'(?i)variable_name\s*=\s*[\'"].*password.*[\'"]',
            r'(?i)select\s+.*password.*from\s+performance_schema',
            
            # Authentication-related variables
            r'(?i)variable_name\s*like\s*[\'"]%auth%[\'"]',
            r'(?i)variable_name\s*like\s*[\'"]%ssl%[\'"]',
            r'(?i)variable_name\s*like\s*[\'"]%secure%[\'"]',
            
            # User privilege information
            r'(?i)select\s+.*privilege.*from\s+performance_schema',
            r'(?i)select\s+.*grant.*from\s+performance_schema',
            r'(?i)variable_name\s*like\s*[\'"]%privilege%[\'"]',
            
            # Connection and session security
            r'(?i)select\s+.*connection_id.*from\s+performance_schema',
            r'(?i)select\s+.*thread_id.*user.*from\s+performance_schema',
        ]
    
    def start_monitoring(self, db_config: Dict[str, Any]) -> bool:
        """
        Start performance schema monitoring
        
        Args:
            db_config: Database configuration for monitoring connection
            
        Returns:
            True if monitoring started successfully, False otherwise
        """
        try:
            if self._active:
                self.logger.warning("Performance schema monitor is already active")
                return True
            
            # Establish monitoring connection
            self._monitor_connection = mysql.connector.connect(
                host=db_config.get('host', 'localhost'),
                port=db_config.get('port', 3306),
                database='performance_schema',  # Connect directly to performance_schema
                user=db_config.get('user', 'uba_user'),
                password=db_config.get('password', ''),
                connection_timeout=db_config.get('connection_timeout_seconds', 30),
                autocommit=True
            )
            
            # Start baseline learning
            self._baseline_start_time = datetime.now(timezone.utc)
            self._baseline_established = False
            
            # Start monitoring thread
            self._active = True
            self._stop_event.clear()
            self._monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self._monitor_thread.start()
            
            self.logger.info("Performance schema monitoring started")
            return True
            
        except MySQLError as e:
            self.logger.error(f"Error starting performance schema monitor: {e}")
            self._active = False
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error starting performance schema monitor: {e}")
            self._active = False
            return False
    
    def stop_monitoring(self) -> bool:
        """
        Stop performance schema monitoring
        
        Returns:
            True if monitoring stopped successfully, False otherwise
        """
        try:
            if not self._active:
                self.logger.warning("Performance schema monitor is not active")
                return True
            
            # Signal stop and wait for thread
            self._stop_event.set()
            self._active = False
            
            if self._monitor_thread and self._monitor_thread.is_alive():
                self._monitor_thread.join(timeout=10)
            
            # Close monitoring connection
            if self._monitor_connection and self._monitor_connection.is_connected():
                self._monitor_connection.close()
            
            self.logger.info("Performance schema monitoring stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping performance schema monitor: {e}")
            return False
    
    def _monitoring_loop(self):
        """Main monitoring loop for performance schema access"""
        while not self._stop_event.is_set():
            try:
                # Monitor table access patterns
                self._monitor_table_access()
                
                # Monitor query patterns
                self._monitor_query_patterns()
                
                # Check for reconnaissance patterns
                self._detect_reconnaissance_patterns()
                
                # Process queued events
                self._process_access_events()
                
                # Update baseline if in learning period
                self._update_baseline()
                
                # Clean up old events
                self._cleanup_old_events()
                
            except Exception as e:
                self.logger.error(f"Error in performance schema monitoring loop: {e}")
            
            # Wait before next iteration
            self._stop_event.wait(10)  # 10 second intervals
    
    def _monitor_table_access(self):
        """Monitor performance schema table access patterns"""
        try:
            if not self._monitor_connection or not self._monitor_connection.is_connected():
                return
            
            cursor = self._monitor_connection.cursor(dictionary=True)
            
            # Get table I/O statistics
            cursor.execute("""
                SELECT 
                    OBJECT_SCHEMA,
                    OBJECT_NAME,
                    COUNT_READ,
                    COUNT_WRITE,
                    COUNT_FETCH,
                    COUNT_INSERT,
                    COUNT_UPDATE,
                    COUNT_DELETE
                FROM table_io_waits_summary_by_table
                WHERE OBJECT_SCHEMA = 'performance_schema'
                AND (COUNT_READ > 0 OR COUNT_WRITE > 0 OR COUNT_FETCH > 0)
                AND 'UBA_EVENT' = 'UBA_EVENT'
                ORDER BY (COUNT_READ + COUNT_WRITE + COUNT_FETCH) DESC
                LIMIT 100
            """)
            
            table_stats = cursor.fetchall()
            
            # Analyze access patterns
            for stats in table_stats:
                self._analyze_table_access(stats)
            
            cursor.close()
            
        except MySQLError as e:
            self.logger.error(f"Error monitoring table access: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error monitoring table access: {e}")
    
    def _analyze_table_access(self, stats: Dict[str, Any]):
        """Analyze individual table access for suspicious patterns"""
        try:
            table_name = stats.get('OBJECT_NAME', '')
            total_reads = stats.get('COUNT_READ', 0) + stats.get('COUNT_FETCH', 0)
            total_writes = stats.get('COUNT_WRITE', 0) + stats.get('COUNT_INSERT', 0) + \
                          stats.get('COUNT_UPDATE', 0) + stats.get('COUNT_DELETE', 0)
            total_access = total_reads + total_writes
            
            if total_access == 0:
                return
            
            # Calculate risk score based on table sensitivity and access patterns
            risk_score = 0.0
            threat_indicators = []
            
            # Check if table is sensitive
            if table_name in self._sensitive_tables:
                table_threat_level = self._sensitive_tables[table_name]
                if table_threat_level == ThreatLevel.CRITICAL:
                    risk_score += 0.8
                    threat_indicators.append("critical_table_access")
                elif table_threat_level == ThreatLevel.HIGH:
                    risk_score += 0.6
                    threat_indicators.append("high_risk_table_access")
                elif table_threat_level == ThreatLevel.MEDIUM:
                    risk_score += 0.4
                    threat_indicators.append("medium_risk_table_access")
            
            # Check for excessive access
            if total_access > 1000:
                risk_score += 0.3
                threat_indicators.append("excessive_access_count")
            elif total_access > 100:
                risk_score += 0.2
                threat_indicators.append("high_access_count")
            
            # Check for write operations on read-only tables
            if total_writes > 0 and table_name in ['accounts', 'users', 'hosts']:
                risk_score += 0.5
                threat_indicators.append("unexpected_write_operations")
            
            # Create access event if suspicious
            if risk_score >= 0.4 or threat_indicators:
                access_event = PerformanceSchemaAccess(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.now(timezone.utc),
                    user="system",  # Table stats don't include user info
                    host="localhost",
                    table_name=table_name,
                    access_type="read" if total_writes == 0 else "write",
                    query_pattern="table_io_statistics",
                    access_count=total_access,
                    risk_score=min(risk_score, 1.0),
                    threat_indicators=threat_indicators
                )
                
                self._access_events.put(access_event)
            
        except Exception as e:
            self.logger.error(f"Error analyzing table access: {e}")
    
    def _monitor_query_patterns(self):
        """Monitor query patterns against performance schema"""
        try:
            if not self._monitor_connection or not self._monitor_connection.is_connected():
                return
            
            cursor = self._monitor_connection.cursor(dictionary=True)
            
            # Monitor recent statements that accessed performance_schema
            try:
                cursor.execute("""
                    SELECT 
                        THREAD_ID,
                        EVENT_ID,
                        SQL_TEXT,
                        CURRENT_SCHEMA,
                        TIMER_START,
                        TIMER_END,
                        ROWS_EXAMINED,
                        ROWS_SENT
                    FROM events_statements_history
                    WHERE (SQL_TEXT LIKE '%performance_schema%' 
                        OR SQL_TEXT LIKE '%uba_db%' 
                        OR SQL_TEXT LIKE '%uba_user%')
                    AND TIMER_START >= (SELECT MAX(TIMER_START) - 300000000000 FROM events_statements_history)
                    AND 'UBA_EVENT' = 'UBA_EVENT'
                    ORDER BY TIMER_START DESC
                    LIMIT 50
                """)
                
                recent_queries = cursor.fetchall()
                
                for query_info in recent_queries:
                    self._analyze_performance_schema_query(query_info)
                    
            except MySQLError as e:
                # Events tables might not be enabled or accessible
                self.logger.debug(f"Cannot access events_statements_history: {e}")
            
            cursor.close()
            
        except Exception as e:
            self.logger.error(f"Error monitoring query patterns: {e}")
    
    def _analyze_performance_schema_query(self, query_info: Dict[str, Any]):
        """Analyze individual query against performance schema"""
        try:
            sql_text = query_info.get('SQL_TEXT', '').strip()
            if not sql_text or len(sql_text) < 10:
                return
            
            thread_id = query_info.get('THREAD_ID', 0)
            rows_examined = query_info.get('ROWS_EXAMINED', 0)
            rows_sent = query_info.get('ROWS_SENT', 0)
            
            # Analyze query for threat patterns
            risk_score = 0.0
            threat_indicators = []
            matched_patterns = []
            
            # Check reconnaissance patterns
            for pattern_info in self._reconnaissance_patterns:
                for pattern in pattern_info['patterns']:
                    if re.search(pattern, sql_text):
                        risk_score += pattern_info['risk_score']
                        threat_indicators.append(pattern_info['name'])
                        matched_patterns.append(pattern)
            
            # Check credential extraction patterns
            for pattern in self._credential_extraction_patterns:
                if re.search(pattern, sql_text):
                    risk_score += 0.8
                    threat_indicators.append("credential_extraction_attempt")
                    matched_patterns.append(pattern)
            
            # Check for bulk data extraction
            if rows_sent > 1000:
                risk_score += 0.4
                threat_indicators.append("bulk_data_extraction")
            elif rows_sent > 100:
                risk_score += 0.2
                threat_indicators.append("large_result_set")
            
            # Check for sensitive table access
            for table_name, threat_level in self._sensitive_tables.items():
                if table_name in sql_text.lower():
                    if threat_level == ThreatLevel.CRITICAL:
                        risk_score += 0.6
                        threat_indicators.append(f"critical_table_query:{table_name}")
                    elif threat_level == ThreatLevel.HIGH:
                        risk_score += 0.4
                        threat_indicators.append(f"high_risk_table_query:{table_name}")
            
            # Create access event if suspicious
            if risk_score >= 0.3 or threat_indicators:
                access_event = PerformanceSchemaAccess(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.now(timezone.utc),
                    user="unknown",  # Would need to correlate with threads table
                    host="unknown",
                    table_name="multiple" if len(matched_patterns) > 1 else "unknown",
                    access_type="query",
                    query_pattern=sql_text[:200],  # Truncate long queries
                    access_count=1,
                    risk_score=min(risk_score, 1.0),
                    threat_indicators=threat_indicators
                )
                
                self._access_events.put(access_event)
                
                # Log high-risk queries immediately
                if risk_score >= 0.7:
                    self.crypto_logger.log_monitoring_event(
                        "high_risk_performance_schema_query",
                        "performance_schema",
                        {
                            "query_snippet": sql_text[:200],
                            "threat_indicators": threat_indicators,
                            "risk_score": risk_score,
                            "thread_id": thread_id,
                            "rows_sent": rows_sent
                        },
                        risk_score
                    )
            
        except Exception as e:
            self.logger.error(f"Error analyzing performance schema query: {e}")
    
    def _detect_reconnaissance_patterns(self):
        """Detect reconnaissance patterns across multiple queries"""
        try:
            # Analyze recent access events for patterns
            current_time = datetime.now(timezone.utc)
            recent_events = [
                event for event in self._access_history
                if (current_time - event.timestamp).total_seconds() < 300  # Last 5 minutes
            ]
            
            if len(recent_events) < 2:
                return
            
            # Group events by user/host
            user_events = {}
            for event in recent_events:
                user_key = f"{event.user}@{event.host}"
                if user_key not in user_events:
                    user_events[user_key] = []
                user_events[user_key].append(event)
            
            # Detect reconnaissance patterns
            for user_key, events in user_events.items():
                if len(events) >= 3:  # Multiple accesses from same user
                    self._analyze_user_reconnaissance_pattern(user_key, events)
            
        except Exception as e:
            self.logger.error(f"Error detecting reconnaissance patterns: {e}")
    
    def _analyze_user_reconnaissance_pattern(self, user_key: str, events: List[PerformanceSchemaAccess]):
        """Analyze user's access pattern for reconnaissance indicators"""
        try:
            # Check for systematic table enumeration
            accessed_tables = set(event.table_name for event in events)
            sensitive_tables_accessed = [
                table for table in accessed_tables
                if table in self._sensitive_tables
            ]
            
            # Calculate pattern risk
            pattern_risk = 0.0
            pattern_indicators = []
            
            # Multiple sensitive tables accessed
            if len(sensitive_tables_accessed) >= 3:
                pattern_risk += 0.8
                pattern_indicators.append("systematic_sensitive_table_enumeration")
            elif len(sensitive_tables_accessed) >= 2:
                pattern_risk += 0.6
                pattern_indicators.append("multiple_sensitive_table_access")
            
            # High frequency access
            time_span = (events[-1].timestamp - events[0].timestamp).total_seconds()
            if time_span < 60 and len(events) >= 5:  # 5+ accesses in 1 minute
                pattern_risk += 0.5
                pattern_indicators.append("rapid_fire_access")
            
            # Escalating access pattern (low to high sensitivity)
            table_risks = [
                self._get_table_risk_score(event.table_name) for event in events
            ]
            if len(table_risks) >= 3 and table_risks[-1] > table_risks[0]:
                pattern_risk += 0.4
                pattern_indicators.append("escalating_access_pattern")
            
            # Create reconnaissance detection event
            if pattern_risk >= 0.5:
                reconnaissance_event = InfrastructureEvent(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.now(timezone.utc),
                    event_type="performance_schema_reconnaissance",
                    source_ip=user_key.split('@')[1] if '@' in user_key else 'unknown',
                    user_account=user_key.split('@')[0] if '@' in user_key else user_key,
                    target_component=ComponentType.PERFORMANCE_SCHEMA,
                    action_details={
                        "accessed_tables": list(accessed_tables),
                        "sensitive_tables": sensitive_tables_accessed,
                        "access_count": len(events),
                        "time_span_seconds": time_span,
                        "pattern_indicators": pattern_indicators,
                        "average_risk_score": sum(event.risk_score for event in events) / len(events)
                    },
                    risk_score=min(pattern_risk, 1.0),
                    integrity_hash=""
                )
                
                reconnaissance_event.integrity_hash = self.crypto_logger.create_checksum(reconnaissance_event.__dict__)
                
                # Log critical reconnaissance detection
                self.crypto_logger.log_monitoring_event(
                    "performance_schema_reconnaissance_detected",
                    "performance_schema",
                    {
                        "user": user_key,
                        "accessed_tables": list(accessed_tables),
                        "pattern_indicators": pattern_indicators,
                        "risk_score": pattern_risk
                    },
                    pattern_risk
                )
            
        except Exception as e:
            self.logger.error(f"Error analyzing user reconnaissance pattern: {e}")
    
    def _get_table_risk_score(self, table_name: str) -> float:
        """Get risk score for a table based on its sensitivity"""
        if table_name not in self._sensitive_tables:
            return 0.1
        
        threat_level = self._sensitive_tables[table_name]
        if threat_level == ThreatLevel.CRITICAL:
            return 1.0
        elif threat_level == ThreatLevel.HIGH:
            return 0.8
        elif threat_level == ThreatLevel.MEDIUM:
            return 0.6
        else:
            return 0.4
    
    def _process_access_events(self):
        """Process queued access events"""
        try:
            events_processed = 0
            max_events = 100
            
            while not self._access_events.empty() and events_processed < max_events:
                try:
                    event = self._access_events.get_nowait()
                    
                    # Add to history
                    self._access_history.append(event)
                    
                    # Create audit trail
                    self.crypto_logger.create_audit_trail(event)
                    
                    # Update access tracking
                    self._update_access_tracking(event)
                    
                    events_processed += 1
                    
                except queue.Empty:
                    break
                except Exception as e:
                    self.logger.error(f"Error processing access event: {e}")
            
        except Exception as e:
            self.logger.error(f"Error processing access events: {e}")
    
    def _update_access_tracking(self, event: PerformanceSchemaAccess):
        """Update access tracking statistics"""
        try:
            # Update table access counts
            if event.table_name not in self._table_access_counts:
                self._table_access_counts[event.table_name] = {}
            
            if event.user not in self._table_access_counts[event.table_name]:
                self._table_access_counts[event.table_name][event.user] = 0
            
            self._table_access_counts[event.table_name][event.user] += event.access_count
            
            # Update user access patterns
            if event.user not in self._user_access_patterns:
                self._user_access_patterns[event.user] = []
            
            self._user_access_patterns[event.user].append(event)
            
            # Limit history size per user
            if len(self._user_access_patterns[event.user]) > 100:
                self._user_access_patterns[event.user] = self._user_access_patterns[event.user][-100:]
            
        except Exception as e:
            self.logger.error(f"Error updating access tracking: {e}")
    
    def _update_baseline(self):
        """Update baseline access patterns during learning period"""
        try:
            if self._baseline_established:
                return
            
            if self._baseline_start_time is None:
                return
            
            # Check if baseline period is complete
            current_time = datetime.now(timezone.utc)
            elapsed_time = (current_time - self._baseline_start_time).total_seconds()
            
            if elapsed_time >= self._baseline_period:
                # Establish baseline from learned patterns
                for user, events in self._user_access_patterns.items():
                    if user not in self._normal_access_patterns:
                        self._normal_access_patterns[user] = set()
                    
                    # Add tables accessed during baseline period to normal patterns
                    for event in events:
                        if (event.timestamp - self._baseline_start_time).total_seconds() <= self._baseline_period:
                            self._normal_access_patterns[user].add(event.table_name)
                
                self._baseline_established = True
                self.logger.info("Performance schema access baseline established")
            
        except Exception as e:
            self.logger.error(f"Error updating baseline: {e}")
    
    def _cleanup_old_events(self):
        """Clean up old events to prevent memory leaks"""
        try:
            current_time = datetime.now(timezone.utc)
            cutoff_time = current_time.timestamp() - 86400  # 24 hours
            
            # Clean access history
            self._access_history = [
                event for event in self._access_history
                if event.timestamp.timestamp() > cutoff_time
            ]
            
            # Clean user access patterns
            for user in list(self._user_access_patterns.keys()):
                self._user_access_patterns[user] = [
                    event for event in self._user_access_patterns[user]
                    if event.timestamp.timestamp() > cutoff_time
                ]
                
                # Remove empty entries
                if not self._user_access_patterns[user]:
                    del self._user_access_patterns[user]
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old events: {e}")
    
    def block_credential_extraction_query(self, query: str) -> bool:
        """
        Block queries that attempt credential extraction
        
        Args:
            query: SQL query to analyze
            
        Returns:
            True if query should be blocked, False otherwise
        """
        try:
            # Check against credential extraction patterns
            for pattern in self._credential_extraction_patterns:
                if re.search(pattern, query, re.IGNORECASE):
                    # Log blocked query
                    blocked_query = {
                        'timestamp': datetime.now(timezone.utc),
                        'query': query[:200],
                        'pattern': pattern,
                        'reason': 'credential_extraction_attempt'
                    }
                    self._blocked_queries.append(blocked_query)
                    
                    self.crypto_logger.log_monitoring_event(
                        "blocked_credential_extraction_query",
                        "performance_schema",
                        blocked_query,
                        0.9
                    )
                    
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking query for blocking: {e}")
            return False
    
    def get_access_statistics(self) -> Dict[str, Any]:
        """Get performance schema access statistics"""
        try:
            return {
                "active": self._active,
                "baseline_established": self._baseline_established,
                "access_events_queued": self._access_events.qsize(),
                "access_history_size": len(self._access_history),
                "monitored_tables": len(self._sensitive_tables),
                "tracked_users": len(self._user_access_patterns),
                "blocked_queries": len(self._blocked_queries),
                "reconnaissance_patterns": len(self._reconnaissance_patterns),
                "credential_extraction_patterns": len(self._credential_extraction_patterns)
            }
        except Exception as e:
            self.logger.error(f"Error getting access statistics: {e}")
            return {}
    
    def get_recent_access_events(self, limit: int = 50) -> List[PerformanceSchemaAccess]:
        """Get recent access events"""
        return self._access_history[-limit:]
    
    def get_blocked_queries(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recently blocked queries"""
        return self._blocked_queries[-limit:]
    
    def is_healthy(self) -> bool:
        """Check if the performance schema monitor is healthy"""
        try:
            if not self._active:
                return False
            
            if not self._monitor_connection or not self._monitor_connection.is_connected():
                return False
            
            if self._monitor_thread and not self._monitor_thread.is_alive():
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking performance schema monitor health: {e}")
            return False