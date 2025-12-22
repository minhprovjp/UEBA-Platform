"""
Behavioral Anomaly Detection System for UBA Self-Monitoring

This module implements baseline behavior profiling for the uba_user account and other
system components, detecting deviations from normal operational patterns including
connection patterns, concurrent session monitoring, and behavioral analysis.
"""

import logging
import threading
import time
import statistics
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, deque
import json
import uuid
import numpy as np

try:
    from .interfaces import (
        DetectionInterface,
        InfrastructureEvent, 
        ThreatDetection,
        ComponentType,
        ThreatLevel
    )
    from .crypto_logger import CryptoLogger
    from .config_manager import SelfMonitoringConfig
except ImportError:
    # For direct execution or testing
    from interfaces import (
        DetectionInterface,
        InfrastructureEvent, 
        ThreatDetection,
        ComponentType,
        ThreatLevel
    )
    from crypto_logger import CryptoLogger
    from config_manager import SelfMonitoringConfig


@dataclass
class ConnectionPattern:
    """Connection pattern information for baseline profiling"""
    user: str
    host_patterns: Set[str] = field(default_factory=set)
    connection_times: List[datetime] = field(default_factory=list)
    session_durations: List[float] = field(default_factory=list)
    concurrent_sessions: List[int] = field(default_factory=list)
    command_patterns: Dict[str, int] = field(default_factory=dict)
    database_access_patterns: Set[str] = field(default_factory=set)
    query_frequency: List[int] = field(default_factory=list)  # queries per minute
    
    def add_connection(self, host: str, timestamp: datetime, duration: float = 0.0, 
                      command: str = "", database: str = "", concurrent_count: int = 1):
        """Add a connection event to the pattern"""
        self.host_patterns.add(host)
        self.connection_times.append(timestamp)
        if duration > 0:
            self.session_durations.append(duration)
        self.concurrent_sessions.append(concurrent_count)
        
        if command:
            self.command_patterns[command] = self.command_patterns.get(command, 0) + 1
        
        if database:
            self.database_access_patterns.add(database)


@dataclass
class BaselineBehavior:
    """Baseline behavior profile for a user or component"""
    user: str
    profile_start: datetime
    profile_end: Optional[datetime] = None
    
    # Connection patterns
    typical_hosts: Set[str] = field(default_factory=set)
    connection_frequency_per_hour: float = 0.0
    avg_session_duration: float = 0.0
    max_concurrent_sessions: int = 1
    
    # Temporal patterns
    active_hours: Set[int] = field(default_factory=set)  # Hours of day (0-23)
    active_days: Set[int] = field(default_factory=set)   # Days of week (0-6)
    
    # Command patterns
    typical_commands: Dict[str, float] = field(default_factory=dict)  # command -> frequency
    
    # Database access patterns
    typical_databases: Set[str] = field(default_factory=set)
    
    # Query patterns
    avg_queries_per_minute: float = 0.0
    query_complexity_distribution: Dict[str, float] = field(default_factory=dict)
    
    # Anomaly thresholds (calculated from baseline)
    connection_frequency_threshold: float = 0.0
    session_duration_threshold: float = 0.0
    concurrent_session_threshold: int = 0
    query_frequency_threshold: float = 0.0
    
    def is_established(self, min_events: int = 50) -> bool:
        """
        Check if baseline is sufficiently established
        
        Args:
            min_events: Minimum number of events required (default: 50)
            
        Returns:
            True if baseline is sufficiently established for anomaly detection
        """
        if not self.profile_end:
            return False
        
        profile_duration = (self.profile_end - self.profile_start).total_seconds()
        min_duration = 24 * 3600  # At least 24 hours of data for reliable baseline
        
        # Require sufficient time, events, and diversity
        return (profile_duration >= min_duration and 
                len(self.typical_hosts) > 0 and
                self.connection_frequency_per_hour > 0 and
                len(self.active_hours) >= 2 and  # Active in at least 2 different hours
                len(self.typical_commands) >= 1)  # At least 1 command pattern observed


@dataclass
class AnomalyDetection:
    """Anomaly detection result"""
    detection_id: str
    timestamp: datetime
    user: str
    anomaly_type: str
    severity: ThreatLevel
    confidence: float
    baseline_value: Any
    observed_value: Any
    deviation_score: float
    context: Dict[str, Any]


class BehavioralAnomalyDetector(DetectionInterface):
    """
    Behavioral anomaly detection system for UBA infrastructure monitoring
    
    This class implements baseline behavior profiling and deviation detection
    for the uba_user account and other critical system components.
    """
    
    def __init__(self, config_manager: Optional[SelfMonitoringConfig] = None,
                 crypto_logger: Optional[CryptoLogger] = None):
        """
        Initialize behavioral anomaly detector
        
        Args:
            config_manager: Configuration manager instance
            crypto_logger: Cryptographic logger for secure audit trails
        """
        self.config_manager = config_manager or SelfMonitoringConfig()
        self.crypto_logger = crypto_logger or CryptoLogger()
        self.logger = logging.getLogger(__name__)
        
        # Detection state
        self._active = False
        self._detection_thread = None
        self._stop_event = threading.Event()
        
        # Baseline profiles
        self._baselines: Dict[str, BaselineBehavior] = {}
        self._connection_patterns: Dict[str, ConnectionPattern] = {}
        
        # Event tracking
        self._recent_events: deque = deque(maxlen=10000)
        self._event_lock = threading.Lock()
        
        # Detection configuration
        self._load_detection_config()
        
        # Anomaly detection results
        self._recent_anomalies: List[AnomalyDetection] = []
        self._anomaly_lock = threading.Lock()
        
        # Learning parameters
        self._learning_window_hours = 72  # Hours to collect baseline data (increased)
        self._min_events_for_baseline = 100  # Minimum events needed for baseline (increased)
        self._anomaly_threshold = 2.5  # Standard deviations for anomaly detection (increased)
    
    def _load_detection_config(self):
        """Load detection configuration"""
        try:
            config = self.config_manager.load_config()
            detection_config = config.get('behavioral_detection', {})
            
            self._learning_window_hours = detection_config.get('learning_window_hours', 72)  # Increased to 72 hours
            self._min_events_for_baseline = detection_config.get('min_events_for_baseline', 100)  # Increased to 100 events
            self._anomaly_threshold = detection_config.get('anomaly_threshold', 2.5)  # Increased threshold
            
            # Users to monitor
            self._monitored_users = detection_config.get('monitored_users', ['uba_user'])
            
            # Anomaly detection thresholds
            self._thresholds = detection_config.get('thresholds', {
                'connection_frequency_multiplier': 4.0,  # More conservative
                'session_duration_multiplier': 6.0,      # More conservative
                'concurrent_session_limit': 5,           # Higher limit
                'query_frequency_multiplier': 5.0,       # More conservative
                'new_host_risk_score': 0.6,             # Lower initial risk
                'unusual_time_risk_score': 0.4           # Lower initial risk
            })
            
            self.logger.info("Behavioral detection configuration loaded")
            
        except Exception as e:
            self.logger.error(f"Error loading detection configuration: {e}")
            # Use safe defaults
            self._monitored_users = ['uba_user']
            self._thresholds = {
                'connection_frequency_multiplier': 4.0,
                'session_duration_multiplier': 6.0,
                'concurrent_session_limit': 5,
                'query_frequency_multiplier': 5.0,
                'new_host_risk_score': 0.6,
                'unusual_time_risk_score': 0.4
            }
    
    def start_detection(self) -> bool:
        """Start behavioral anomaly detection"""
        try:
            if self._active:
                self.logger.warning("Behavioral anomaly detection is already active")
                return True
            
            # Start detection thread
            self._active = True
            self._stop_event.clear()
            self._detection_thread = threading.Thread(target=self._detection_loop, daemon=True)
            self._detection_thread.start()
            
            self.logger.info("Behavioral anomaly detection started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting behavioral anomaly detection: {e}")
            self._active = False
            return False
    
    def stop_detection(self) -> bool:
        """Stop behavioral anomaly detection"""
        try:
            if not self._active:
                self.logger.warning("Behavioral anomaly detection is not active")
                return True
            
            # Signal stop and wait for thread
            self._stop_event.set()
            self._active = False
            
            if self._detection_thread and self._detection_thread.is_alive():
                self._detection_thread.join(timeout=10)
            
            self.logger.info("Behavioral anomaly detection stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping behavioral anomaly detection: {e}")
            return False
    
    def _detection_loop(self):
        """Main detection loop"""
        while not self._stop_event.is_set():
            try:
                # Update baselines with recent events
                self._update_baselines()
                
                # Detect anomalies in recent events
                self._detect_anomalies()
                
                # Clean up old data
                self._cleanup_old_data()
                
            except Exception as e:
                self.logger.error(f"Error in detection loop: {e}")
            
            # Wait before next iteration
            self._stop_event.wait(60)  # Run every minute
    
    def add_event(self, event: InfrastructureEvent):
        """
        Add infrastructure event for behavioral analysis
        
        Args:
            event: Infrastructure event to analyze
        """
        try:
            with self._event_lock:
                self._recent_events.append(event)
            
            # Update connection patterns for monitored users
            if event.user_account in self._monitored_users:
                self._update_connection_pattern(event)
            
        except Exception as e:
            self.logger.error(f"Error adding event for behavioral analysis: {e}")
    
    def _update_connection_pattern(self, event: InfrastructureEvent):
        """Update connection pattern for a user"""
        try:
            user = event.user_account
            
            if user not in self._connection_patterns:
                self._connection_patterns[user] = ConnectionPattern(user=user)
            
            pattern = self._connection_patterns[user]
            
            # Extract connection details from event
            host = event.source_ip
            timestamp = event.timestamp
            
            # Get additional details from action_details
            details = event.action_details or {}
            command = details.get('command', '')
            database = details.get('database', '')
            duration = details.get('duration', 0.0)
            
            # Count concurrent sessions for this user
            concurrent_count = self._count_concurrent_sessions(user, timestamp)
            
            # Add to pattern
            pattern.add_connection(
                host=host,
                timestamp=timestamp,
                duration=duration,
                command=command,
                database=database,
                concurrent_count=concurrent_count
            )
            
        except Exception as e:
            self.logger.error(f"Error updating connection pattern: {e}")
    
    def _count_concurrent_sessions(self, user: str, timestamp: datetime) -> int:
        """Count concurrent sessions for a user at a given time"""
        try:
            # Look for connection events within a small time window
            window_start = timestamp - timedelta(seconds=30)
            window_end = timestamp + timedelta(seconds=30)
            
            concurrent_count = 0
            
            with self._event_lock:
                for event in self._recent_events:
                    if (event.user_account == user and 
                        window_start <= event.timestamp <= window_end and
                        event.event_type in ['database_connection', 'db_connect']):
                        concurrent_count += 1
            
            return max(concurrent_count, 1)
            
        except Exception as e:
            self.logger.error(f"Error counting concurrent sessions: {e}")
            return 1
    
    def _update_baselines(self):
        """Update baseline behavior profiles"""
        try:
            current_time = datetime.now(timezone.utc)
            
            for user in self._monitored_users:
                if user not in self._connection_patterns:
                    continue
                
                pattern = self._connection_patterns[user]
                
                # Check if we have enough data to establish/update baseline
                if len(pattern.connection_times) < self._min_events_for_baseline:
                    continue
                
                # Create or update baseline
                if user not in self._baselines:
                    self._baselines[user] = BaselineBehavior(
                        user=user,
                        profile_start=min(pattern.connection_times)
                    )
                
                baseline = self._baselines[user]
                self._calculate_baseline_metrics(baseline, pattern, current_time)
                
        except Exception as e:
            self.logger.error(f"Error updating baselines: {e}")
    
    def _calculate_baseline_metrics(self, baseline: BaselineBehavior, 
                                  pattern: ConnectionPattern, current_time: datetime):
        """Calculate baseline metrics from connection pattern"""
        try:
            # Update profile end time
            baseline.profile_end = current_time
            
            # Calculate connection frequency (connections per hour)
            if pattern.connection_times:
                time_span = (max(pattern.connection_times) - min(pattern.connection_times)).total_seconds()
                if time_span > 0:
                    baseline.connection_frequency_per_hour = len(pattern.connection_times) / (time_span / 3600)
            
            # Calculate average session duration
            if pattern.session_durations:
                baseline.avg_session_duration = statistics.mean(pattern.session_durations)
            
            # Calculate maximum concurrent sessions
            if pattern.concurrent_sessions:
                baseline.max_concurrent_sessions = max(pattern.concurrent_sessions)
            
            # Update typical hosts
            baseline.typical_hosts.update(pattern.host_patterns)
            
            # Update typical databases
            baseline.typical_databases.update(pattern.database_access_patterns)
            
            # Calculate active hours and days
            for timestamp in pattern.connection_times:
                baseline.active_hours.add(timestamp.hour)
                baseline.active_days.add(timestamp.weekday())
            
            # Calculate command frequency
            total_commands = sum(pattern.command_patterns.values())
            if total_commands > 0:
                for command, count in pattern.command_patterns.items():
                    baseline.typical_commands[command] = count / total_commands
            
            # Calculate anomaly thresholds
            self._calculate_anomaly_thresholds(baseline, pattern)
            
        except Exception as e:
            self.logger.error(f"Error calculating baseline metrics: {e}")
    
    def _calculate_anomaly_thresholds(self, baseline: BaselineBehavior, pattern: ConnectionPattern):
        """Calculate anomaly detection thresholds from baseline data"""
        try:
            # Connection frequency threshold
            baseline.connection_frequency_threshold = (
                baseline.connection_frequency_per_hour * 
                self._thresholds['connection_frequency_multiplier']
            )
            
            # Session duration threshold
            if pattern.session_durations and len(pattern.session_durations) > 1:
                mean_duration = statistics.mean(pattern.session_durations)
                std_duration = statistics.stdev(pattern.session_durations)
                baseline.session_duration_threshold = (
                    mean_duration + (self._anomaly_threshold * std_duration)
                )
            else:
                baseline.session_duration_threshold = (
                    baseline.avg_session_duration * 
                    self._thresholds['session_duration_multiplier']
                )
            
            # Concurrent session threshold
            baseline.concurrent_session_threshold = max(
                baseline.max_concurrent_sessions,
                self._thresholds['concurrent_session_limit']
            )
            
            # Query frequency threshold (placeholder - would need query data)
            baseline.query_frequency_threshold = (
                baseline.avg_queries_per_minute * 
                self._thresholds['query_frequency_multiplier']
            )
            
        except Exception as e:
            self.logger.error(f"Error calculating anomaly thresholds: {e}")
    
    def _detect_anomalies(self):
        """Detect behavioral anomalies in recent events"""
        try:
            current_time = datetime.now(timezone.utc)
            
            # Look at events from the last few minutes
            recent_window = current_time - timedelta(minutes=5)
            
            with self._event_lock:
                recent_events = [
                    event for event in self._recent_events
                    if event.timestamp >= recent_window
                ]
            
            for event in recent_events:
                if event.user_account in self._monitored_users:
                    anomalies = self._analyze_event_for_anomalies(event)
                    
                    with self._anomaly_lock:
                        self._recent_anomalies.extend(anomalies)
            
        except Exception as e:
            self.logger.error(f"Error detecting anomalies: {e}")
    
    def _analyze_event_for_anomalies(self, event: InfrastructureEvent) -> List[AnomalyDetection]:
        """Analyze a single event for behavioral anomalies"""
        anomalies = []
        
        try:
            user = event.user_account
            
            # Skip if no baseline established or insufficient learning data
            if user not in self._baselines:
                return anomalies
                
            baseline = self._baselines[user]
            
            # Check if baseline is sufficiently established
            if not baseline.is_established():
                return anomalies
            
            # Additional check: ensure we have enough events for reliable detection
            pattern = self._connection_patterns.get(user)
            if pattern and len(pattern.connection_times) < self._min_events_for_baseline:
                return anomalies
            
            # Check baseline maturity - avoid false positives during early learning
            profile_age_hours = (datetime.now(timezone.utc) - baseline.profile_start).total_seconds() / 3600
            if profile_age_hours < self._learning_window_hours:
                # During learning phase, only detect high-confidence anomalies
                return self._detect_high_confidence_anomalies_only(event, baseline)
            
            # Full anomaly detection for mature baselines
            return self._detect_all_anomalies(event, baseline)
            
        except Exception as e:
            self.logger.error(f"Error analyzing event for anomalies: {e}")
        
        return anomalies
    
    def _detect_high_confidence_anomalies_only(self, event: InfrastructureEvent, 
                                             baseline: BaselineBehavior) -> List[AnomalyDetection]:
        """Detect only high-confidence anomalies during learning phase"""
        anomalies = []
        user = event.user_account
        
        try:
            # Only detect severe anomalies during learning phase
            
            # 1. Excessive concurrent sessions (high confidence)
            concurrent_count = self._count_concurrent_sessions(user, event.timestamp)
            if concurrent_count > max(baseline.concurrent_session_threshold, 5):  # Higher threshold during learning
                anomaly = AnomalyDetection(
                    detection_id=str(uuid.uuid4()),
                    timestamp=event.timestamp,
                    user=user,
                    anomaly_type="excessive_concurrent_sessions",
                    severity=ThreatLevel.HIGH,
                    confidence=0.9,  # High confidence
                    baseline_value=baseline.concurrent_session_threshold,
                    observed_value=concurrent_count,
                    deviation_score=concurrent_count / max(baseline.concurrent_session_threshold, 1),
                    context={
                        "event_id": event.event_id,
                        "event_type": event.event_type,
                        "max_baseline_sessions": baseline.max_concurrent_sessions,
                        "learning_phase": True
                    }
                )
                anomalies.append(anomaly)
            
            # 2. Connections from completely new IP ranges (high confidence)
            if event.source_ip not in baseline.typical_hosts:
                # Check if it's from a completely different subnet
                event_ip_parts = event.source_ip.split('.')[:3]  # First 3 octets
                is_new_subnet = True
                
                for known_host in baseline.typical_hosts:
                    known_ip_parts = known_host.split('.')[:3]
                    if event_ip_parts == known_ip_parts:
                        is_new_subnet = False
                        break
                
                if is_new_subnet and len(baseline.typical_hosts) >= 2:  # Only if we have established patterns
                    anomaly = AnomalyDetection(
                        detection_id=str(uuid.uuid4()),
                        timestamp=event.timestamp,
                        user=user,
                        anomaly_type="new_subnet_connection",
                        severity=ThreatLevel.HIGH,
                        confidence=0.8,
                        baseline_value=list(baseline.typical_hosts),
                        observed_value=event.source_ip,
                        deviation_score=1.0,
                        context={
                            "event_id": event.event_id,
                            "event_type": event.event_type,
                            "typical_hosts": list(baseline.typical_hosts),
                            "learning_phase": True,
                            "new_subnet": True
                        }
                    )
                    anomalies.append(anomaly)
            
        except Exception as e:
            self.logger.error(f"Error detecting high-confidence anomalies: {e}")
        
        return anomalies
    
    def _detect_all_anomalies(self, event: InfrastructureEvent, 
                            baseline: BaselineBehavior) -> List[AnomalyDetection]:
        """Detect all types of anomalies for mature baselines"""
        anomalies = []
        user = event.user_account
        
        try:
            # Check for new host anomaly
            if event.source_ip not in baseline.typical_hosts:
                anomaly = AnomalyDetection(
                    detection_id=str(uuid.uuid4()),
                    timestamp=event.timestamp,
                    user=user,
                    anomaly_type="new_host_connection",
                    severity=ThreatLevel.MEDIUM,
                    confidence=self._thresholds['new_host_risk_score'],
                    baseline_value=list(baseline.typical_hosts),
                    observed_value=event.source_ip,
                    deviation_score=1.0,
                    context={
                        "event_id": event.event_id,
                        "event_type": event.event_type,
                        "typical_hosts": list(baseline.typical_hosts)
                    }
                )
                anomalies.append(anomaly)
            
            # Check for unusual time anomaly
            event_hour = event.timestamp.hour
            event_day = event.timestamp.weekday()
            
            if (event_hour not in baseline.active_hours or 
                event_day not in baseline.active_days):
                anomaly = AnomalyDetection(
                    detection_id=str(uuid.uuid4()),
                    timestamp=event.timestamp,
                    user=user,
                    anomaly_type="unusual_time_access",
                    severity=ThreatLevel.LOW,
                    confidence=self._thresholds['unusual_time_risk_score'],
                    baseline_value={
                        "typical_hours": list(baseline.active_hours),
                        "typical_days": list(baseline.active_days)
                    },
                    observed_value={
                        "hour": event_hour,
                        "day": event_day
                    },
                    deviation_score=0.8,
                    context={
                        "event_id": event.event_id,
                        "event_type": event.event_type
                    }
                )
                anomalies.append(anomaly)
            
            # Check for concurrent session anomaly
            concurrent_count = self._count_concurrent_sessions(user, event.timestamp)
            if concurrent_count > baseline.concurrent_session_threshold:
                anomaly = AnomalyDetection(
                    detection_id=str(uuid.uuid4()),
                    timestamp=event.timestamp,
                    user=user,
                    anomaly_type="excessive_concurrent_sessions",
                    severity=ThreatLevel.HIGH,
                    confidence=0.8,
                    baseline_value=baseline.concurrent_session_threshold,
                    observed_value=concurrent_count,
                    deviation_score=concurrent_count / baseline.concurrent_session_threshold,
                    context={
                        "event_id": event.event_id,
                        "event_type": event.event_type,
                        "max_baseline_sessions": baseline.max_concurrent_sessions
                    }
                )
                anomalies.append(anomaly)
            
            # Check for unusual command anomaly
            command = event.action_details.get('command', '') if event.action_details else ''
            if command and command not in baseline.typical_commands:
                anomaly = AnomalyDetection(
                    detection_id=str(uuid.uuid4()),
                    timestamp=event.timestamp,
                    user=user,
                    anomaly_type="unusual_command",
                    severity=ThreatLevel.MEDIUM,
                    confidence=0.6,
                    baseline_value=list(baseline.typical_commands.keys()),
                    observed_value=command,
                    deviation_score=1.0,
                    context={
                        "event_id": event.event_id,
                        "event_type": event.event_type,
                        "command": command
                    }
                )
                anomalies.append(anomaly)
            
        except Exception as e:
            self.logger.error(f"Error detecting all anomalies: {e}")
        
        return anomalies
    
    def _cleanup_old_data(self):
        """Clean up old data to prevent memory leaks"""
        try:
            current_time = datetime.now(timezone.utc)
            cutoff_time = current_time - timedelta(hours=self._learning_window_hours * 2)
            
            # Clean up connection patterns
            for user, pattern in self._connection_patterns.items():
                pattern.connection_times = [
                    t for t in pattern.connection_times if t >= cutoff_time
                ]
            
            # Clean up old anomalies
            with self._anomaly_lock:
                self._recent_anomalies = [
                    anomaly for anomaly in self._recent_anomalies
                    if anomaly.timestamp >= cutoff_time
                ]
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {e}")
    
    def analyze_events(self, events: List[InfrastructureEvent]) -> List[ThreatDetection]:
        """
        Analyze events for behavioral anomalies
        
        Args:
            events: List of infrastructure events to analyze
            
        Returns:
            List of threat detections
        """
        threat_detections = []
        
        try:
            # Add events to behavioral analysis
            for event in events:
                self.add_event(event)
            
            # Get recent anomalies and convert to threat detections
            with self._anomaly_lock:
                recent_anomalies = self._recent_anomalies.copy()
            
            for anomaly in recent_anomalies:
                threat_detection = ThreatDetection(
                    detection_id=anomaly.detection_id,
                    timestamp=anomaly.timestamp,
                    threat_type=f"behavioral_anomaly_{anomaly.anomaly_type}",
                    severity=anomaly.severity,
                    affected_components=[ComponentType.USER_ACCOUNT],
                    attack_indicators={
                        "anomaly_type": anomaly.anomaly_type,
                        "baseline_value": anomaly.baseline_value,
                        "observed_value": anomaly.observed_value,
                        "deviation_score": anomaly.deviation_score,
                        "user": anomaly.user
                    },
                    confidence_score=anomaly.confidence,
                    response_actions=[
                        "monitor_user_activity",
                        "verify_user_identity",
                        "check_session_validity"
                    ],
                    evidence_chain=[anomaly.context.get("event_id", "")]
                )
                
                threat_detections.append(threat_detection)
            
        except Exception as e:
            self.logger.error(f"Error analyzing events for behavioral anomalies: {e}")
        
        return threat_detections
    
    def update_patterns(self, new_patterns: Dict[str, Any]) -> bool:
        """
        Update behavioral detection patterns
        
        Args:
            new_patterns: New detection patterns to apply
            
        Returns:
            True if patterns updated successfully, False otherwise
        """
        try:
            # Update thresholds
            if 'thresholds' in new_patterns:
                self._thresholds.update(new_patterns['thresholds'])
            
            # Update monitored users
            if 'monitored_users' in new_patterns:
                self._monitored_users = new_patterns['monitored_users']
            
            # Update learning parameters
            if 'learning_window_hours' in new_patterns:
                self._learning_window_hours = new_patterns['learning_window_hours']
            
            if 'min_events_for_baseline' in new_patterns:
                self._min_events_for_baseline = new_patterns['min_events_for_baseline']
            
            if 'anomaly_threshold' in new_patterns:
                self._anomaly_threshold = new_patterns['anomaly_threshold']
            
            self.logger.info("Behavioral detection patterns updated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating behavioral detection patterns: {e}")
            return False
    
    def get_detection_rules(self) -> Dict[str, Any]:
        """
        Get current behavioral detection rules
        
        Returns:
            Dictionary containing current detection rules and configuration
        """
        try:
            return {
                "monitored_users": self._monitored_users,
                "thresholds": self._thresholds,
                "learning_window_hours": self._learning_window_hours,
                "min_events_for_baseline": self._min_events_for_baseline,
                "anomaly_threshold": self._anomaly_threshold,
                "baselines_established": {
                    user: baseline.is_established() 
                    for user, baseline in self._baselines.items()
                },
                "active": self._active
            }
            
        except Exception as e:
            self.logger.error(f"Error getting detection rules: {e}")
            return {}
    
    def get_baseline_status(self, user: str) -> Dict[str, Any]:
        """
        Get baseline status for a specific user
        
        Args:
            user: Username to get baseline status for
            
        Returns:
            Dictionary containing baseline status and metrics
        """
        try:
            if user not in self._baselines:
                return {"established": False, "reason": "no_baseline_data"}
            
            baseline = self._baselines[user]
            pattern = self._connection_patterns.get(user)
            
            status = {
                "established": baseline.is_established(),
                "profile_start": baseline.profile_start.isoformat(),
                "profile_end": baseline.profile_end.isoformat() if baseline.profile_end else None,
                "connection_frequency_per_hour": baseline.connection_frequency_per_hour,
                "avg_session_duration": baseline.avg_session_duration,
                "max_concurrent_sessions": baseline.max_concurrent_sessions,
                "typical_hosts": list(baseline.typical_hosts),
                "typical_databases": list(baseline.typical_databases),
                "active_hours": list(baseline.active_hours),
                "active_days": list(baseline.active_days),
                "typical_commands": baseline.typical_commands,
                "thresholds": {
                    "connection_frequency": baseline.connection_frequency_threshold,
                    "session_duration": baseline.session_duration_threshold,
                    "concurrent_sessions": baseline.concurrent_session_threshold,
                    "query_frequency": baseline.query_frequency_threshold
                }
            }
            
            if pattern:
                status["events_collected"] = len(pattern.connection_times)
            
            return status
            
        except Exception as e:
            self.logger.error(f"Error getting baseline status for user {user}: {e}")
            return {"established": False, "error": str(e)}
    
    def get_recent_anomalies(self, user: Optional[str] = None, 
                           hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get recent anomalies for analysis
        
        Args:
            user: Optional user filter
            hours: Hours to look back for anomalies
            
        Returns:
            List of recent anomaly dictionaries
        """
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
            
            with self._anomaly_lock:
                filtered_anomalies = [
                    anomaly for anomaly in self._recent_anomalies
                    if (anomaly.timestamp >= cutoff_time and 
                        (user is None or anomaly.user == user))
                ]
            
            return [
                {
                    "detection_id": anomaly.detection_id,
                    "timestamp": anomaly.timestamp.isoformat(),
                    "user": anomaly.user,
                    "anomaly_type": anomaly.anomaly_type,
                    "severity": anomaly.severity.value,
                    "confidence": anomaly.confidence,
                    "baseline_value": anomaly.baseline_value,
                    "observed_value": anomaly.observed_value,
                    "deviation_score": anomaly.deviation_score,
                    "context": anomaly.context
                }
                for anomaly in filtered_anomalies
            ]
            
        except Exception as e:
            self.logger.error(f"Error getting recent anomalies: {e}")
            return []
    
    def is_healthy(self) -> bool:
        """Check if the behavioral anomaly detector is healthy"""
        try:
            return (self._active and 
                   self._detection_thread and 
                   self._detection_thread.is_alive())
        except Exception as e:
            self.logger.error(f"Error checking health: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get behavioral anomaly detection statistics"""
        try:
            with self._anomaly_lock:
                anomaly_count = len(self._recent_anomalies)
            
            return {
                "active": self._active,
                "monitored_users": len(self._monitored_users),
                "baselines_established": len([
                    b for b in self._baselines.values() if b.is_established()
                ]),
                "total_baselines": len(self._baselines),
                "recent_events": len(self._recent_events),
                "recent_anomalies": anomaly_count,
                "connection_patterns": len(self._connection_patterns)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting statistics: {e}")
            return {}