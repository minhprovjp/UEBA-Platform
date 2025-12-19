"""
Attack Pattern Recognition Engine for UBA Self-Monitoring System

This module implements comprehensive attack pattern recognition capabilities including
malicious query pattern database, privilege escalation detection, lateral movement
detection, and persistence detection mechanisms.
"""

import logging
import re
import json
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple, Pattern
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import uuid

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


class AttackType(Enum):
    """Types of attacks that can be detected"""
    SQL_INJECTION = "sql_injection"
    PRIVILEGE_ESCALATION = "privilege_escalation"
    LATERAL_MOVEMENT = "lateral_movement"
    PERSISTENCE = "persistence"
    RECONNAISSANCE = "reconnaissance"
    DATA_EXFILTRATION = "data_exfiltration"
    CREDENTIAL_HARVESTING = "credential_harvesting"
    SYSTEM_MANIPULATION = "system_manipulation"


@dataclass
class AttackPattern:
    """Attack pattern definition"""
    pattern_id: str
    name: str
    attack_type: AttackType
    regex_patterns: List[str]
    compiled_patterns: List[Pattern] = field(default_factory=list, init=False)
    severity: ThreatLevel = ThreatLevel.MEDIUM
    confidence_weight: float = 1.0
    description: str = ""
    indicators: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Compile regex patterns after initialization"""
        self.compiled_patterns = []
        for pattern in self.regex_patterns:
            try:
                self.compiled_patterns.append(re.compile(pattern, re.IGNORECASE))
            except re.error as e:
                logging.error(f"Invalid regex pattern '{pattern}': {e}")


@dataclass
class AttackSequence:
    """Sequence of related attack events"""
    sequence_id: str
    attack_type: AttackType
    events: List[InfrastructureEvent] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    source_ips: Set[str] = field(default_factory=set)
    target_components: Set[ComponentType] = field(default_factory=set)
    confidence_score: float = 0.0
    
    def add_event(self, event: InfrastructureEvent, confidence: float):
        """Add an event to the attack sequence"""
        self.events.append(event)
        self.source_ips.add(event.source_ip)
        self.target_components.add(event.target_component)
        
        if not self.start_time or event.timestamp < self.start_time:
            self.start_time = event.timestamp
        if not self.end_time or event.timestamp > self.end_time:
            self.end_time = event.timestamp
        
        # Update confidence score (weighted average)
        total_weight = len(self.events)
        self.confidence_score = ((self.confidence_score * (total_weight - 1)) + confidence) / total_weight


@dataclass
class AttackIndicator:
    """Individual attack indicator from pattern matching"""
    indicator_id: str
    event: InfrastructureEvent
    pattern: AttackPattern
    matched_content: str
    confidence: float
    timestamp: datetime
    context: Dict[str, Any] = field(default_factory=dict)


class AttackPatternRecognitionEngine(DetectionInterface):
    """
    Attack pattern recognition engine for detecting sophisticated attacks
    
    This class implements comprehensive attack pattern recognition including
    malicious query patterns, privilege escalation, lateral movement, and
    persistence detection mechanisms.
    """
    
    def __init__(self, config_manager: Optional[SelfMonitoringConfig] = None,
                 crypto_logger: Optional[CryptoLogger] = None):
        """
        Initialize attack pattern recognition engine
        
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
        
        # Pattern database
        self._attack_patterns: Dict[str, AttackPattern] = {}
        self._patterns_by_type: Dict[AttackType, List[AttackPattern]] = defaultdict(list)
        
        # Event tracking
        self._recent_events: deque = deque(maxlen=10000)
        self._event_lock = threading.Lock()
        
        # Attack sequence tracking
        self._active_sequences: Dict[str, AttackSequence] = {}
        self._completed_sequences: List[AttackSequence] = []
        self._sequence_lock = threading.Lock()
        
        # Detection results
        self._recent_detections: List[ThreatDetection] = []
        self._detection_lock = threading.Lock()
        
        # Load configuration and patterns
        self._load_detection_config()
        self._initialize_attack_patterns()
        
        # Sequence correlation parameters
        self._sequence_timeout = 3600  # 1 hour timeout for sequences
        self._min_sequence_events = 2  # Minimum events to form a sequence
    
    def _load_detection_config(self):
        """Load detection configuration"""
        try:
            config = self.config_manager.load_config()
            attack_config = config.get('attack_recognition', {})
            
            self._sequence_timeout = attack_config.get('sequence_timeout_seconds', 3600)
            self._min_sequence_events = attack_config.get('min_sequence_events', 2)
            
            # Detection thresholds
            self._thresholds = attack_config.get('thresholds', {
                'sql_injection_confidence': 0.7,
                'privilege_escalation_confidence': 0.8,
                'lateral_movement_confidence': 0.6,
                'persistence_confidence': 0.9,
                'reconnaissance_confidence': 0.5,
                'sequence_correlation_window': 300  # 5 minutes
            })
            
            self.logger.info("Attack recognition configuration loaded")
            
        except Exception as e:
            self.logger.error(f"Error loading attack recognition configuration: {e}")
            # Use safe defaults
            self._thresholds = {
                'sql_injection_confidence': 0.7,
                'privilege_escalation_confidence': 0.8,
                'lateral_movement_confidence': 0.6,
                'persistence_confidence': 0.9,
                'reconnaissance_confidence': 0.5,
                'sequence_correlation_window': 300
            }
    
    def _initialize_attack_patterns(self):
        """Initialize attack pattern database"""
        try:
            # SQL Injection patterns
            sql_injection_patterns = [
                AttackPattern(
                    pattern_id="sqli_001",
                    name="Union-based SQL Injection",
                    attack_type=AttackType.SQL_INJECTION,
                    regex_patterns=[
                        r"(?i)union\s+select",
                        r"(?i)union\s+all\s+select",
                        r"(?i)'\s*union\s+select"
                    ],
                    severity=ThreatLevel.HIGH,
                    confidence_weight=0.9,
                    description="Union-based SQL injection attempt",
                    indicators=["union_select", "sql_injection"]
                ),
                AttackPattern(
                    pattern_id="sqli_002",
                    name="Boolean-based SQL Injection",
                    attack_type=AttackType.SQL_INJECTION,
                    regex_patterns=[
                        r"(?i)or\s+1\s*=\s*1",
                        r"(?i)and\s+1\s*=\s*1",
                        r"(?i)'\s*or\s*'1'\s*=\s*'1",
                        r"(?i)'\s*and\s*'1'\s*=\s*'1"
                    ],
                    severity=ThreatLevel.HIGH,
                    confidence_weight=0.8,
                    description="Boolean-based SQL injection attempt",
                    indicators=["boolean_sqli", "sql_injection"]
                ),
                AttackPattern(
                    pattern_id="sqli_003",
                    name="Time-based SQL Injection",
                    attack_type=AttackType.SQL_INJECTION,
                    regex_patterns=[
                        r"(?i)sleep\s*\(\s*\d+\s*\)",
                        r"(?i)benchmark\s*\(",
                        r"(?i)waitfor\s+delay",
                        r"(?i)pg_sleep\s*\("
                    ],
                    severity=ThreatLevel.HIGH,
                    confidence_weight=0.9,
                    description="Time-based SQL injection attempt",
                    indicators=["time_based_sqli", "sql_injection"]
                )
            ]
            
            # Privilege escalation patterns
            privilege_escalation_patterns = [
                AttackPattern(
                    pattern_id="privesc_001",
                    name="User Creation/Modification",
                    attack_type=AttackType.PRIVILEGE_ESCALATION,
                    regex_patterns=[
                        r"(?i)create\s+user",
                        r"(?i)alter\s+user",
                        r"(?i)drop\s+user",
                        r"(?i)rename\s+user"
                    ],
                    severity=ThreatLevel.CRITICAL,
                    confidence_weight=0.9,
                    description="Unauthorized user account manipulation",
                    indicators=["user_manipulation", "privilege_escalation"]
                ),
                AttackPattern(
                    pattern_id="privesc_002",
                    name="Privilege Grant Operations",
                    attack_type=AttackType.PRIVILEGE_ESCALATION,
                    regex_patterns=[
                        r"(?i)grant\s+.*\s+to",
                        r"(?i)grant\s+all\s+privileges",
                        r"(?i)grant\s+.*\s+with\s+grant\s+option",
                        r"(?i)revoke\s+.*\s+from"
                    ],
                    severity=ThreatLevel.CRITICAL,
                    confidence_weight=0.95,
                    description="Unauthorized privilege modification",
                    indicators=["privilege_grant", "privilege_escalation"]
                ),
                AttackPattern(
                    pattern_id="privesc_003",
                    name="System Configuration Changes",
                    attack_type=AttackType.PRIVILEGE_ESCALATION,
                    regex_patterns=[
                        r"(?i)set\s+global",
                        r"(?i)flush\s+privileges",
                        r"(?i)reset\s+master",
                        r"(?i)shutdown"
                    ],
                    severity=ThreatLevel.CRITICAL,
                    confidence_weight=0.9,
                    description="Unauthorized system configuration changes",
                    indicators=["system_config", "privilege_escalation"]
                )
            ]
            
            # Reconnaissance patterns
            reconnaissance_patterns = [
                AttackPattern(
                    pattern_id="recon_001",
                    name="Database Enumeration",
                    attack_type=AttackType.RECONNAISSANCE,
                    regex_patterns=[
                        r"(?i)select\s+schema_name\s+from\s+information_schema\.schemata",
                        r"(?i)select\s+table_name\s+from\s+information_schema\.tables",
                        r"(?i)select\s+column_name\s+from\s+information_schema\.columns",
                        r"(?i)show\s+databases",
                        r"(?i)show\s+tables"
                    ],
                    severity=ThreatLevel.MEDIUM,
                    confidence_weight=0.7,
                    description="Database structure enumeration",
                    indicators=["database_enum", "reconnaissance"]
                ),
                AttackPattern(
                    pattern_id="recon_002",
                    name="User and Privilege Enumeration",
                    attack_type=AttackType.RECONNAISSANCE,
                    regex_patterns=[
                        r"(?i)select\s+.*\s+from\s+mysql\.user",
                        r"(?i)select\s+grantee\s+from\s+information_schema\.user_privileges",
                        r"(?i)show\s+grants",
                        r"(?i)select\s+user\s*\(\s*\)",
                        r"(?i)select\s+current_user"
                    ],
                    severity=ThreatLevel.MEDIUM,
                    confidence_weight=0.8,
                    description="User and privilege enumeration",
                    indicators=["user_enum", "reconnaissance"]
                ),
                AttackPattern(
                    pattern_id="recon_003",
                    name="System Information Gathering",
                    attack_type=AttackType.RECONNAISSANCE,
                    regex_patterns=[
                        r"(?i)select\s+version\s*\(\s*\)",
                        r"(?i)select\s+@@version",
                        r"(?i)show\s+variables",
                        r"(?i)show\s+status",
                        r"(?i)show\s+processlist"
                    ],
                    severity=ThreatLevel.LOW,
                    confidence_weight=0.6,
                    description="System information gathering",
                    indicators=["system_info", "reconnaissance"]
                )
            ]
            
            # Persistence patterns
            persistence_patterns = [
                AttackPattern(
                    pattern_id="persist_001",
                    name="Backdoor User Creation",
                    attack_type=AttackType.PERSISTENCE,
                    regex_patterns=[
                        r"(?i)create\s+user\s+.*\s+identified\s+by",
                        r"(?i)insert\s+into\s+mysql\.user"
                    ],
                    severity=ThreatLevel.CRITICAL,
                    confidence_weight=0.95,
                    description="Creation of backdoor user accounts",
                    indicators=["backdoor_user", "persistence"]
                ),
                AttackPattern(
                    pattern_id="persist_002",
                    name="Trigger/Procedure Installation",
                    attack_type=AttackType.PERSISTENCE,
                    regex_patterns=[
                        r"(?i)create\s+trigger",
                        r"(?i)create\s+procedure",
                        r"(?i)create\s+function"
                    ],
                    severity=ThreatLevel.HIGH,
                    confidence_weight=0.8,
                    description="Installation of persistent triggers or procedures",
                    indicators=["trigger_install", "persistence"]
                )
            ]
            
            # Lateral movement patterns
            lateral_movement_patterns = [
                AttackPattern(
                    pattern_id="lateral_001",
                    name="Cross-Database Access",
                    attack_type=AttackType.LATERAL_MOVEMENT,
                    regex_patterns=[
                        r"(?i)use\s+\w+",
                        r"(?i)select\s+.*\s+from\s+\w+\.\w+"
                    ],
                    severity=ThreatLevel.MEDIUM,
                    confidence_weight=0.6,
                    description="Unauthorized cross-database access",
                    indicators=["cross_db_access", "lateral_movement"]
                ),
                AttackPattern(
                    pattern_id="lateral_002",
                    name="Network Function Usage",
                    attack_type=AttackType.LATERAL_MOVEMENT,
                    regex_patterns=[
                        r"(?i)load_file\s*\(",
                        r"(?i)into\s+outfile",
                        r"(?i)into\s+dumpfile"
                    ],
                    severity=ThreatLevel.HIGH,
                    confidence_weight=0.9,
                    description="Use of network functions for lateral movement",
                    indicators=["network_functions", "lateral_movement"]
                )
            ]
            
            # Data exfiltration patterns
            data_exfiltration_patterns = [
                AttackPattern(
                    pattern_id="exfil_001",
                    name="Bulk Data Extraction",
                    attack_type=AttackType.DATA_EXFILTRATION,
                    regex_patterns=[
                        r"(?i)select\s+\*\s+from\s+\w+\s+limit\s+\d{3,}",
                        r"(?i)select\s+count\s*\(\s*\*\s*\)\s+from",
                        r"(?i)mysqldump"
                    ],
                    severity=ThreatLevel.HIGH,
                    confidence_weight=0.8,
                    description="Bulk data extraction attempt",
                    indicators=["bulk_extraction", "data_exfiltration"]
                )
            ]
            
            # Add all patterns to the database
            all_patterns = (
                sql_injection_patterns + 
                privilege_escalation_patterns + 
                reconnaissance_patterns + 
                persistence_patterns + 
                lateral_movement_patterns + 
                data_exfiltration_patterns
            )
            
            for pattern in all_patterns:
                self._attack_patterns[pattern.pattern_id] = pattern
                self._patterns_by_type[pattern.attack_type].append(pattern)
            
            self.logger.info(f"Initialized {len(all_patterns)} attack patterns")
            
        except Exception as e:
            self.logger.error(f"Error initializing attack patterns: {e}")
    
    def start_detection(self) -> bool:
        """Start attack pattern recognition"""
        try:
            if self._active:
                self.logger.warning("Attack pattern recognition is already active")
                return True
            
            # Start detection thread
            self._active = True
            self._stop_event.clear()
            self._detection_thread = threading.Thread(target=self._detection_loop, daemon=True)
            self._detection_thread.start()
            
            self.logger.info("Attack pattern recognition started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting attack pattern recognition: {e}")
            self._active = False
            return False
    
    def stop_detection(self) -> bool:
        """Stop attack pattern recognition"""
        try:
            if not self._active:
                self.logger.warning("Attack pattern recognition is not active")
                return True
            
            # Signal stop and wait for thread
            self._stop_event.set()
            self._active = False
            
            if self._detection_thread and self._detection_thread.is_alive():
                self._detection_thread.join(timeout=10)
            
            self.logger.info("Attack pattern recognition stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping attack pattern recognition: {e}")
            return False
    
    def _detection_loop(self):
        """Main detection loop"""
        while not self._stop_event.is_set():
            try:
                # Process recent events for attack patterns
                self._process_events_for_patterns()
                
                # Correlate events into attack sequences
                self._correlate_attack_sequences()
                
                # Clean up old data
                self._cleanup_old_data()
                
            except Exception as e:
                self.logger.error(f"Error in attack pattern detection loop: {e}")
            
            # Wait before next iteration
            self._stop_event.wait(30)  # Run every 30 seconds
    
    def add_event(self, event: InfrastructureEvent):
        """
        Add infrastructure event for attack pattern analysis
        
        Args:
            event: Infrastructure event to analyze
        """
        try:
            with self._event_lock:
                self._recent_events.append(event)
            
        except Exception as e:
            self.logger.error(f"Error adding event for attack pattern analysis: {e}")
    
    def _process_events_for_patterns(self):
        """Process recent events for attack pattern matching"""
        try:
            # Get events from the last few minutes
            current_time = datetime.now(timezone.utc)
            recent_window = current_time - timedelta(minutes=5)
            
            with self._event_lock:
                recent_events = [
                    event for event in self._recent_events
                    if event.timestamp >= recent_window
                ]
            
            for event in recent_events:
                indicators = self._analyze_event_for_attack_patterns(event)
                
                if indicators:
                    # Process indicators for threat detection
                    self._process_attack_indicators(indicators)
            
        except Exception as e:
            self.logger.error(f"Error processing events for attack patterns: {e}")
    
    def _analyze_event_for_attack_patterns(self, event: InfrastructureEvent) -> List[AttackIndicator]:
        """Analyze a single event for attack patterns"""
        indicators = []
        
        try:
            # Extract content to analyze
            content_sources = []
            
            # Add query content if available
            if event.action_details:
                query = event.action_details.get('query', '')
                query_snippet = event.action_details.get('query_snippet', '')
                argument = event.action_details.get('argument', '')
                
                if query:
                    content_sources.append(('query', query))
                if query_snippet:
                    content_sources.append(('query_snippet', query_snippet))
                if argument:
                    content_sources.append(('argument', argument))
            
            # Analyze each content source against all patterns
            for content_type, content in content_sources:
                if not content or len(content) < 5:  # Skip very short content
                    continue
                
                for pattern in self._attack_patterns.values():
                    matches = self._match_pattern_against_content(pattern, content)
                    
                    for match in matches:
                        confidence = self._calculate_pattern_confidence(pattern, match, event)
                        
                        if confidence >= self._get_confidence_threshold(pattern.attack_type):
                            indicator = AttackIndicator(
                                indicator_id=str(uuid.uuid4()),
                                event=event,
                                pattern=pattern,
                                matched_content=match,
                                confidence=confidence,
                                timestamp=event.timestamp,
                                context={
                                    'content_type': content_type,
                                    'full_content': content[:500],  # Truncate for storage
                                    'event_type': event.event_type,
                                    'source_ip': event.source_ip,
                                    'user_account': event.user_account
                                }
                            )
                            indicators.append(indicator)
            
        except Exception as e:
            self.logger.error(f"Error analyzing event for attack patterns: {e}")
        
        return indicators
    
    def _match_pattern_against_content(self, pattern: AttackPattern, content: str) -> List[str]:
        """Match a pattern against content and return matches"""
        matches = []
        
        try:
            for compiled_pattern in pattern.compiled_patterns:
                pattern_matches = compiled_pattern.findall(content)
                if pattern_matches:
                    # Handle both string matches and tuple matches from groups
                    for match in pattern_matches:
                        if isinstance(match, tuple):
                            match_str = ' '.join(str(m) for m in match if m)
                        else:
                            match_str = str(match)
                        
                        if match_str and match_str not in matches:
                            matches.append(match_str)
            
        except Exception as e:
            self.logger.error(f"Error matching pattern {pattern.pattern_id}: {e}")
        
        return matches
    
    def _calculate_pattern_confidence(self, pattern: AttackPattern, match: str, event: InfrastructureEvent) -> float:
        """Calculate confidence score for a pattern match"""
        try:
            base_confidence = pattern.confidence_weight
            
            # Adjust confidence based on context
            confidence_adjustments = 0.0
            
            # Higher confidence for uba_user account (critical system account)
            if event.user_account == 'uba_user':
                confidence_adjustments += 0.2
            
            # Higher confidence for sensitive databases
            if event.action_details:
                database = event.action_details.get('database', '')
                if database in ['mysql', 'information_schema', 'performance_schema']:
                    confidence_adjustments += 0.15
            
            # Higher confidence for remote connections
            if event.source_ip not in ['localhost', '127.0.0.1', '::1']:
                confidence_adjustments += 0.1
            
            # Higher confidence for longer/more complex matches
            if len(match) > 20:
                confidence_adjustments += 0.05
            
            # Higher confidence during unusual hours
            event_hour = event.timestamp.hour
            if event_hour < 6 or event_hour > 22:  # Outside business hours
                confidence_adjustments += 0.1
            
            final_confidence = min(base_confidence + confidence_adjustments, 1.0)
            return final_confidence
            
        except Exception as e:
            self.logger.error(f"Error calculating pattern confidence: {e}")
            return pattern.confidence_weight
    
    def _get_confidence_threshold(self, attack_type: AttackType) -> float:
        """Get confidence threshold for attack type"""
        threshold_map = {
            AttackType.SQL_INJECTION: self._thresholds.get('sql_injection_confidence', 0.7),
            AttackType.PRIVILEGE_ESCALATION: self._thresholds.get('privilege_escalation_confidence', 0.8),
            AttackType.LATERAL_MOVEMENT: self._thresholds.get('lateral_movement_confidence', 0.6),
            AttackType.PERSISTENCE: self._thresholds.get('persistence_confidence', 0.9),
            AttackType.RECONNAISSANCE: self._thresholds.get('reconnaissance_confidence', 0.5),
            AttackType.DATA_EXFILTRATION: 0.7,
            AttackType.CREDENTIAL_HARVESTING: 0.8,
            AttackType.SYSTEM_MANIPULATION: 0.8
        }
        
        return threshold_map.get(attack_type, 0.7)
    
    def _process_attack_indicators(self, indicators: List[AttackIndicator]):
        """Process attack indicators and create threat detections"""
        try:
            # Group indicators by attack type and source
            grouped_indicators = defaultdict(list)
            
            for indicator in indicators:
                key = (indicator.pattern.attack_type, indicator.event.source_ip, indicator.event.user_account)
                grouped_indicators[key].append(indicator)
            
            # Create threat detections for each group
            for (attack_type, source_ip, user_account), group_indicators in grouped_indicators.items():
                if len(group_indicators) >= 1:  # At least one indicator
                    threat_detection = self._create_threat_detection_from_indicators(
                        attack_type, source_ip, user_account, group_indicators
                    )
                    
                    with self._detection_lock:
                        self._recent_detections.append(threat_detection)
                    
                    # Log high-severity detections
                    if threat_detection.severity in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                        self.crypto_logger.log_monitoring_event(
                            f"attack_pattern_detected_{attack_type.value}",
                            "uba_infrastructure",
                            {
                                "attack_type": attack_type.value,
                                "source_ip": source_ip,
                                "user_account": user_account,
                                "confidence": threat_detection.confidence_score,
                                "indicators_count": len(group_indicators)
                            },
                            threat_detection.confidence_score
                        )
            
        except Exception as e:
            self.logger.error(f"Error processing attack indicators: {e}")
    
    def _create_threat_detection_from_indicators(self, attack_type: AttackType, source_ip: str, 
                                               user_account: str, indicators: List[AttackIndicator]) -> ThreatDetection:
        """Create threat detection from attack indicators"""
        try:
            # Calculate overall confidence
            total_confidence = sum(indicator.confidence for indicator in indicators)
            avg_confidence = total_confidence / len(indicators)
            
            # Determine severity based on attack type and confidence
            severity = self._determine_threat_severity(attack_type, avg_confidence)
            
            # Collect evidence
            evidence_chain = [indicator.indicator_id for indicator in indicators]
            
            # Collect attack indicators
            attack_indicators = {
                "attack_type": attack_type.value,
                "patterns_matched": [indicator.pattern.pattern_id for indicator in indicators],
                "matched_content": [indicator.matched_content for indicator in indicators],
                "source_ip": source_ip,
                "user_account": user_account,
                "event_count": len(indicators),
                "time_span": self._calculate_time_span(indicators)
            }
            
            # Determine affected components
            affected_components = list(set(
                indicator.event.target_component for indicator in indicators
            ))
            
            # Determine response actions
            response_actions = self._get_response_actions(attack_type, severity)
            
            threat_detection = ThreatDetection(
                detection_id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                threat_type=f"attack_pattern_{attack_type.value}",
                severity=severity,
                affected_components=affected_components,
                attack_indicators=attack_indicators,
                confidence_score=avg_confidence,
                response_actions=response_actions,
                evidence_chain=evidence_chain
            )
            
            return threat_detection
            
        except Exception as e:
            self.logger.error(f"Error creating threat detection from indicators: {e}")
            # Return a basic threat detection
            return ThreatDetection(
                detection_id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                threat_type=f"attack_pattern_{attack_type.value}",
                severity=ThreatLevel.MEDIUM,
                affected_components=[ComponentType.DATABASE],
                attack_indicators={"attack_type": attack_type.value},
                confidence_score=0.5,
                response_actions=["investigate_activity"],
                evidence_chain=[]
            )
    
    def _determine_threat_severity(self, attack_type: AttackType, confidence: float) -> ThreatLevel:
        """Determine threat severity based on attack type and confidence"""
        # Base severity by attack type
        base_severity_map = {
            AttackType.SQL_INJECTION: ThreatLevel.HIGH,
            AttackType.PRIVILEGE_ESCALATION: ThreatLevel.CRITICAL,
            AttackType.LATERAL_MOVEMENT: ThreatLevel.HIGH,
            AttackType.PERSISTENCE: ThreatLevel.CRITICAL,
            AttackType.RECONNAISSANCE: ThreatLevel.MEDIUM,
            AttackType.DATA_EXFILTRATION: ThreatLevel.HIGH,
            AttackType.CREDENTIAL_HARVESTING: ThreatLevel.HIGH,
            AttackType.SYSTEM_MANIPULATION: ThreatLevel.CRITICAL
        }
        
        base_severity = base_severity_map.get(attack_type, ThreatLevel.MEDIUM)
        
        # Adjust based on confidence
        if confidence >= 0.9:
            # Very high confidence - escalate severity
            if base_severity == ThreatLevel.MEDIUM:
                return ThreatLevel.HIGH
            elif base_severity == ThreatLevel.HIGH:
                return ThreatLevel.CRITICAL
        elif confidence < 0.6:
            # Lower confidence - reduce severity
            if base_severity == ThreatLevel.CRITICAL:
                return ThreatLevel.HIGH
            elif base_severity == ThreatLevel.HIGH:
                return ThreatLevel.MEDIUM
        
        return base_severity
    
    def _calculate_time_span(self, indicators: List[AttackIndicator]) -> float:
        """Calculate time span of indicators in seconds"""
        try:
            if len(indicators) <= 1:
                return 0.0
            
            timestamps = [indicator.timestamp for indicator in indicators]
            min_time = min(timestamps)
            max_time = max(timestamps)
            
            return (max_time - min_time).total_seconds()
            
        except Exception as e:
            self.logger.error(f"Error calculating time span: {e}")
            return 0.0
    
    def _get_response_actions(self, attack_type: AttackType, severity: ThreatLevel) -> List[str]:
        """Get recommended response actions for attack type and severity"""
        base_actions = ["investigate_activity", "monitor_user_activity"]
        
        # Add attack-type specific actions
        if attack_type == AttackType.SQL_INJECTION:
            base_actions.extend(["block_malicious_queries", "review_input_validation"])
        elif attack_type == AttackType.PRIVILEGE_ESCALATION:
            base_actions.extend(["review_user_privileges", "audit_privilege_changes"])
        elif attack_type == AttackType.LATERAL_MOVEMENT:
            base_actions.extend(["isolate_affected_systems", "review_network_access"])
        elif attack_type == AttackType.PERSISTENCE:
            base_actions.extend(["scan_for_backdoors", "review_system_changes"])
        elif attack_type == AttackType.RECONNAISSANCE:
            base_actions.extend(["monitor_information_access", "review_access_patterns"])
        
        # Add severity-based actions
        if severity in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
            base_actions.extend(["escalate_to_security_team", "consider_account_suspension"])
        
        if severity == ThreatLevel.CRITICAL:
            base_actions.extend(["immediate_incident_response", "emergency_containment"])
        
        return list(set(base_actions))  # Remove duplicates
    
    def _correlate_attack_sequences(self):
        """Correlate individual attacks into sequences"""
        try:
            correlation_window = self._thresholds.get('sequence_correlation_window', 300)
            current_time = datetime.now(timezone.utc)
            
            with self._detection_lock:
                recent_detections = [
                    detection for detection in self._recent_detections
                    if (current_time - detection.timestamp).total_seconds() <= correlation_window
                ]
            
            # Group detections by source and attack type for sequence analysis
            grouped_detections = defaultdict(list)
            
            for detection in recent_detections:
                source_ip = detection.attack_indicators.get('source_ip', 'unknown')
                user_account = detection.attack_indicators.get('user_account', 'unknown')
                attack_type_str = detection.attack_indicators.get('attack_type', 'unknown')
                
                try:
                    attack_type = AttackType(attack_type_str)
                except ValueError:
                    continue
                
                key = (source_ip, user_account, attack_type)
                grouped_detections[key].append(detection)
            
            # Create or update attack sequences
            with self._sequence_lock:
                for (source_ip, user_account, attack_type), detections in grouped_detections.items():
                    if len(detections) >= self._min_sequence_events:
                        sequence_key = f"{source_ip}_{user_account}_{attack_type.value}"
                        
                        if sequence_key not in self._active_sequences:
                            # Create new sequence
                            sequence = AttackSequence(
                                sequence_id=str(uuid.uuid4()),
                                attack_type=attack_type
                            )
                            self._active_sequences[sequence_key] = sequence
                        
                        # Add detections to sequence (convert to events)
                        sequence = self._active_sequences[sequence_key]
                        for detection in detections:
                            # Create a synthetic event from detection for sequence tracking
                            synthetic_event = InfrastructureEvent(
                                event_id=detection.detection_id,
                                timestamp=detection.timestamp,
                                event_type=f"attack_detection_{attack_type.value}",
                                source_ip=source_ip,
                                user_account=user_account,
                                target_component=detection.affected_components[0] if detection.affected_components else ComponentType.DATABASE,
                                action_details=detection.attack_indicators,
                                risk_score=detection.confidence_score,
                                integrity_hash=""
                            )
                            
                            sequence.add_event(synthetic_event, detection.confidence_score)
            
        except Exception as e:
            self.logger.error(f"Error correlating attack sequences: {e}")
    
    def _cleanup_old_data(self):
        """Clean up old data to prevent memory leaks"""
        try:
            current_time = datetime.now(timezone.utc)
            cutoff_time = current_time - timedelta(hours=24)
            
            # Clean up old detections
            with self._detection_lock:
                self._recent_detections = [
                    detection for detection in self._recent_detections
                    if detection.timestamp >= cutoff_time
                ]
            
            # Clean up old sequences
            with self._sequence_lock:
                expired_sequences = []
                for key, sequence in self._active_sequences.items():
                    if sequence.end_time and (current_time - sequence.end_time).total_seconds() > self._sequence_timeout:
                        expired_sequences.append(key)
                        self._completed_sequences.append(sequence)
                
                for key in expired_sequences:
                    del self._active_sequences[key]
                
                # Limit completed sequences
                if len(self._completed_sequences) > 1000:
                    self._completed_sequences = self._completed_sequences[-500:]
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {e}")
    
    def analyze_events(self, events: List[InfrastructureEvent]) -> List[ThreatDetection]:
        """
        Analyze events for attack patterns
        
        Args:
            events: List of infrastructure events to analyze
            
        Returns:
            List of threat detections
        """
        threat_detections = []
        
        try:
            # Add events to pattern analysis
            for event in events:
                self.add_event(event)
            
            # Process events for attack patterns
            all_indicators = []
            for event in events:
                indicators = self._analyze_event_for_attack_patterns(event)
                all_indicators.extend(indicators)
            
            # Process indicators to create threat detections
            if all_indicators:
                self._process_attack_indicators(all_indicators)
            
            # Return recent detections
            with self._detection_lock:
                threat_detections = self._recent_detections.copy()
            
        except Exception as e:
            self.logger.error(f"Error analyzing events for attack patterns: {e}")
        
        return threat_detections
    
    def update_patterns(self, new_patterns: Dict[str, Any]) -> bool:
        """
        Update attack detection patterns
        
        Args:
            new_patterns: New attack patterns to apply
            
        Returns:
            True if patterns updated successfully, False otherwise
        """
        try:
            # Update thresholds
            if 'thresholds' in new_patterns:
                self._thresholds.update(new_patterns['thresholds'])
            
            # Update sequence parameters
            if 'sequence_timeout_seconds' in new_patterns:
                self._sequence_timeout = new_patterns['sequence_timeout_seconds']
            
            if 'min_sequence_events' in new_patterns:
                self._min_sequence_events = new_patterns['min_sequence_events']
            
            # Add new attack patterns
            if 'attack_patterns' in new_patterns:
                for pattern_data in new_patterns['attack_patterns']:
                    try:
                        pattern = AttackPattern(**pattern_data)
                        self._attack_patterns[pattern.pattern_id] = pattern
                        self._patterns_by_type[pattern.attack_type].append(pattern)
                    except Exception as e:
                        self.logger.error(f"Error adding attack pattern: {e}")
            
            self.logger.info("Attack detection patterns updated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating attack detection patterns: {e}")
            return False
    
    def get_detection_rules(self) -> Dict[str, Any]:
        """
        Get current attack detection rules
        
        Returns:
            Dictionary containing current detection rules and configuration
        """
        try:
            return {
                "attack_patterns": {
                    pattern_id: {
                        "name": pattern.name,
                        "attack_type": pattern.attack_type.value,
                        "severity": pattern.severity.value,
                        "confidence_weight": pattern.confidence_weight,
                        "description": pattern.description,
                        "indicators": pattern.indicators
                    }
                    for pattern_id, pattern in self._attack_patterns.items()
                },
                "thresholds": self._thresholds,
                "sequence_timeout": self._sequence_timeout,
                "min_sequence_events": self._min_sequence_events,
                "patterns_by_type": {
                    attack_type.value: len(patterns)
                    for attack_type, patterns in self._patterns_by_type.items()
                },
                "active": self._active
            }
            
        except Exception as e:
            self.logger.error(f"Error getting detection rules: {e}")
            return {}
    
    def get_recent_detections(self, attack_type: Optional[AttackType] = None, 
                            hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get recent attack detections
        
        Args:
            attack_type: Optional attack type filter
            hours: Hours to look back for detections
            
        Returns:
            List of recent detection dictionaries
        """
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
            
            with self._detection_lock:
                filtered_detections = [
                    detection for detection in self._recent_detections
                    if (detection.timestamp >= cutoff_time and 
                        (attack_type is None or 
                         detection.attack_indicators.get('attack_type') == attack_type.value))
                ]
            
            return [
                {
                    "detection_id": detection.detection_id,
                    "timestamp": detection.timestamp.isoformat(),
                    "threat_type": detection.threat_type,
                    "severity": detection.severity.value,
                    "confidence_score": detection.confidence_score,
                    "attack_indicators": detection.attack_indicators,
                    "affected_components": [comp.value for comp in detection.affected_components],
                    "response_actions": detection.response_actions,
                    "evidence_chain": detection.evidence_chain
                }
                for detection in filtered_detections
            ]
            
        except Exception as e:
            self.logger.error(f"Error getting recent detections: {e}")
            return []
    
    def get_attack_sequences(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get attack sequences
        
        Args:
            active_only: If True, return only active sequences
            
        Returns:
            List of attack sequence dictionaries
        """
        try:
            sequences = []
            
            with self._sequence_lock:
                if active_only:
                    source_sequences = self._active_sequences.values()
                else:
                    source_sequences = list(self._active_sequences.values()) + self._completed_sequences
            
            for sequence in source_sequences:
                sequences.append({
                    "sequence_id": sequence.sequence_id,
                    "attack_type": sequence.attack_type.value,
                    "event_count": len(sequence.events),
                    "start_time": sequence.start_time.isoformat() if sequence.start_time else None,
                    "end_time": sequence.end_time.isoformat() if sequence.end_time else None,
                    "source_ips": list(sequence.source_ips),
                    "target_components": [comp.value for comp in sequence.target_components],
                    "confidence_score": sequence.confidence_score
                })
            
            return sequences
            
        except Exception as e:
            self.logger.error(f"Error getting attack sequences: {e}")
            return []
    
    def is_healthy(self) -> bool:
        """Check if the attack pattern recognition engine is healthy"""
        try:
            return (self._active and 
                   self._detection_thread and 
                   self._detection_thread.is_alive() and
                   len(self._attack_patterns) > 0)
        except Exception as e:
            self.logger.error(f"Error checking health: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get attack pattern recognition statistics"""
        try:
            with self._detection_lock:
                detection_count = len(self._recent_detections)
            
            with self._sequence_lock:
                active_sequences = len(self._active_sequences)
                completed_sequences = len(self._completed_sequences)
            
            return {
                "active": self._active,
                "attack_patterns_loaded": len(self._attack_patterns),
                "patterns_by_type": {
                    attack_type.value: len(patterns)
                    for attack_type, patterns in self._patterns_by_type.items()
                },
                "recent_events": len(self._recent_events),
                "recent_detections": detection_count,
                "active_sequences": active_sequences,
                "completed_sequences": completed_sequences
            }
            
        except Exception as e:
            self.logger.error(f"Error getting statistics: {e}")
            return {}