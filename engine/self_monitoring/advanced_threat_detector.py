"""
Advanced Threat Detection Engine for UBA Self-Monitoring System

This module implements sophisticated attack detection algorithms including:
- Persistence mechanism detection
- Data exfiltration monitoring and blocking
- Evasion technique detection and countermeasures

Requirements: 5.1, 5.3, 5.4, 5.5
"""

import logging
import re
import json
import threading
import time
import hashlib
import statistics
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple, Pattern
from dataclasses import dataclass, field
from collections import defaultdict, deque, Counter
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


class AdvancedThreatType(Enum):
    """Advanced threat types for sophisticated attacks"""
    PERSISTENCE_MECHANISM = "persistence_mechanism"
    DATA_EXFILTRATION = "data_exfiltration"
    EVASION_TECHNIQUE = "evasion_technique"
    ADVANCED_PERSISTENT_THREAT = "advanced_persistent_threat"
    STEGANOGRAPHIC_ATTACK = "steganographic_attack"
    POLYMORPHIC_ATTACK = "polymorphic_attack"
    LIVING_OFF_THE_LAND = "living_off_the_land"


@dataclass
class PersistenceMechanism:
    """Persistence mechanism detection data"""
    mechanism_id: str
    mechanism_type: str
    detection_patterns: List[str]
    persistence_indicators: List[str]
    cleanup_difficulty: str  # easy, medium, hard, very_hard
    stealth_level: float  # 0.0 to 1.0
    impact_level: ThreatLevel


@dataclass
class ExfiltrationPattern:
    """Data exfiltration pattern definition"""
    pattern_id: str
    exfiltration_method: str
    detection_signatures: List[str]
    data_volume_threshold: int
    time_pattern_indicators: List[str]
    network_indicators: List[str]
    stealth_indicators: List[str]


@dataclass
class EvasionTechnique:
    """Evasion technique definition"""
    technique_id: str
    technique_name: str
    evasion_method: str
    detection_countermeasures: List[str]
    behavioral_indicators: List[str]
    timing_indicators: List[str]
    obfuscation_patterns: List[str]


@dataclass
class ThreatIntelligence:
    """Threat intelligence data"""
    intelligence_id: str
    threat_actor: str
    attack_patterns: List[str]
    indicators_of_compromise: List[str]
    tactics_techniques_procedures: Dict[str, List[str]]
    confidence_level: float
    last_updated: datetime


class AdvancedThreatDetector(DetectionInterface):
    """
    Advanced threat detection engine for sophisticated attacks
    
    This class implements detection algorithms for:
    - Persistence mechanisms (backdoors, triggers, scheduled tasks)
    - Data exfiltration (bulk extraction, covert channels, steganography)
    - Evasion techniques (obfuscation, timing attacks, polymorphism)
    """
    
    def __init__(self, config_manager: Optional[SelfMonitoringConfig] = None,
                 crypto_logger: Optional[CryptoLogger] = None):
        """
        Initialize advanced threat detector
        
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
        
        # Threat detection databases
        self._persistence_mechanisms: Dict[str, PersistenceMechanism] = {}
        self._exfiltration_patterns: Dict[str, ExfiltrationPattern] = {}
        self._evasion_techniques: Dict[str, EvasionTechnique] = {}
        self._threat_intelligence: Dict[str, ThreatIntelligence] = {}
        
        # Event tracking and analysis
        self._event_history: deque = deque(maxlen=50000)
        self._event_lock = threading.Lock()
        
        # Behavioral baselines
        self._user_baselines: Dict[str, Dict[str, Any]] = {}
        self._system_baselines: Dict[str, Dict[str, Any]] = {}
        self._baseline_lock = threading.Lock()
        
        # Detection results
        self._advanced_detections: List[ThreatDetection] = []
        self._detection_lock = threading.Lock()
        
        # Statistical analysis
        self._query_patterns: Dict[str, Counter] = defaultdict(Counter)
        self._timing_patterns: Dict[str, List[float]] = defaultdict(list)
        self._volume_patterns: Dict[str, List[int]] = defaultdict(list)
        
        # Load configuration and initialize detection mechanisms
        self._load_advanced_config()
        self._initialize_persistence_detection()
        self._initialize_exfiltration_detection()
        self._initialize_evasion_detection()
        self._initialize_threat_intelligence()
    
    def _load_advanced_config(self):
        """Load advanced threat detection configuration"""
        try:
            config = self.config_manager.load_config()
            advanced_config = config.get('advanced_threat_detection', {})
            
            # Detection thresholds
            self._thresholds = advanced_config.get('thresholds', {
                'persistence_confidence': 0.85,
                'exfiltration_volume_mb': 100,
                'exfiltration_rate_threshold': 0.6,  # Reduced for better detection sensitivity
                'evasion_confidence': 0.6,  # Balanced for detection vs false positives
                'behavioral_deviation_threshold': 2.5,  # Standard deviations
                'timing_anomaly_threshold': 3.0,
                'steganography_detection_threshold': 0.7
            })
            
            # Analysis windows
            self._analysis_windows = advanced_config.get('analysis_windows', {
                'persistence_analysis_hours': 24,
                'exfiltration_analysis_minutes': 60,
                'evasion_analysis_minutes': 30,
                'baseline_learning_days': 7
            })
            
            # Detection parameters
            self._detection_params = advanced_config.get('detection_params', {
                'min_persistence_indicators': 1,  # Reduced for better detection sensitivity
                'max_false_positive_rate': 0.05,
                'adaptive_threshold_enabled': True,
                'threat_intelligence_weight': 0.3
            })
            
            self.logger.info("Advanced threat detection configuration loaded")
            
        except Exception as e:
            self.logger.error(f"Error loading advanced threat detection configuration: {e}")
            # Use safe defaults
            self._thresholds = {
                'persistence_confidence': 0.85,
                'exfiltration_volume_mb': 100,
                'exfiltration_rate_threshold': 0.6,  # Reduced for better detection sensitivity
                'evasion_confidence': 0.5,  # Reduced for better detection sensitivity
                'behavioral_deviation_threshold': 2.5,
                'timing_anomaly_threshold': 3.0,
                'steganography_detection_threshold': 0.7
            }
            self._analysis_windows = {
                'persistence_analysis_hours': 24,
                'exfiltration_analysis_minutes': 60,
                'evasion_analysis_minutes': 30,
                'baseline_learning_days': 7
            }
            self._detection_params = {
                'min_persistence_indicators': 1,  # Reduced for better detection sensitivity
                'max_false_positive_rate': 0.05,
                'adaptive_threshold_enabled': True,
                'threat_intelligence_weight': 0.3
            }
    
    def _initialize_persistence_detection(self):
        """Initialize persistence mechanism detection patterns"""
        try:
            persistence_mechanisms = [
                PersistenceMechanism(
                    mechanism_id="persist_trigger_001",
                    mechanism_type="database_trigger",
                    detection_patterns=[
                        r"(?i)create\s+trigger\s+\w+.*after\s+insert",
                        r"(?i)create\s+trigger\s+\w+.*before\s+update",
                        r"(?i)create\s+trigger\s+\w+.*after\s+delete"
                    ],
                    persistence_indicators=[
                        "trigger_creation",
                        "hidden_trigger_logic",
                        "trigger_with_network_calls"
                    ],
                    cleanup_difficulty="hard",
                    stealth_level=0.8,
                    impact_level=ThreatLevel.HIGH
                ),
                PersistenceMechanism(
                    mechanism_id="persist_procedure_001",
                    mechanism_type="stored_procedure",
                    detection_patterns=[
                        r"(?i)create\s+procedure\s+\w+.*begin.*end",
                        r"(?i)create\s+function\s+\w+.*returns",
                        r"(?i)delimiter\s*\$\$"
                    ],
                    persistence_indicators=[
                        "procedure_with_admin_privileges",
                        "procedure_with_file_operations",
                        "procedure_with_network_access"
                    ],
                    cleanup_difficulty="medium",
                    stealth_level=0.6,
                    impact_level=ThreatLevel.HIGH
                ),
                PersistenceMechanism(
                    mechanism_id="persist_user_001",
                    mechanism_type="backdoor_user",
                    detection_patterns=[
                        r"(?i)create\s+user\s+",
                        r"(?i)insert\s+into\s+mysql\.user"
                    ],
                    persistence_indicators=[
                        "user_with_all_privileges",
                        "user_with_unusual_name",
                        "user_created_outside_hours"
                    ],
                    cleanup_difficulty="easy",
                    stealth_level=0.4,
                    impact_level=ThreatLevel.CRITICAL
                ),
                PersistenceMechanism(
                    mechanism_id="persist_event_001",
                    mechanism_type="scheduled_event",
                    detection_patterns=[
                        r"(?i)create\s+event\s+\w+.*on\s+schedule",
                        r"(?i)alter\s+event\s+\w+.*enable",
                        r"(?i)set\s+global\s+event_scheduler\s*=\s*on"
                    ],
                    persistence_indicators=[
                        "event_with_recurring_schedule",
                        "event_with_system_commands",
                        "event_scheduler_activation"
                    ],
                    cleanup_difficulty="medium",
                    stealth_level=0.7,
                    impact_level=ThreatLevel.HIGH
                ),
                PersistenceMechanism(
                    mechanism_id="persist_config_001",
                    mechanism_type="configuration_modification",
                    detection_patterns=[
                        r"(?i)set\s+global\s+general_log\s*=\s*off",
                        r"(?i)set\s+global\s+log_bin\s*=\s*off",
                        r"(?i)set\s+global\s+slow_query_log\s*=\s*off"
                    ],
                    persistence_indicators=[
                        "logging_disabled",
                        "audit_trail_disabled",
                        "security_features_disabled"
                    ],
                    cleanup_difficulty="easy",
                    stealth_level=0.9,
                    impact_level=ThreatLevel.CRITICAL
                )
            ]
            
            for mechanism in persistence_mechanisms:
                self._persistence_mechanisms[mechanism.mechanism_id] = mechanism
            
            self.logger.info(f"Initialized {len(persistence_mechanisms)} persistence detection mechanisms")
            
        except Exception as e:
            self.logger.error(f"Error initializing persistence detection: {e}")
    
    def _initialize_exfiltration_detection(self):
        """Initialize data exfiltration detection patterns"""
        try:
            exfiltration_patterns = [
                ExfiltrationPattern(
                    pattern_id="exfil_bulk_001",
                    exfiltration_method="bulk_extraction",
                    detection_signatures=[
                        r"(?i)select\s+.*\s+from\s+\w+\s+limit\s+\d{3,}",  # More flexible - 3+ digits
                        r"(?i)select\s+.*\s+from\s+\w+\s+where\s+1\s*=\s*1",
                        r"(?i)mysqldump\s+.*\s+--all-databases",
                        r"(?i)limit\s+\d{4,}"  # Additional pattern for large limits
                    ],
                    data_volume_threshold=1000000,  # 1MB
                    time_pattern_indicators=[
                        "rapid_sequential_queries",
                        "off_hours_bulk_access",
                        "automated_query_patterns"
                    ],
                    network_indicators=[
                        "external_ip_access",
                        "unusual_connection_patterns",
                        "high_bandwidth_usage"
                    ],
                    stealth_indicators=[
                        "query_obfuscation",
                        "timing_randomization",
                        "small_batch_extraction"
                    ]
                ),
                ExfiltrationPattern(
                    pattern_id="exfil_covert_001",
                    exfiltration_method="covert_channel",
                    detection_signatures=[
                        r"(?i)select\s+.*\s+into\s+outfile",
                        r"(?i)load_file\s*\(",
                        r"(?i)select\s+.*\s+union\s+select\s+load_file"
                    ],
                    data_volume_threshold=10000,  # 10KB
                    time_pattern_indicators=[
                        "periodic_small_extractions",
                        "steganographic_timing",
                        "dns_tunneling_patterns"
                    ],
                    network_indicators=[
                        "unusual_dns_queries",
                        "http_header_manipulation",
                        "icmp_data_patterns"
                    ],
                    stealth_indicators=[
                        "data_encoding_patterns",
                        "frequency_analysis_evasion",
                        "traffic_mimicry"
                    ]
                ),
                ExfiltrationPattern(
                    pattern_id="exfil_stego_001",
                    exfiltration_method="steganographic",
                    detection_signatures=[
                        r"(?i)select\s+hex\s*\(",
                        r"(?i)select\s+base64\s*\(",
                        r"(?i)select\s+compress\s*\("
                    ],
                    data_volume_threshold=1000,  # 1KB
                    time_pattern_indicators=[
                        "irregular_access_patterns",
                        "data_transformation_sequences",
                        "encoding_operation_chains"
                    ],
                    network_indicators=[
                        "image_file_requests",
                        "document_metadata_access",
                        "multimedia_file_patterns"
                    ],
                    stealth_indicators=[
                        "least_significant_bit_patterns",
                        "frequency_domain_hiding",
                        "metadata_embedding"
                    ]
                )
            ]
            
            for pattern in exfiltration_patterns:
                self._exfiltration_patterns[pattern.pattern_id] = pattern
            
            self.logger.info(f"Initialized {len(exfiltration_patterns)} exfiltration detection patterns")
            
        except Exception as e:
            self.logger.error(f"Error initializing exfiltration detection: {e}")
    
    def _initialize_evasion_detection(self):
        """Initialize evasion technique detection"""
        try:
            evasion_techniques = [
                EvasionTechnique(
                    technique_id="evasion_obfusc_001",
                    technique_name="query_obfuscation",
                    evasion_method="sql_obfuscation",
                    detection_countermeasures=[
                        "normalize_whitespace",
                        "decode_hex_strings",
                        "expand_comments",
                        "resolve_concatenations"
                    ],
                    behavioral_indicators=[
                        "excessive_whitespace",
                        "unusual_comment_patterns",
                        "hex_encoded_strings",
                        "string_concatenation_chains"
                    ],
                    timing_indicators=[
                        "query_execution_delays",
                        "artificial_timing_patterns",
                        "sleep_injection_attempts"
                    ],
                    obfuscation_patterns=[
                        r"(?i)/\*.*?\*/",  # SQL comments
                        r"(?i)0x[0-9a-f]+",  # Hex strings
                        r"(?i)char\s*\(\s*\d+(?:\s*,\s*\d+)*\s*\)",  # Character encoding
                        r"(?i)concat\s*\(",  # String concatenation
                        r"\s{5,}",  # Excessive whitespace
                    ]
                ),
                EvasionTechnique(
                    technique_id="evasion_timing_001",
                    technique_name="timing_evasion",
                    evasion_method="timing_manipulation",
                    detection_countermeasures=[
                        "statistical_timing_analysis",
                        "query_execution_profiling",
                        "behavioral_timing_baselines",
                        "anomaly_detection_algorithms"
                    ],
                    behavioral_indicators=[
                        "artificial_delays",
                        "randomized_timing_patterns",
                        "sleep_function_usage",
                        "benchmark_function_abuse"
                    ],
                    timing_indicators=[
                        "consistent_delay_patterns",
                        "timing_correlation_with_queries",
                        "execution_time_anomalies"
                    ],
                    obfuscation_patterns=[
                        r"(?i)sleep\s*\(\s*\d+\s*\)",
                        r"(?i)benchmark\s*\(",
                        r"(?i)waitfor\s+delay",
                        r"(?i)pg_sleep\s*\("
                    ]
                ),
                EvasionTechnique(
                    technique_id="evasion_polymorph_001",
                    technique_name="polymorphic_queries",
                    evasion_method="query_polymorphism",
                    detection_countermeasures=[
                        "query_normalization",
                        "semantic_analysis",
                        "pattern_abstraction",
                        "behavioral_clustering"
                    ],
                    behavioral_indicators=[
                        "functionally_equivalent_queries",
                        "syntax_variation_patterns",
                        "semantic_preservation",
                        "automated_generation_signatures"
                    ],
                    timing_indicators=[
                        "rapid_query_variations",
                        "generation_timing_patterns",
                        "template_based_timing"
                    ],
                    obfuscation_patterns=[
                        r"(?i)union\s+all\s+select.*union\s+all\s+select",  # Multiple union chains
                        r"(?i)(and|or)\s+\d+\s*[=<>]\s*\d+.*\1\s+\d+\s*[=<>]\s*\d+",  # Repeated boolean conditions
                        r"(?i)'[^']*'\s*[=<>]\s*'[^']*'.*'[^']*'\s*[=<>]\s*'[^']*'"  # Multiple string comparisons
                    ]
                )
            ]
            
            for technique in evasion_techniques:
                self._evasion_techniques[technique.technique_id] = technique
            
            self.logger.info(f"Initialized {len(evasion_techniques)} evasion detection techniques")
            
        except Exception as e:
            self.logger.error(f"Error initializing evasion detection: {e}")
    
    def _initialize_threat_intelligence(self):
        """Initialize threat intelligence database"""
        try:
            # Sample threat intelligence data
            threat_intelligence = [
                ThreatIntelligence(
                    intelligence_id="apt_001",
                    threat_actor="Advanced Persistent Threat Group",
                    attack_patterns=[
                        "multi_stage_sql_injection",
                        "privilege_escalation_chains",
                        "persistent_backdoor_installation"
                    ],
                    indicators_of_compromise=[
                        "specific_user_agent_strings",
                        "characteristic_query_patterns",
                        "timing_signatures"
                    ],
                    tactics_techniques_procedures={
                        "initial_access": ["sql_injection", "credential_stuffing"],
                        "persistence": ["backdoor_users", "triggers", "procedures"],
                        "privilege_escalation": ["mysql_user_manipulation", "grant_abuse"],
                        "defense_evasion": ["query_obfuscation", "timing_manipulation"],
                        "exfiltration": ["bulk_extraction", "covert_channels"]
                    },
                    confidence_level=0.85,
                    last_updated=datetime.now(timezone.utc)
                )
            ]
            
            for intel in threat_intelligence:
                self._threat_intelligence[intel.intelligence_id] = intel
            
            self.logger.info(f"Initialized {len(threat_intelligence)} threat intelligence entries")
            
        except Exception as e:
            self.logger.error(f"Error initializing threat intelligence: {e}")
    
    def start_detection(self) -> bool:
        """Start advanced threat detection"""
        try:
            if self._active:
                self.logger.warning("Advanced threat detection is already active")
                return True
            
            # Start detection thread
            self._active = True
            self._stop_event.clear()
            self._detection_thread = threading.Thread(target=self._detection_loop, daemon=True)
            self._detection_thread.start()
            
            self.logger.info("Advanced threat detection started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting advanced threat detection: {e}")
            self._active = False
            return False
    
    def stop_detection(self) -> bool:
        """Stop advanced threat detection"""
        try:
            if not self._active:
                self.logger.warning("Advanced threat detection is not active")
                return True
            
            # Signal stop and wait for thread
            self._stop_event.set()
            self._active = False
            
            if self._detection_thread and self._detection_thread.is_alive():
                self._detection_thread.join(timeout=10)
            
            self.logger.info("Advanced threat detection stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping advanced threat detection: {e}")
            return False
    
    def _detection_loop(self):
        """Main advanced threat detection loop"""
        while not self._stop_event.is_set():
            try:
                # Detect persistence mechanisms
                self._detect_persistence_mechanisms()
                
                # Detect data exfiltration
                self._detect_data_exfiltration()
                
                # Detect evasion techniques
                self._detect_evasion_techniques()
                
                # Update behavioral baselines
                self._update_behavioral_baselines()
                
                # Clean up old data
                self._cleanup_old_data()
                
            except Exception as e:
                self.logger.error(f"Error in advanced threat detection loop: {e}")
            
            # Wait before next iteration
            self._stop_event.wait(60)  # Run every minute
    
    def add_event(self, event: InfrastructureEvent):
        """Add infrastructure event for advanced threat analysis"""
        try:
            with self._event_lock:
                self._event_history.append(event)
                
                # Update statistical patterns
                self._update_statistical_patterns(event)
            
        except Exception as e:
            self.logger.error(f"Error adding event for advanced threat analysis: {e}")
    
    def _update_statistical_patterns(self, event: InfrastructureEvent):
        """Update statistical patterns for behavioral analysis"""
        try:
            user_key = f"{event.user_account}_{event.source_ip}"
            
            # Update query patterns
            if event.action_details:
                query = event.action_details.get('query', '')
                if query:
                    # Normalize query for pattern analysis
                    normalized_query = self._normalize_query(query)
                    self._query_patterns[user_key][normalized_query] += 1
            
            # Update timing patterns
            current_time = event.timestamp.timestamp()
            self._timing_patterns[user_key].append(current_time)
            
            # Keep only recent timing data (last 24 hours)
            cutoff_time = current_time - 86400
            self._timing_patterns[user_key] = [
                t for t in self._timing_patterns[user_key] if t >= cutoff_time
            ]
            
            # Update volume patterns (query length as proxy for data volume)
            if event.action_details:
                query = event.action_details.get('query', '')
                self._volume_patterns[user_key].append(len(query))
                
                # Keep only recent volume data
                if len(self._volume_patterns[user_key]) > 1000:
                    self._volume_patterns[user_key] = self._volume_patterns[user_key][-500:]
            
        except Exception as e:
            self.logger.error(f"Error updating statistical patterns: {e}")
    
    def _normalize_query(self, query: str) -> str:
        """Normalize query for pattern analysis"""
        try:
            # Convert to lowercase
            normalized = query.lower()
            
            # Remove extra whitespace
            normalized = re.sub(r'\s+', ' ', normalized)
            
            # Replace string literals with placeholder
            normalized = re.sub(r"'[^']*'", "'STRING'", normalized)
            
            # Replace numeric literals with placeholder
            normalized = re.sub(r'\b\d+\b', 'NUMBER', normalized)
            
            # Remove comments
            normalized = re.sub(r'/\*.*?\*/', '', normalized)
            normalized = re.sub(r'--.*$', '', normalized, flags=re.MULTILINE)
            
            return normalized.strip()
            
        except Exception as e:
            self.logger.error(f"Error normalizing query: {e}")
            return query
    
    def _detect_persistence_mechanisms(self):
        """Detect persistence mechanisms in recent events"""
        try:
            # Get events from the last analysis window
            current_time = datetime.now(timezone.utc)
            analysis_window = current_time - timedelta(hours=self._analysis_windows['persistence_analysis_hours'])
            
            with self._event_lock:
                recent_events = [
                    event for event in self._event_history
                    if event.timestamp >= analysis_window
                ]
            
            # Analyze events for persistence indicators
            persistence_indicators = []
            
            for event in recent_events:
                indicators = self._analyze_event_for_persistence(event)
                persistence_indicators.extend(indicators)
            
            # Group indicators by mechanism type and source
            grouped_indicators = defaultdict(list)
            for indicator in persistence_indicators:
                key = (indicator['mechanism_type'], indicator['source_ip'], indicator['user_account'])
                grouped_indicators[key].append(indicator)
            
            # Create threat detections for significant persistence attempts
            for (mechanism_type, source_ip, user_account), indicators in grouped_indicators.items():
                if len(indicators) >= self._detection_params['min_persistence_indicators']:
                    threat_detection = self._create_persistence_threat_detection(
                        mechanism_type, source_ip, user_account, indicators
                    )
                    
                    with self._detection_lock:
                        self._advanced_detections.append(threat_detection)
                    
                    # Log critical persistence detections
                    if threat_detection.severity == ThreatLevel.CRITICAL:
                        self.crypto_logger.log_monitoring_event(
                            f"persistence_mechanism_detected_{mechanism_type}",
                            "uba_infrastructure",
                            {
                                "mechanism_type": mechanism_type,
                                "source_ip": source_ip,
                                "user_account": user_account,
                                "indicators_count": len(indicators),
                                "confidence": threat_detection.confidence_score
                            },
                            threat_detection.confidence_score
                        )
            
        except Exception as e:
            self.logger.error(f"Error detecting persistence mechanisms: {e}")
    
    def _analyze_event_for_persistence(self, event: InfrastructureEvent) -> List[Dict[str, Any]]:
        """Analyze event for persistence mechanism indicators"""
        indicators = []
        
        try:
            # Extract query content
            query = ""
            if event.action_details:
                query = event.action_details.get('query', '')
                query += " " + event.action_details.get('query_snippet', '')
                query += " " + event.action_details.get('argument', '')
            
            if not query or len(query) < 10:
                return indicators
            
            # Check against persistence mechanism patterns
            for mechanism_id, mechanism in self._persistence_mechanisms.items():
                for pattern in mechanism.detection_patterns:
                    try:
                        if re.search(pattern, query, re.IGNORECASE):
                            confidence = self._calculate_persistence_confidence(mechanism, event, query)
                            
                            if confidence >= self._thresholds['persistence_confidence']:
                                indicators.append({
                                    'mechanism_id': mechanism_id,
                                    'mechanism_type': mechanism.mechanism_type,
                                    'pattern_matched': pattern,
                                    'confidence': confidence,
                                    'event_id': event.event_id,
                                    'timestamp': event.timestamp,
                                    'source_ip': event.source_ip,
                                    'user_account': event.user_account,
                                    'query_content': query[:500],  # Truncate for storage
                                    'stealth_level': mechanism.stealth_level,
                                    'cleanup_difficulty': mechanism.cleanup_difficulty
                                })
                    except re.error as e:
                        self.logger.error(f"Invalid persistence pattern {pattern}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error analyzing event for persistence: {e}")
        
        return indicators
    
    def _calculate_persistence_confidence(self, mechanism: PersistenceMechanism, 
                                        event: InfrastructureEvent, query: str) -> float:
        """Calculate confidence score for persistence mechanism detection"""
        try:
            base_confidence = 0.7
            
            # Adjust based on mechanism stealth level (higher stealth = higher confidence when detected)
            stealth_adjustment = mechanism.stealth_level * 0.2
            
            # Adjust based on user account (uba_user is critical)
            user_adjustment = 0.0
            if event.user_account == 'uba_user':
                user_adjustment = 0.3
            elif event.user_account in ['root', 'admin', 'administrator']:
                user_adjustment = 0.2
            
            # Adjust based on timing (off-hours activity)
            timing_adjustment = 0.0
            event_hour = event.timestamp.hour
            if event_hour < 6 or event_hour > 22:
                timing_adjustment = 0.15
            
            # Adjust based on source IP (external sources more suspicious)
            source_adjustment = 0.0
            if event.source_ip not in ['localhost', '127.0.0.1', '::1']:
                source_adjustment = 0.1
            
            # Adjust based on query complexity
            complexity_adjustment = 0.0
            if len(query) > 200:
                complexity_adjustment = 0.05
            
            # Check for additional persistence indicators
            indicator_adjustment = 0.0
            for indicator in mechanism.persistence_indicators:
                if indicator.lower() in query.lower():
                    indicator_adjustment += 0.1
            
            final_confidence = min(
                base_confidence + stealth_adjustment + user_adjustment + 
                timing_adjustment + source_adjustment + complexity_adjustment + indicator_adjustment,
                1.0
            )
            
            return final_confidence
            
        except Exception as e:
            self.logger.error(f"Error calculating persistence confidence: {e}")
            return 0.5
    
    def _create_persistence_threat_detection(self, mechanism_type: str, source_ip: str,
                                           user_account: str, indicators: List[Dict[str, Any]]) -> ThreatDetection:
        """Create threat detection for persistence mechanism"""
        try:
            # Calculate overall confidence
            confidences = [indicator['confidence'] for indicator in indicators]
            avg_confidence = statistics.mean(confidences)
            
            # Determine severity
            severity = ThreatLevel.HIGH
            if avg_confidence >= 0.9 or mechanism_type in ['backdoor_user', 'configuration_modification']:
                severity = ThreatLevel.CRITICAL
            
            # Collect evidence
            evidence_chain = [indicator['event_id'] for indicator in indicators]
            
            # Create attack indicators
            attack_indicators = {
                "threat_type": AdvancedThreatType.PERSISTENCE_MECHANISM.value,
                "mechanism_type": mechanism_type,
                "source_ip": source_ip,
                "user_account": user_account,
                "indicators_count": len(indicators),
                "patterns_matched": [indicator['pattern_matched'] for indicator in indicators],
                "stealth_levels": [indicator['stealth_level'] for indicator in indicators],
                "cleanup_difficulties": [indicator['cleanup_difficulty'] for indicator in indicators],
                "time_span_hours": self._calculate_indicator_time_span(indicators)
            }
            
            # Determine affected components
            affected_components = [ComponentType.DATABASE]
            if mechanism_type == 'backdoor_user':
                affected_components.append(ComponentType.USER_ACCOUNT)
            
            # Determine response actions
            response_actions = [
                "investigate_persistence_mechanism",
                "scan_for_backdoors",
                "review_system_changes",
                "audit_user_accounts",
                "check_triggers_procedures"
            ]
            
            if severity == ThreatLevel.CRITICAL:
                response_actions.extend([
                    "immediate_incident_response",
                    "isolate_affected_systems",
                    "emergency_containment"
                ])
            
            threat_detection = ThreatDetection(
                detection_id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                threat_type=f"persistence_{mechanism_type}",
                severity=severity,
                affected_components=affected_components,
                attack_indicators=attack_indicators,
                confidence_score=avg_confidence,
                response_actions=response_actions,
                evidence_chain=evidence_chain
            )
            
            return threat_detection
            
        except Exception as e:
            self.logger.error(f"Error creating persistence threat detection: {e}")
            # Return basic threat detection
            return ThreatDetection(
                detection_id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                threat_type=f"persistence_{mechanism_type}",
                severity=ThreatLevel.HIGH,
                affected_components=[ComponentType.DATABASE],
                attack_indicators={"mechanism_type": mechanism_type},
                confidence_score=0.7,
                response_actions=["investigate_persistence_mechanism"],
                evidence_chain=[]
            )
    
    def _calculate_indicator_time_span(self, indicators: List[Dict[str, Any]]) -> float:
        """Calculate time span of indicators in hours"""
        try:
            if len(indicators) <= 1:
                return 0.0
            
            timestamps = [indicator['timestamp'] for indicator in indicators]
            min_time = min(timestamps)
            max_time = max(timestamps)
            
            return (max_time - min_time).total_seconds() / 3600.0
            
        except Exception as e:
            self.logger.error(f"Error calculating indicator time span: {e}")
            return 0.0
    
    def _detect_data_exfiltration(self):
        """Detect data exfiltration attempts"""
        try:
            # Get events from the last analysis window
            current_time = datetime.now(timezone.utc)
            analysis_window = current_time - timedelta(minutes=self._analysis_windows['exfiltration_analysis_minutes'])
            
            with self._event_lock:
                recent_events = [
                    event for event in self._event_history
                    if event.timestamp >= analysis_window
                ]
            
            # Analyze events for exfiltration patterns
            exfiltration_indicators = []
            
            for event in recent_events:
                indicators = self._analyze_event_for_exfiltration(event)
                exfiltration_indicators.extend(indicators)
            
            # Analyze volume and timing patterns
            volume_anomalies = self._detect_volume_anomalies(recent_events)
            timing_anomalies = self._detect_timing_anomalies(recent_events)
            
            # Combine pattern-based and statistical indicators
            all_indicators = exfiltration_indicators + volume_anomalies + timing_anomalies
            
            # Group indicators by source and method
            grouped_indicators = defaultdict(list)
            for indicator in all_indicators:
                key = (indicator['source_ip'], indicator['user_account'], indicator.get('method', 'unknown'))
                grouped_indicators[key].append(indicator)
            
            # Create threat detections for significant exfiltration attempts
            for (source_ip, user_account, method), indicators in grouped_indicators.items():
                if len(indicators) >= 1:  # At least 1 indicator for exfiltration
                    threat_detection = self._create_exfiltration_threat_detection(
                        source_ip, user_account, method, indicators
                    )
                    
                    with self._detection_lock:
                        self._advanced_detections.append(threat_detection)
                    
                    # Log high-severity exfiltration detections
                    if threat_detection.severity in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                        self.crypto_logger.log_monitoring_event(
                            f"data_exfiltration_detected_{method}",
                            "uba_infrastructure",
                            {
                                "method": method,
                                "source_ip": source_ip,
                                "user_account": user_account,
                                "indicators_count": len(indicators),
                                "confidence": threat_detection.confidence_score
                            },
                            threat_detection.confidence_score
                        )
            
        except Exception as e:
            self.logger.error(f"Error detecting data exfiltration: {e}")
    
    def _analyze_event_for_exfiltration(self, event: InfrastructureEvent) -> List[Dict[str, Any]]:
        """Analyze event for data exfiltration indicators"""
        indicators = []
        
        try:
            # Extract query content
            query = ""
            if event.action_details:
                query = event.action_details.get('query', '')
                query += " " + event.action_details.get('query_snippet', '')
                query += " " + event.action_details.get('argument', '')
            
            if not query or len(query) < 10:
                return indicators
            
            # Check against exfiltration patterns
            for pattern_id, pattern in self._exfiltration_patterns.items():
                for signature in pattern.detection_signatures:
                    try:
                        if re.search(signature, query, re.IGNORECASE):
                            confidence = self._calculate_exfiltration_confidence(pattern, event, query)
                            
                            if confidence >= self._thresholds['exfiltration_rate_threshold']:
                                indicators.append({
                                    'pattern_id': pattern_id,
                                    'method': pattern.exfiltration_method,
                                    'signature_matched': signature,
                                    'confidence': confidence,
                                    'event_id': event.event_id,
                                    'timestamp': event.timestamp,
                                    'source_ip': event.source_ip,
                                    'user_account': event.user_account,
                                    'query_content': query[:500],
                                    'estimated_volume': len(query),
                                    'detection_type': 'pattern_based'
                                })
                    except re.error as e:
                        self.logger.error(f"Invalid exfiltration pattern {signature}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error analyzing event for exfiltration: {e}")
        
        return indicators
    
    def _calculate_exfiltration_confidence(self, pattern: ExfiltrationPattern,
                                         event: InfrastructureEvent, query: str) -> float:
        """Calculate confidence score for exfiltration detection"""
        try:
            base_confidence = 0.6
            
            # Adjust based on query size (larger queries more suspicious for bulk extraction)
            size_adjustment = 0.0
            if len(query) > 1000:
                size_adjustment = 0.2
            elif len(query) > 500:
                size_adjustment = 0.1
            
            # Adjust based on timing
            timing_adjustment = 0.0
            event_hour = event.timestamp.hour
            if event_hour < 6 or event_hour > 22:
                timing_adjustment = 0.15
            
            # Adjust based on user account
            user_adjustment = 0.0
            if event.user_account == 'uba_user':
                user_adjustment = 0.25
            
            # Adjust based on source IP
            source_adjustment = 0.0
            if event.source_ip not in ['localhost', '127.0.0.1', '::1']:
                source_adjustment = 0.15
            
            # Check for stealth indicators
            stealth_adjustment = 0.0
            for stealth_indicator in pattern.stealth_indicators:
                if stealth_indicator.lower() in query.lower():
                    stealth_adjustment += 0.1
            
            final_confidence = min(
                base_confidence + size_adjustment + timing_adjustment + 
                user_adjustment + source_adjustment + stealth_adjustment,
                1.0
            )
            
            return final_confidence
            
        except Exception as e:
            self.logger.error(f"Error calculating exfiltration confidence: {e}")
            return 0.5
    
    def _detect_volume_anomalies(self, events: List[InfrastructureEvent]) -> List[Dict[str, Any]]:
        """Detect volume-based exfiltration anomalies"""
        anomalies = []
        
        try:
            # Group events by user and source
            user_volumes = defaultdict(list)
            
            for event in events:
                if event.action_details:
                    query = event.action_details.get('query', '')
                    if query:
                        user_key = f"{event.user_account}_{event.source_ip}"
                        user_volumes[user_key].append({
                            'event': event,
                            'volume': len(query),
                            'timestamp': event.timestamp
                        })
            
            # Analyze volume patterns for each user
            for user_key, volumes in user_volumes.items():
                if len(volumes) < 5:  # Need sufficient data points
                    continue
                
                # Calculate volume statistics
                volume_values = [v['volume'] for v in volumes]
                mean_volume = statistics.mean(volume_values)
                
                if len(volume_values) > 1:
                    stdev_volume = statistics.stdev(volume_values)
                    
                    # Detect anomalous volumes
                    for volume_data in volumes:
                        if volume_data['volume'] > mean_volume + (self._thresholds['behavioral_deviation_threshold'] * stdev_volume):
                            user_account, source_ip = user_key.split('_', 1)
                            
                            anomalies.append({
                                'detection_type': 'volume_anomaly',
                                'method': 'statistical_analysis',
                                'event_id': volume_data['event'].event_id,
                                'timestamp': volume_data['timestamp'],
                                'source_ip': source_ip,
                                'user_account': user_account,
                                'volume': volume_data['volume'],
                                'mean_volume': mean_volume,
                                'deviation_factor': (volume_data['volume'] - mean_volume) / stdev_volume,
                                'confidence': min(0.8, 0.5 + ((volume_data['volume'] - mean_volume) / stdev_volume) * 0.1)
                            })
            
        except Exception as e:
            self.logger.error(f"Error detecting volume anomalies: {e}")
        
        return anomalies
    
    def _detect_timing_anomalies(self, events: List[InfrastructureEvent]) -> List[Dict[str, Any]]:
        """Detect timing-based exfiltration anomalies"""
        anomalies = []
        
        try:
            # Group events by user and source
            user_timings = defaultdict(list)
            
            for event in events:
                user_key = f"{event.user_account}_{event.source_ip}"
                user_timings[user_key].append(event.timestamp.timestamp())
            
            # Analyze timing patterns for each user
            for user_key, timestamps in user_timings.items():
                if len(timestamps) < 10:  # Need sufficient data points
                    continue
                
                timestamps.sort()
                
                # Calculate inter-arrival times
                inter_arrivals = []
                for i in range(1, len(timestamps)):
                    inter_arrivals.append(timestamps[i] - timestamps[i-1])
                
                if len(inter_arrivals) > 1:
                    mean_interval = statistics.mean(inter_arrivals)
                    stdev_interval = statistics.stdev(inter_arrivals)
                    
                    # Detect suspiciously regular patterns (potential automation)
                    regular_intervals = sum(1 for interval in inter_arrivals 
                                          if abs(interval - mean_interval) < stdev_interval * 0.1)
                    
                    regularity_ratio = regular_intervals / len(inter_arrivals)
                    
                    if regularity_ratio > 0.8:  # Very regular pattern
                        user_account, source_ip = user_key.split('_', 1)
                        
                        anomalies.append({
                            'detection_type': 'timing_anomaly',
                            'method': 'regularity_analysis',
                            'source_ip': source_ip,
                            'user_account': user_account,
                            'regularity_ratio': regularity_ratio,
                            'mean_interval': mean_interval,
                            'event_count': len(timestamps),
                            'confidence': min(0.9, 0.5 + regularity_ratio * 0.4),
                            'timestamp': datetime.fromtimestamp(timestamps[-1], timezone.utc)
                        })
            
        except Exception as e:
            self.logger.error(f"Error detecting timing anomalies: {e}")
        
        return anomalies
    
    def _create_exfiltration_threat_detection(self, source_ip: str, user_account: str,
                                            method: str, indicators: List[Dict[str, Any]]) -> ThreatDetection:
        """Create threat detection for data exfiltration"""
        try:
            # Calculate overall confidence
            confidences = [indicator['confidence'] for indicator in indicators]
            avg_confidence = statistics.mean(confidences)
            
            # Determine severity based on method and confidence
            severity = ThreatLevel.MEDIUM
            if method in ['bulk_extraction', 'covert_channel'] or avg_confidence >= 0.8:
                severity = ThreatLevel.HIGH
            if user_account == 'uba_user' or avg_confidence >= 0.9:
                severity = ThreatLevel.CRITICAL
            
            # Collect evidence
            evidence_chain = [indicator.get('event_id', '') for indicator in indicators if indicator.get('event_id')]
            
            # Create attack indicators
            attack_indicators = {
                "threat_type": AdvancedThreatType.DATA_EXFILTRATION.value,
                "exfiltration_method": method,
                "source_ip": source_ip,
                "user_account": user_account,
                "indicators_count": len(indicators),
                "detection_types": list(set(indicator.get('detection_type', 'unknown') for indicator in indicators)),
                "estimated_total_volume": sum(indicator.get('volume', 0) for indicator in indicators),
                "time_span_minutes": self._calculate_exfiltration_time_span(indicators)
            }
            
            # Add method-specific indicators
            if method == 'bulk_extraction':
                attack_indicators['bulk_patterns'] = [
                    indicator.get('signature_matched', '') for indicator in indicators 
                    if indicator.get('signature_matched')
                ]
            elif method == 'statistical_analysis':
                attack_indicators['volume_anomalies'] = [
                    indicator.get('deviation_factor', 0) for indicator in indicators 
                    if indicator.get('deviation_factor')
                ]
            
            # Determine affected components
            affected_components = [ComponentType.DATABASE, ComponentType.AUDIT_LOG]
            
            # Determine response actions
            response_actions = [
                "investigate_data_access_patterns",
                "monitor_data_volume",
                "review_query_logs",
                "check_network_traffic"
            ]
            
            if severity in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                response_actions.extend([
                    "block_suspicious_queries",
                    "limit_data_access",
                    "escalate_to_security_team"
                ])
            
            if severity == ThreatLevel.CRITICAL:
                response_actions.extend([
                    "immediate_incident_response",
                    "emergency_data_protection",
                    "isolate_affected_systems"
                ])
            
            threat_detection = ThreatDetection(
                detection_id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                threat_type=f"exfiltration_{method}",
                severity=severity,
                affected_components=affected_components,
                attack_indicators=attack_indicators,
                confidence_score=avg_confidence,
                response_actions=response_actions,
                evidence_chain=evidence_chain
            )
            
            return threat_detection
            
        except Exception as e:
            self.logger.error(f"Error creating exfiltration threat detection: {e}")
            # Return basic threat detection
            return ThreatDetection(
                detection_id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                threat_type=f"exfiltration_{method}",
                severity=ThreatLevel.HIGH,
                affected_components=[ComponentType.DATABASE],
                attack_indicators={"method": method},
                confidence_score=0.7,
                response_actions=["investigate_data_access_patterns"],
                evidence_chain=[]
            )
    
    def _calculate_exfiltration_time_span(self, indicators: List[Dict[str, Any]]) -> float:
        """Calculate time span of exfiltration indicators in minutes"""
        try:
            timestamps = []
            for indicator in indicators:
                if 'timestamp' in indicator:
                    if isinstance(indicator['timestamp'], datetime):
                        timestamps.append(indicator['timestamp'])
                    else:
                        # Handle other timestamp formats if needed
                        continue
            
            if len(timestamps) <= 1:
                return 0.0
            
            min_time = min(timestamps)
            max_time = max(timestamps)
            
            return (max_time - min_time).total_seconds() / 60.0
            
        except Exception as e:
            self.logger.error(f"Error calculating exfiltration time span: {e}")
            return 0.0
    
    def _detect_evasion_techniques(self):
        """Detect evasion techniques in recent events"""
        try:
            # Get events from the last analysis window
            current_time = datetime.now(timezone.utc)
            analysis_window = current_time - timedelta(minutes=self._analysis_windows['evasion_analysis_minutes'])
            
            with self._event_lock:
                recent_events = [
                    event for event in self._event_history
                    if event.timestamp >= analysis_window
                ]
            
            # Analyze events for evasion techniques
            evasion_indicators = []
            
            for event in recent_events:
                indicators = self._analyze_event_for_evasion(event)
                evasion_indicators.extend(indicators)
            
            # Group indicators by technique and source
            grouped_indicators = defaultdict(list)
            for indicator in evasion_indicators:
                key = (indicator['technique_name'], indicator['source_ip'], indicator['user_account'])
                grouped_indicators[key].append(indicator)
            
            # Create threat detections for significant evasion attempts
            for (technique_name, source_ip, user_account), indicators in grouped_indicators.items():
                if len(indicators) >= 1:  # At least 1 indicator for evasion
                    threat_detection = self._create_evasion_threat_detection(
                        technique_name, source_ip, user_account, indicators
                    )
                    
                    with self._detection_lock:
                        self._advanced_detections.append(threat_detection)
                    
                    # Log evasion technique detections
                    if threat_detection.severity in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                        self.crypto_logger.log_monitoring_event(
                            f"evasion_technique_detected_{technique_name}",
                            "uba_infrastructure",
                            {
                                "technique": technique_name,
                                "source_ip": source_ip,
                                "user_account": user_account,
                                "indicators_count": len(indicators),
                                "confidence": threat_detection.confidence_score
                            },
                            threat_detection.confidence_score
                        )
            
        except Exception as e:
            self.logger.error(f"Error detecting evasion techniques: {e}")
    
    def _analyze_event_for_evasion(self, event: InfrastructureEvent) -> List[Dict[str, Any]]:
        """Analyze event for evasion technique indicators"""
        indicators = []
        
        try:
            # Extract query content
            query = ""
            if event.action_details:
                query = event.action_details.get('query', '')
                query += " " + event.action_details.get('query_snippet', '')
                query += " " + event.action_details.get('argument', '')
            
            if not query or len(query) < 10:
                return indicators
            
            # Check against evasion technique patterns
            for technique_id, technique in self._evasion_techniques.items():
                # Check obfuscation patterns
                obfuscation_matches = 0
                matched_patterns = []
                
                for pattern in technique.obfuscation_patterns:
                    try:
                        matches = re.findall(pattern, query, re.IGNORECASE)
                        if matches:
                            obfuscation_matches += len(matches)
                            matched_patterns.append(pattern)
                    except re.error as e:
                        self.logger.error(f"Invalid evasion pattern {pattern}: {e}")
                
                # Check behavioral indicators
                behavioral_matches = 0
                for indicator in technique.behavioral_indicators:
                    if self._check_behavioral_indicator(indicator, query, event):
                        behavioral_matches += 1
                
                # Calculate confidence based on matches
                if obfuscation_matches > 0 or behavioral_matches > 0:
                    confidence = self._calculate_evasion_confidence(
                        technique, event, query, obfuscation_matches, behavioral_matches
                    )
                    
                    if confidence >= self._thresholds['evasion_confidence']:
                        indicators.append({
                            'technique_id': technique_id,
                            'technique_name': technique.technique_name,
                            'evasion_method': technique.evasion_method,
                            'confidence': confidence,
                            'event_id': event.event_id,
                            'timestamp': event.timestamp,
                            'source_ip': event.source_ip,
                            'user_account': event.user_account,
                            'query_content': query[:500],
                            'obfuscation_matches': obfuscation_matches,
                            'behavioral_matches': behavioral_matches,
                            'matched_patterns': matched_patterns,
                            'detection_type': 'evasion_technique'
                        })
            
        except Exception as e:
            self.logger.error(f"Error analyzing event for evasion: {e}")
        
        return indicators
    
    def _check_behavioral_indicator(self, indicator: str, query: str, event: InfrastructureEvent) -> bool:
        """Check if a behavioral indicator is present"""
        try:
            indicator_lower = indicator.lower()
            query_lower = query.lower()
            
            if indicator_lower == "excessive_whitespace":
                # Check for unusual amounts of whitespace
                whitespace_ratio = len(re.findall(r'\s', query)) / len(query) if query else 0
                return whitespace_ratio > 0.3
            
            elif indicator_lower == "unusual_comment_patterns":
                # Check for SQL comments
                return bool(re.search(r'/\*.*?\*/', query, re.IGNORECASE))
            
            elif indicator_lower == "hex_encoded_strings":
                # Check for hex-encoded strings
                return bool(re.search(r'0x[0-9a-f]+', query, re.IGNORECASE))
            
            elif indicator_lower == "string_concatenation_chains":
                # Check for multiple concatenations
                concat_count = len(re.findall(r'(?i)concat\s*\(', query))
                return concat_count > 2
            
            elif indicator_lower == "artificial_delays":
                # Check for sleep/delay functions
                return bool(re.search(r'(?i)(sleep|waitfor|benchmark)\s*\(', query))
            
            elif indicator_lower == "randomized_timing_patterns":
                # This would require historical analysis - simplified check
                return "rand" in query_lower or "random" in query_lower
            
            elif indicator_lower == "functionally_equivalent_queries":
                # Check for query variations (simplified)
                return self._detect_query_variations(query, event)
            
            else:
                # Generic string matching
                return indicator_lower in query_lower
            
        except Exception as e:
            self.logger.error(f"Error checking behavioral indicator {indicator}: {e}")
            return False
    
    def _detect_query_variations(self, query: str, event: InfrastructureEvent) -> bool:
        """Detect if query is a variation of previous queries"""
        try:
            user_key = f"{event.user_account}_{event.source_ip}"
            normalized_query = self._normalize_query(query)
            
            # Check if we have similar normalized queries from this user
            if user_key in self._query_patterns:
                for existing_query, count in self._query_patterns[user_key].items():
                    if count > 1:  # Query seen multiple times
                        # Calculate similarity (simplified)
                        similarity = self._calculate_query_similarity(normalized_query, existing_query)
                        if 0.7 <= similarity < 0.95:  # Similar but not identical
                            return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error detecting query variations: {e}")
            return False
    
    def _calculate_query_similarity(self, query1: str, query2: str) -> float:
        """Calculate similarity between two queries"""
        try:
            # Simple similarity based on common words
            words1 = set(query1.split())
            words2 = set(query2.split())
            
            if not words1 and not words2:
                return 1.0
            if not words1 or not words2:
                return 0.0
            
            intersection = len(words1.intersection(words2))
            union = len(words1.union(words2))
            
            return intersection / union if union > 0 else 0.0
            
        except Exception as e:
            self.logger.error(f"Error calculating query similarity: {e}")
            return 0.0
    
    def _calculate_evasion_confidence(self, technique: EvasionTechnique, event: InfrastructureEvent,
                                    query: str, obfuscation_matches: int, behavioral_matches: int) -> float:
        """Calculate confidence score for evasion technique detection"""
        try:
            base_confidence = 0.5
            
            # Adjust based on number of obfuscation matches
            obfuscation_adjustment = min(obfuscation_matches * 0.1, 0.3)
            
            # Adjust based on behavioral matches
            behavioral_adjustment = min(behavioral_matches * 0.15, 0.3)
            
            # Adjust based on user account
            user_adjustment = 0.0
            if event.user_account == 'uba_user':
                user_adjustment = 0.2
            
            # Adjust based on timing
            timing_adjustment = 0.0
            event_hour = event.timestamp.hour
            if event_hour < 6 or event_hour > 22:
                timing_adjustment = 0.1
            
            # Adjust based on query complexity
            complexity_adjustment = 0.0
            if len(query) > 500:
                complexity_adjustment = 0.1
            
            final_confidence = min(
                base_confidence + obfuscation_adjustment + behavioral_adjustment + 
                user_adjustment + timing_adjustment + complexity_adjustment,
                1.0
            )
            
            return final_confidence
            
        except Exception as e:
            self.logger.error(f"Error calculating evasion confidence: {e}")
            return 0.5
    
    def _create_evasion_threat_detection(self, technique_name: str, source_ip: str,
                                       user_account: str, indicators: List[Dict[str, Any]]) -> ThreatDetection:
        """Create threat detection for evasion technique"""
        try:
            # Calculate overall confidence
            confidences = [indicator['confidence'] for indicator in indicators]
            avg_confidence = statistics.mean(confidences)
            
            # Determine severity
            severity = ThreatLevel.MEDIUM
            if avg_confidence >= 0.8 or technique_name in ['query_obfuscation', 'polymorphic_queries']:
                severity = ThreatLevel.HIGH
            if user_account == 'uba_user':
                severity = ThreatLevel.CRITICAL
            
            # Collect evidence
            evidence_chain = [indicator['event_id'] for indicator in indicators]
            
            # Create attack indicators
            attack_indicators = {
                "threat_type": AdvancedThreatType.EVASION_TECHNIQUE.value,
                "technique_name": technique_name,
                "evasion_method": indicators[0].get('evasion_method', 'unknown'),
                "source_ip": source_ip,
                "user_account": user_account,
                "indicators_count": len(indicators),
                "total_obfuscation_matches": sum(indicator.get('obfuscation_matches', 0) for indicator in indicators),
                "total_behavioral_matches": sum(indicator.get('behavioral_matches', 0) for indicator in indicators),
                "unique_patterns": list(set(
                    pattern for indicator in indicators 
                    for pattern in indicator.get('matched_patterns', [])
                )),
                "time_span_minutes": self._calculate_evasion_time_span(indicators)
            }
            
            # Determine affected components
            affected_components = [ComponentType.DATABASE, ComponentType.MONITORING_SERVICE]
            
            # Determine response actions
            response_actions = [
                "investigate_evasion_techniques",
                "analyze_query_patterns",
                "review_obfuscation_methods",
                "monitor_behavioral_changes"
            ]
            
            if severity in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                response_actions.extend([
                    "implement_query_normalization",
                    "enhance_pattern_detection",
                    "escalate_to_security_team"
                ])
            
            if severity == ThreatLevel.CRITICAL:
                response_actions.extend([
                    "immediate_incident_response",
                    "emergency_monitoring_enhancement",
                    "isolate_affected_systems"
                ])
            
            threat_detection = ThreatDetection(
                detection_id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                threat_type=f"evasion_{technique_name}",
                severity=severity,
                affected_components=affected_components,
                attack_indicators=attack_indicators,
                confidence_score=avg_confidence,
                response_actions=response_actions,
                evidence_chain=evidence_chain
            )
            
            return threat_detection
            
        except Exception as e:
            self.logger.error(f"Error creating evasion threat detection: {e}")
            # Return basic threat detection
            return ThreatDetection(
                detection_id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                threat_type=f"evasion_{technique_name}",
                severity=ThreatLevel.MEDIUM,
                affected_components=[ComponentType.DATABASE],
                attack_indicators={"technique": technique_name},
                confidence_score=0.6,
                response_actions=["investigate_evasion_techniques"],
                evidence_chain=[]
            )
    
    def _calculate_evasion_time_span(self, indicators: List[Dict[str, Any]]) -> float:
        """Calculate time span of evasion indicators in minutes"""
        try:
            timestamps = [indicator['timestamp'] for indicator in indicators]
            
            if len(timestamps) <= 1:
                return 0.0
            
            min_time = min(timestamps)
            max_time = max(timestamps)
            
            return (max_time - min_time).total_seconds() / 60.0
            
        except Exception as e:
            self.logger.error(f"Error calculating evasion time span: {e}")
            return 0.0
    
    def _update_behavioral_baselines(self):
        """Update behavioral baselines for users and systems"""
        try:
            current_time = datetime.now(timezone.utc)
            baseline_window = current_time - timedelta(days=self._analysis_windows['baseline_learning_days'])
            
            with self._event_lock:
                baseline_events = [
                    event for event in self._event_history
                    if event.timestamp >= baseline_window
                ]
            
            # Update user baselines
            with self._baseline_lock:
                user_activities = defaultdict(list)
                
                for event in baseline_events:
                    user_key = f"{event.user_account}_{event.source_ip}"
                    user_activities[user_key].append(event)
                
                for user_key, events in user_activities.items():
                    if len(events) >= 10:  # Need sufficient data for baseline
                        baseline = self._calculate_user_baseline(events)
                        self._user_baselines[user_key] = baseline
            
        except Exception as e:
            self.logger.error(f"Error updating behavioral baselines: {e}")
    
    def _calculate_user_baseline(self, events: List[InfrastructureEvent]) -> Dict[str, Any]:
        """Calculate behavioral baseline for a user"""
        try:
            # Calculate timing patterns
            timestamps = [event.timestamp.timestamp() for event in events]
            timestamps.sort()
            
            inter_arrivals = []
            for i in range(1, len(timestamps)):
                inter_arrivals.append(timestamps[i] - timestamps[i-1])
            
            # Calculate query characteristics
            query_lengths = []
            query_types = Counter()
            
            for event in events:
                if event.action_details:
                    query = event.action_details.get('query', '')
                    if query:
                        query_lengths.append(len(query))
                        
                        # Classify query type (simplified)
                        query_lower = query.lower()
                        if 'select' in query_lower:
                            query_types['select'] += 1
                        elif 'insert' in query_lower:
                            query_types['insert'] += 1
                        elif 'update' in query_lower:
                            query_types['update'] += 1
                        elif 'delete' in query_lower:
                            query_types['delete'] += 1
                        else:
                            query_types['other'] += 1
            
            # Calculate activity hours
            activity_hours = [event.timestamp.hour for event in events]
            hour_distribution = Counter(activity_hours)
            
            baseline = {
                'event_count': len(events),
                'avg_inter_arrival': statistics.mean(inter_arrivals) if inter_arrivals else 0,
                'std_inter_arrival': statistics.stdev(inter_arrivals) if len(inter_arrivals) > 1 else 0,
                'avg_query_length': statistics.mean(query_lengths) if query_lengths else 0,
                'std_query_length': statistics.stdev(query_lengths) if len(query_lengths) > 1 else 0,
                'query_type_distribution': dict(query_types),
                'activity_hour_distribution': dict(hour_distribution),
                'most_active_hours': [hour for hour, count in hour_distribution.most_common(3)],
                'last_updated': datetime.now(timezone.utc)
            }
            
            return baseline
            
        except Exception as e:
            self.logger.error(f"Error calculating user baseline: {e}")
            return {}
    
    def _cleanup_old_data(self):
        """Clean up old data to prevent memory leaks"""
        try:
            current_time = datetime.now(timezone.utc)
            cutoff_time = current_time - timedelta(hours=48)  # Keep 48 hours of data
            
            # Clean up old detections
            with self._detection_lock:
                self._advanced_detections = [
                    detection for detection in self._advanced_detections
                    if detection.timestamp >= cutoff_time
                ]
            
            # Clean up old baselines
            with self._baseline_lock:
                expired_baselines = []
                for user_key, baseline in self._user_baselines.items():
                    if baseline.get('last_updated', current_time) < cutoff_time:
                        expired_baselines.append(user_key)
                
                for user_key in expired_baselines:
                    del self._user_baselines[user_key]
            
            # Clean up old statistical patterns
            for user_key in list(self._timing_patterns.keys()):
                cutoff_timestamp = cutoff_time.timestamp()
                self._timing_patterns[user_key] = [
                    t for t in self._timing_patterns[user_key] if t >= cutoff_timestamp
                ]
                
                if not self._timing_patterns[user_key]:
                    del self._timing_patterns[user_key]
            
            # Clean up old volume patterns
            for user_key in list(self._volume_patterns.keys()):
                if len(self._volume_patterns[user_key]) > 1000:
                    self._volume_patterns[user_key] = self._volume_patterns[user_key][-500:]
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {e}")
    
    # Implementation of DetectionInterface methods
    
    def analyze_events(self, events: List[InfrastructureEvent]) -> List[ThreatDetection]:
        """
        Analyze events for advanced threats
        
        Args:
            events: List of infrastructure events to analyze
            
        Returns:
            List of threat detections
        """
        threat_detections = []
        
        try:
            # Add events to analysis
            for event in events:
                self.add_event(event)
            
            # Directly analyze the provided events instead of relying on time windows
            # This ensures immediate analysis for testing and API usage
            threat_detections.extend(self._analyze_events_for_persistence(events))
            threat_detections.extend(self._analyze_events_for_exfiltration(events))
            threat_detections.extend(self._analyze_events_for_evasion(events))
            
            # Also add to recent detections for consistency
            with self._detection_lock:
                self._advanced_detections.extend(threat_detections)
            
        except Exception as e:
            self.logger.error(f"Error analyzing events for advanced threats: {e}")
        
        return threat_detections
    
    def update_patterns(self, new_patterns: Dict[str, Any]) -> bool:
        """
        Update advanced threat detection patterns
        
        Args:
            new_patterns: New patterns and configuration to apply
            
        Returns:
            True if patterns updated successfully, False otherwise
        """
        try:
            # Update thresholds
            if 'thresholds' in new_patterns:
                self._thresholds.update(new_patterns['thresholds'])
            
            # Update analysis windows
            if 'analysis_windows' in new_patterns:
                self._analysis_windows.update(new_patterns['analysis_windows'])
            
            # Update detection parameters
            if 'detection_params' in new_patterns:
                self._detection_params.update(new_patterns['detection_params'])
            
            # Update persistence mechanisms
            if 'persistence_mechanisms' in new_patterns:
                for mechanism_data in new_patterns['persistence_mechanisms']:
                    try:
                        mechanism = PersistenceMechanism(**mechanism_data)
                        self._persistence_mechanisms[mechanism.mechanism_id] = mechanism
                    except Exception as e:
                        self.logger.error(f"Error adding persistence mechanism: {e}")
            
            # Update exfiltration patterns
            if 'exfiltration_patterns' in new_patterns:
                for pattern_data in new_patterns['exfiltration_patterns']:
                    try:
                        pattern = ExfiltrationPattern(**pattern_data)
                        self._exfiltration_patterns[pattern.pattern_id] = pattern
                    except Exception as e:
                        self.logger.error(f"Error adding exfiltration pattern: {e}")
            
            # Update evasion techniques
            if 'evasion_techniques' in new_patterns:
                for technique_data in new_patterns['evasion_techniques']:
                    try:
                        technique = EvasionTechnique(**technique_data)
                        self._evasion_techniques[technique.technique_id] = technique
                    except Exception as e:
                        self.logger.error(f"Error adding evasion technique: {e}")
            
            # Update threat intelligence
            if 'threat_intelligence' in new_patterns:
                for intel_data in new_patterns['threat_intelligence']:
                    try:
                        intel = ThreatIntelligence(**intel_data)
                        self._threat_intelligence[intel.intelligence_id] = intel
                    except Exception as e:
                        self.logger.error(f"Error adding threat intelligence: {e}")
            
            self.logger.info("Advanced threat detection patterns updated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating advanced threat detection patterns: {e}")
            return False
    
    def get_detection_rules(self) -> Dict[str, Any]:
        """
        Get current advanced threat detection rules
        
        Returns:
            Dictionary containing current detection rules and configuration
        """
        try:
            return {
                "persistence_mechanisms": {
                    mechanism_id: {
                        "mechanism_type": mechanism.mechanism_type,
                        "detection_patterns": mechanism.detection_patterns,
                        "stealth_level": mechanism.stealth_level,
                        "cleanup_difficulty": mechanism.cleanup_difficulty,
                        "impact_level": mechanism.impact_level.value
                    }
                    for mechanism_id, mechanism in self._persistence_mechanisms.items()
                },
                "exfiltration_patterns": {
                    pattern_id: {
                        "exfiltration_method": pattern.exfiltration_method,
                        "detection_signatures": pattern.detection_signatures,
                        "data_volume_threshold": pattern.data_volume_threshold
                    }
                    for pattern_id, pattern in self._exfiltration_patterns.items()
                },
                "evasion_techniques": {
                    technique_id: {
                        "technique_name": technique.technique_name,
                        "evasion_method": technique.evasion_method,
                        "detection_countermeasures": technique.detection_countermeasures
                    }
                    for technique_id, technique in self._evasion_techniques.items()
                },
                "threat_intelligence": {
                    intel_id: {
                        "threat_actor": intel.threat_actor,
                        "confidence_level": intel.confidence_level,
                        "last_updated": intel.last_updated.isoformat()
                    }
                    for intel_id, intel in self._threat_intelligence.items()
                },
                "thresholds": self._thresholds,
                "analysis_windows": self._analysis_windows,
                "detection_params": self._detection_params,
                "active": self._active
            }
            
        except Exception as e:
            self.logger.error(f"Error getting advanced detection rules: {e}")
            return {}
    
    def is_healthy(self) -> bool:
        """Check if the advanced threat detector is healthy"""
        try:
            # Basic health check - patterns loaded and system initialized
            basic_health = (len(self._persistence_mechanisms) > 0 and
                           len(self._exfiltration_patterns) > 0 and
                           len(self._evasion_techniques) > 0)
            
            # If active, also check thread health
            if self._active:
                return (basic_health and 
                       self._detection_thread and 
                       self._detection_thread.is_alive())
            
            return basic_health
        except Exception as e:
            self.logger.error(f"Error checking advanced threat detector health: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get advanced threat detection statistics"""
        try:
            with self._detection_lock:
                detection_count = len(self._advanced_detections)
            
            with self._baseline_lock:
                baseline_count = len(self._user_baselines)
            
            return {
                "active": self._active,
                "persistence_mechanisms": len(self._persistence_mechanisms),
                "exfiltration_patterns": len(self._exfiltration_patterns),
                "evasion_techniques": len(self._evasion_techniques),
                "threat_intelligence_entries": len(self._threat_intelligence),
                "event_history_size": len(self._event_history),
                "recent_detections": detection_count,
                "user_baselines": baseline_count,
                "query_patterns_tracked": len(self._query_patterns),
                "timing_patterns_tracked": len(self._timing_patterns),
                "volume_patterns_tracked": len(self._volume_patterns)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting advanced threat detection statistics: {e}")
            return {}
    
    def _analyze_events_for_persistence(self, events: List[InfrastructureEvent]) -> List[ThreatDetection]:
        """Directly analyze provided events for persistence mechanisms"""
        threat_detections = []
        
        try:
            # Analyze events for persistence indicators
            persistence_indicators = []
            
            for event in events:
                indicators = self._analyze_event_for_persistence(event)
                persistence_indicators.extend(indicators)
            
            # Group indicators by mechanism type and source
            grouped_indicators = defaultdict(list)
            for indicator in persistence_indicators:
                key = (indicator['mechanism_type'], indicator['source_ip'], indicator['user_account'])
                grouped_indicators[key].append(indicator)
            
            # Create threat detections for significant persistence attempts
            for (mechanism_type, source_ip, user_account), indicators in grouped_indicators.items():
                if len(indicators) >= self._detection_params['min_persistence_indicators']:
                    threat_detection = self._create_persistence_threat_detection(
                        mechanism_type, source_ip, user_account, indicators
                    )
                    threat_detections.append(threat_detection)
                    
                    # Log critical persistence detections
                    if threat_detection.severity == ThreatLevel.CRITICAL:
                        self.crypto_logger.log_monitoring_event(
                            f"persistence_mechanism_detected_{mechanism_type}",
                            "uba_infrastructure",
                            {
                                "mechanism_type": mechanism_type,
                                "source_ip": source_ip,
                                "user_account": user_account,
                                "indicators_count": len(indicators),
                                "confidence": threat_detection.confidence_score
                            },
                            threat_detection.confidence_score
                        )
            
        except Exception as e:
            self.logger.error(f"Error analyzing events for persistence: {e}")
        
        return threat_detections
    
    def _analyze_events_for_exfiltration(self, events: List[InfrastructureEvent]) -> List[ThreatDetection]:
        """Directly analyze provided events for data exfiltration"""
        threat_detections = []
        
        try:
            # Analyze events for exfiltration patterns
            exfiltration_indicators = []
            
            for event in events:
                indicators = self._analyze_event_for_exfiltration(event)
                exfiltration_indicators.extend(indicators)
            
            # Analyze volume and timing patterns
            volume_anomalies = self._detect_volume_anomalies(events)
            timing_anomalies = self._detect_timing_anomalies(events)
            
            # Combine pattern-based and statistical indicators
            all_indicators = exfiltration_indicators + volume_anomalies + timing_anomalies
            
            # Group indicators by source and method
            grouped_indicators = defaultdict(list)
            for indicator in all_indicators:
                key = (indicator['source_ip'], indicator['user_account'], indicator.get('method', 'unknown'))
                grouped_indicators[key].append(indicator)
            
            # Create threat detections for significant exfiltration attempts
            for (source_ip, user_account, method), indicators in grouped_indicators.items():
                if len(indicators) >= 1:  # At least 1 indicator for exfiltration
                    threat_detection = self._create_exfiltration_threat_detection(
                        source_ip, user_account, method, indicators
                    )
                    threat_detections.append(threat_detection)
                    
                    # Log high-severity exfiltration detections
                    if threat_detection.severity in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                        self.crypto_logger.log_monitoring_event(
                            f"data_exfiltration_detected_{method}",
                            "uba_infrastructure",
                            {
                                "method": method,
                                "source_ip": source_ip,
                                "user_account": user_account,
                                "indicators_count": len(indicators),
                                "confidence": threat_detection.confidence_score
                            },
                            threat_detection.confidence_score
                        )
            
        except Exception as e:
            self.logger.error(f"Error analyzing events for exfiltration: {e}")
        
        return threat_detections
    
    def _analyze_events_for_evasion(self, events: List[InfrastructureEvent]) -> List[ThreatDetection]:
        """Directly analyze provided events for evasion techniques"""
        threat_detections = []
        
        try:
            # Analyze events for evasion techniques
            evasion_indicators = []
            
            for event in events:
                indicators = self._analyze_event_for_evasion(event)
                evasion_indicators.extend(indicators)
            
            # Group indicators by technique and source
            grouped_indicators = defaultdict(list)
            for indicator in evasion_indicators:
                key = (indicator['technique_name'], indicator['source_ip'], indicator['user_account'])
                grouped_indicators[key].append(indicator)
            
            # Create threat detections for significant evasion attempts
            for (technique_name, source_ip, user_account), indicators in grouped_indicators.items():
                if len(indicators) >= 1:  # At least 1 indicator for evasion
                    threat_detection = self._create_evasion_threat_detection(
                        technique_name, source_ip, user_account, indicators
                    )
                    threat_detections.append(threat_detection)
                    
                    # Log evasion technique detections
                    if threat_detection.severity in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                        self.crypto_logger.log_monitoring_event(
                            f"evasion_technique_detected_{technique_name}",
                            "uba_infrastructure",
                            {
                                "technique": technique_name,
                                "source_ip": source_ip,
                                "user_account": user_account,
                                "indicators_count": len(indicators),
                                "confidence": threat_detection.confidence_score
                            },
                            threat_detection.confidence_score
                        )
            
        except Exception as e:
            self.logger.error(f"Error analyzing events for evasion: {e}")
        
        return threat_detections