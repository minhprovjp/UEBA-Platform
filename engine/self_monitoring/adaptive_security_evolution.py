"""
Adaptive Security Evolution System for UBA Self-Monitoring

This module implements dynamic detection capability updates, threat intelligence
integration, and proactive protection measure deployment.

Requirements: 8.3, 8.5
"""

import logging
import json
import threading
import time
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple
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
    from .advanced_threat_detector import (
        AdvancedThreatDetector,
        PersistenceMechanism,
        ExfiltrationPattern,
        EvasionTechnique,
        ThreatIntelligence
    )
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
    from advanced_threat_detector import (
        AdvancedThreatDetector,
        PersistenceMechanism,
        ExfiltrationPattern,
        EvasionTechnique,
        ThreatIntelligence
    )


class EvolutionTrigger(Enum):
    """Triggers for security evolution"""
    NEW_THREAT_PATTERN = "new_threat_pattern"
    DETECTION_FAILURE = "detection_failure"
    FALSE_POSITIVE_SPIKE = "false_positive_spike"
    THREAT_INTELLIGENCE_UPDATE = "threat_intelligence_update"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    MANUAL_UPDATE = "manual_update"


@dataclass
class SecurityUpdate:
    """Security update definition"""
    update_id: str
    update_type: str
    trigger: EvolutionTrigger
    timestamp: datetime
    description: str
    changes: Dict[str, Any]
    confidence: float
    impact_assessment: Dict[str, Any]
    rollback_data: Optional[Dict[str, Any]] = None
    applied: bool = False
    success: bool = False


@dataclass
class ThreatIntelligenceFeed:
    """Threat intelligence feed configuration"""
    feed_id: str
    feed_name: str
    feed_url: str
    feed_type: str  # json, xml, csv, etc.
    update_frequency: int  # seconds
    last_update: Optional[datetime] = None
    active: bool = True
    authentication: Optional[Dict[str, str]] = None


class AdaptiveSecurityEvolution:
    """
    Adaptive security evolution system that dynamically updates detection
    capabilities based on threat intelligence and system performance
    """
    
    def __init__(self, advanced_detector: AdvancedThreatDetector,
                 config_manager: Optional[SelfMonitoringConfig] = None,
                 crypto_logger: Optional[CryptoLogger] = None):
        """
        Initialize adaptive security evolution system
        
        Args:
            advanced_detector: Advanced threat detector to evolve
            config_manager: Configuration manager instance
            crypto_logger: Cryptographic logger for secure audit trails
        """
        self.advanced_detector = advanced_detector
        self.config_manager = config_manager or SelfMonitoringConfig()
        self.crypto_logger = crypto_logger or CryptoLogger()
        self.logger = logging.getLogger(__name__)
        
        # Evolution state
        self._active = False
        self._evolution_thread = None
        self._stop_event = threading.Event()
        
        # Update tracking
        self._pending_updates: List[SecurityUpdate] = []
        self._applied_updates: List[SecurityUpdate] = []
        self._update_lock = threading.Lock()
        
        # Threat intelligence feeds
        self._intelligence_feeds: Dict[str, ThreatIntelligenceFeed] = {}
        self._feed_lock = threading.Lock()
        
        # Performance monitoring
        self._performance_metrics: Dict[str, List[float]] = defaultdict(list)
        self._detection_effectiveness: Dict[str, float] = {}
        self._false_positive_rates: Dict[str, float] = {}
        
        # Evolution parameters
        self._evolution_config = {}
        
        # Load configuration
        self._load_evolution_config()
        self._initialize_threat_intelligence_feeds()
    
    def _load_evolution_config(self):
        """Load adaptive security evolution configuration"""
        try:
            config = self.config_manager.load_config()
            evolution_config = config.get('adaptive_security_evolution', {})
            
            self._evolution_config = {
                'update_check_interval': evolution_config.get('update_check_interval', 300),  # 5 minutes
                'threat_intelligence_update_interval': evolution_config.get('threat_intelligence_update_interval', 3600),  # 1 hour
                'performance_monitoring_window': evolution_config.get('performance_monitoring_window', 86400),  # 24 hours
                'false_positive_threshold': evolution_config.get('false_positive_threshold', 0.1),  # 10%
                'detection_effectiveness_threshold': evolution_config.get('detection_effectiveness_threshold', 0.8),  # 80%
                'auto_apply_updates': evolution_config.get('auto_apply_updates', True),
                'max_pending_updates': evolution_config.get('max_pending_updates', 50),
                'rollback_enabled': evolution_config.get('rollback_enabled', True)
            }
            
            self.logger.info("Adaptive security evolution configuration loaded")
            
        except Exception as e:
            self.logger.error(f"Error loading evolution configuration: {e}")
            # Use safe defaults
            self._evolution_config = {
                'update_check_interval': 300,
                'threat_intelligence_update_interval': 3600,
                'performance_monitoring_window': 86400,
                'false_positive_threshold': 0.1,
                'detection_effectiveness_threshold': 0.8,
                'auto_apply_updates': True,
                'max_pending_updates': 50,
                'rollback_enabled': True
            }
    
    def _initialize_threat_intelligence_feeds(self):
        """Initialize threat intelligence feeds"""
        try:
            # Sample threat intelligence feeds
            feeds = [
                ThreatIntelligenceFeed(
                    feed_id="mitre_attack",
                    feed_name="MITRE ATT&CK Framework",
                    feed_url="https://attack.mitre.org/data/enterprise-attack.json",
                    feed_type="json",
                    update_frequency=86400,  # Daily
                    active=True
                ),
                ThreatIntelligenceFeed(
                    feed_id="cve_database",
                    feed_name="CVE Database",
                    feed_url="https://cve.mitre.org/data/downloads/allitems.xml",
                    feed_type="xml",
                    update_frequency=43200,  # Twice daily
                    active=True
                ),
                ThreatIntelligenceFeed(
                    feed_id="internal_threat_intel",
                    feed_name="Internal Threat Intelligence",
                    feed_url="internal://threat-intel/feed.json",
                    feed_type="json",
                    update_frequency=1800,  # 30 minutes
                    active=True
                )
            ]
            
            for feed in feeds:
                self._intelligence_feeds[feed.feed_id] = feed
            
            self.logger.info(f"Initialized {len(feeds)} threat intelligence feeds")
            
        except Exception as e:
            self.logger.error(f"Error initializing threat intelligence feeds: {e}")
    
    def start_evolution(self) -> bool:
        """Start adaptive security evolution"""
        try:
            if self._active:
                self.logger.warning("Adaptive security evolution is already active")
                return True
            
            # Start evolution thread
            self._active = True
            self._stop_event.clear()
            self._evolution_thread = threading.Thread(target=self._evolution_loop, daemon=True)
            self._evolution_thread.start()
            
            self.logger.info("Adaptive security evolution started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting adaptive security evolution: {e}")
            self._active = False
            return False
    
    def stop_evolution(self) -> bool:
        """Stop adaptive security evolution"""
        try:
            if not self._active:
                self.logger.warning("Adaptive security evolution is not active")
                return True
            
            # Signal stop and wait for thread
            self._stop_event.set()
            self._active = False
            
            if self._evolution_thread and self._evolution_thread.is_alive():
                self._evolution_thread.join(timeout=10)
            
            self.logger.info("Adaptive security evolution stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping adaptive security evolution: {e}")
            return False 
    def _evolution_loop(self):
        """Main adaptive security evolution loop"""
        while not self._stop_event.is_set():
            try:
                # Check for threat intelligence updates
                self._update_threat_intelligence()
                
                # Monitor detection performance
                self._monitor_detection_performance()
                
                # Generate security updates based on analysis
                self._generate_security_updates()
                
                # Apply pending updates if auto-apply is enabled
                if self._evolution_config['auto_apply_updates']:
                    self._apply_pending_updates()
                
                # Clean up old data
                self._cleanup_old_data()
                
            except Exception as e:
                self.logger.error(f"Error in adaptive security evolution loop: {e}")
            
            # Wait before next iteration
            self._stop_event.wait(self._evolution_config['update_check_interval'])
    
    def _update_threat_intelligence(self):
        """Update threat intelligence from configured feeds"""
        try:
            current_time = datetime.now(timezone.utc)
            
            with self._feed_lock:
                for feed_id, feed in self._intelligence_feeds.items():
                    if not feed.active:
                        continue
                    
                    # Check if update is needed
                    if (feed.last_update is None or 
                        (current_time - feed.last_update).total_seconds() >= feed.update_frequency):
                        
                        self.logger.info(f"Updating threat intelligence from feed: {feed.feed_name}")
                        
                        # Simulate threat intelligence update (in real implementation, would fetch from URL)
                        intelligence_data = self._fetch_threat_intelligence(feed)
                        
                        if intelligence_data:
                            # Process and integrate new intelligence
                            updates = self._process_threat_intelligence(feed_id, intelligence_data)
                            
                            # Add updates to pending list
                            with self._update_lock:
                                self._pending_updates.extend(updates)
                            
                            feed.last_update = current_time
                            
                            self.logger.info(f"Generated {len(updates)} security updates from {feed.feed_name}")
            
        except Exception as e:
            self.logger.error(f"Error updating threat intelligence: {e}")
    
    def _fetch_threat_intelligence(self, feed: ThreatIntelligenceFeed) -> Optional[Dict[str, Any]]:
        """Fetch threat intelligence data from feed (simulated)"""
        try:
            # In a real implementation, this would make HTTP requests to fetch data
            # For now, simulate with sample threat intelligence data
            
            sample_intelligence = {
                "threats": [
                    {
                        "id": "T1003",
                        "name": "OS Credential Dumping",
                        "description": "Adversaries may attempt to dump credentials to obtain account login information",
                        "techniques": ["LSASS Memory", "Security Account Manager", "NTDS"],
                        "detection_patterns": [
                            r"(?i)select\s+.*\s+from\s+mysql\.user",
                            r"(?i)show\s+grants\s+for",
                            r"(?i)select\s+user\s*\(\s*\)"
                        ],
                        "severity": "high",
                        "confidence": 0.9
                    },
                    {
                        "id": "T1505.003",
                        "name": "Web Shell",
                        "description": "Adversaries may backdoor web servers with web shells",
                        "techniques": ["PHP Web Shell", "ASP Web Shell", "JSP Web Shell"],
                        "detection_patterns": [
                            r"(?i)create\s+procedure.*exec",
                            r"(?i)create\s+function.*system",
                            r"(?i)load_file\s*\("
                        ],
                        "severity": "critical",
                        "confidence": 0.95
                    }
                ],
                "indicators": [
                    {
                        "type": "query_pattern",
                        "value": r"(?i)union\s+select.*password",
                        "description": "SQL injection targeting password fields",
                        "confidence": 0.85
                    },
                    {
                        "type": "timing_pattern",
                        "value": "rapid_credential_queries",
                        "description": "Rapid succession of credential-related queries",
                        "confidence": 0.8
                    }
                ],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            return sample_intelligence
            
        except Exception as e:
            self.logger.error(f"Error fetching threat intelligence from {feed.feed_name}: {e}")
            return None
    
    def _process_threat_intelligence(self, feed_id: str, intelligence_data: Dict[str, Any]) -> List[SecurityUpdate]:
        """Process threat intelligence data and generate security updates"""
        updates = []
        
        try:
            # Process new threats
            for threat in intelligence_data.get("threats", []):
                update = self._create_threat_update(feed_id, threat)
                if update:
                    updates.append(update)
            
            # Process new indicators
            for indicator in intelligence_data.get("indicators", []):
                update = self._create_indicator_update(feed_id, indicator)
                if update:
                    updates.append(update)
            
        except Exception as e:
            self.logger.error(f"Error processing threat intelligence: {e}")
        
        return updates
    
    def _create_threat_update(self, feed_id: str, threat: Dict[str, Any]) -> Optional[SecurityUpdate]:
        """Create security update from threat intelligence"""
        try:
            threat_id = str(threat.get("id", str(uuid.uuid4())))
            threat_name = str(threat.get("name", "Unknown Threat"))
            detection_patterns = threat.get("detection_patterns", [])
            # Ensure detection_patterns is a list
            if not isinstance(detection_patterns, list):
                detection_patterns = [str(detection_patterns)] if detection_patterns else []
            severity = str(threat.get("severity", "medium"))
            # Safe confidence conversion
            try:
                confidence = float(threat.get("confidence", 0.7))
                # Clamp confidence to valid range
                confidence = max(0.0, min(1.0, confidence))
            except (ValueError, TypeError):
                confidence = 0.7
            
            if not detection_patterns:
                return None
            
            # Determine update type based on threat characteristics
            update_type = "persistence_mechanism"
            threat_name_lower = threat_name.lower()
            if "credential" in threat_name_lower or "dump" in threat_name_lower:
                update_type = "persistence_mechanism"
            elif "shell" in threat_name_lower or "backdoor" in threat_name_lower:
                update_type = "persistence_mechanism"
            elif "exfiltration" in threat_name_lower or "data" in threat_name_lower:
                update_type = "exfiltration_pattern"
            else:
                update_type = "evasion_technique"
            
            # Create security update
            update = SecurityUpdate(
                update_id=str(uuid.uuid4()),
                update_type=update_type,
                trigger=EvolutionTrigger.THREAT_INTELLIGENCE_UPDATE,
                timestamp=datetime.now(timezone.utc),
                description=f"New {update_type} from {feed_id}: {threat_name}",
                changes={
                    "add_patterns": detection_patterns,
                    "threat_id": threat_id,
                    "threat_name": threat_name,
                    "severity": severity,
                    "source_feed": feed_id
                },
                confidence=confidence,
                impact_assessment={
                    "detection_improvement": 0.1,
                    "false_positive_risk": 0.05,
                    "performance_impact": 0.02
                }
            )
            
            return update
            
        except Exception as e:
            self.logger.error(f"Error creating threat update: {e}")
            return None
    
    def _create_indicator_update(self, feed_id: str, indicator: Dict[str, Any]) -> Optional[SecurityUpdate]:
        """Create security update from threat indicator"""
        try:
            indicator_type = str(indicator.get("type", "unknown"))
            indicator_value = str(indicator.get("value", ""))
            description = str(indicator.get("description", ""))
            # Safe confidence conversion
            try:
                confidence = float(indicator.get("confidence", 0.7))
                # Clamp confidence to valid range
                confidence = max(0.0, min(1.0, confidence))
            except (ValueError, TypeError):
                confidence = 0.7
            
            if not indicator_value:
                return None
            
            # Create security update based on indicator type
            update = SecurityUpdate(
                update_id=str(uuid.uuid4()),
                update_type="detection_pattern",
                trigger=EvolutionTrigger.THREAT_INTELLIGENCE_UPDATE,
                timestamp=datetime.now(timezone.utc),
                description=f"New {indicator_type} indicator from {feed_id}: {description}",
                changes={
                    "add_indicator": {
                        "type": indicator_type,
                        "value": indicator_value,
                        "description": description
                    },
                    "source_feed": feed_id
                },
                confidence=confidence,
                impact_assessment={
                    "detection_improvement": 0.05,
                    "false_positive_risk": 0.03,
                    "performance_impact": 0.01
                }
            )
            
            return update
            
        except Exception as e:
            self.logger.error(f"Error creating indicator update: {e}")
            return None
    
    def _monitor_detection_performance(self):
        """Monitor detection performance and identify areas for improvement"""
        try:
            # Get recent detection statistics
            detector_stats = self.advanced_detector.get_statistics()
            
            # Calculate detection effectiveness
            self._calculate_detection_effectiveness(detector_stats)
            
            # Monitor false positive rates
            self._monitor_false_positive_rates()
            
            # Check for performance degradation
            self._check_performance_degradation()
            
        except Exception as e:
            self.logger.error(f"Error monitoring detection performance: {e}")
    
    def _calculate_detection_effectiveness(self, stats: Dict[str, Any]):
        """Calculate detection effectiveness metrics"""
        try:
            current_time = datetime.now(timezone.utc)
            
            # Store performance metrics
            self._performance_metrics['total_detections'].append(stats.get('recent_detections', 0))
            self._performance_metrics['event_processing_rate'].append(stats.get('event_history_size', 0))
            
            # Calculate effectiveness for each detection type
            detection_types = ['persistence_mechanisms', 'exfiltration_patterns', 'evasion_techniques']
            
            for detection_type in detection_types:
                count = stats.get(detection_type, 0)
                if count > 0:
                    # Simple effectiveness calculation (in real implementation, would be more sophisticated)
                    effectiveness = min(1.0, count / 10.0)  # Normalize to 0-1 scale
                    self._detection_effectiveness[detection_type] = effectiveness
                    
                    # Check if effectiveness is below threshold
                    if effectiveness < self._evolution_config['detection_effectiveness_threshold']:
                        self._generate_effectiveness_improvement_update(detection_type, effectiveness)
            
            # Keep only recent metrics
            window_size = 100
            for metric_name in self._performance_metrics:
                if len(self._performance_metrics[metric_name]) > window_size:
                    self._performance_metrics[metric_name] = self._performance_metrics[metric_name][-window_size:]
            
        except Exception as e:
            self.logger.error(f"Error calculating detection effectiveness: {e}")
    
    def _monitor_false_positive_rates(self):
        """Monitor false positive rates and generate updates if needed"""
        try:
            # In a real implementation, this would analyze actual false positive reports
            # For now, simulate monitoring
            
            detection_types = ['persistence_mechanisms', 'exfiltration_patterns', 'evasion_techniques']
            
            for detection_type in detection_types:
                # Simulate false positive rate calculation
                simulated_fp_rate = 0.05  # 5% false positive rate
                self._false_positive_rates[detection_type] = simulated_fp_rate
                
                # Check if false positive rate is above threshold
                if simulated_fp_rate > self._evolution_config['false_positive_threshold']:
                    self._generate_false_positive_reduction_update(detection_type, simulated_fp_rate)
            
        except Exception as e:
            self.logger.error(f"Error monitoring false positive rates: {e}")
    
    def _check_performance_degradation(self):
        """Check for performance degradation and generate optimization updates"""
        try:
            # Check processing rate trends
            if len(self._performance_metrics['event_processing_rate']) >= 10:
                recent_rates = self._performance_metrics['event_processing_rate'][-10:]
                avg_recent = sum(recent_rates) / len(recent_rates)
                
                if len(self._performance_metrics['event_processing_rate']) >= 20:
                    older_rates = self._performance_metrics['event_processing_rate'][-20:-10]
                    avg_older = sum(older_rates) / len(older_rates)
                    
                    # Check for significant degradation
                    if avg_older > 0 and (avg_recent / avg_older) < 0.8:  # 20% degradation
                        self._generate_performance_optimization_update(avg_recent, avg_older)
            
        except Exception as e:
            self.logger.error(f"Error checking performance degradation: {e}")
    
    def _generate_effectiveness_improvement_update(self, detection_type: str, current_effectiveness: float):
        """Generate update to improve detection effectiveness"""
        try:
            update = SecurityUpdate(
                update_id=str(uuid.uuid4()),
                update_type="effectiveness_improvement",
                trigger=EvolutionTrigger.DETECTION_FAILURE,
                timestamp=datetime.now(timezone.utc),
                description=f"Improve {detection_type} effectiveness (current: {current_effectiveness:.2f})",
                changes={
                    "detection_type": detection_type,
                    "adjust_thresholds": {
                        "reduce_confidence_threshold": 0.1,
                        "increase_sensitivity": 0.2
                    },
                    "add_complementary_patterns": True
                },
                confidence=0.8,
                impact_assessment={
                    "detection_improvement": 0.2,
                    "false_positive_risk": 0.1,
                    "performance_impact": 0.05
                }
            )
            
            with self._update_lock:
                self._pending_updates.append(update)
            
            self.logger.info(f"Generated effectiveness improvement update for {detection_type}")
            
        except Exception as e:
            self.logger.error(f"Error generating effectiveness improvement update: {e}")
    
    def _generate_false_positive_reduction_update(self, detection_type: str, fp_rate: float):
        """Generate update to reduce false positive rates"""
        try:
            update = SecurityUpdate(
                update_id=str(uuid.uuid4()),
                update_type="false_positive_reduction",
                trigger=EvolutionTrigger.FALSE_POSITIVE_SPIKE,
                timestamp=datetime.now(timezone.utc),
                description=f"Reduce {detection_type} false positives (current rate: {fp_rate:.2f})",
                changes={
                    "detection_type": detection_type,
                    "adjust_thresholds": {
                        "increase_confidence_threshold": 0.1,
                        "add_context_filters": True
                    },
                    "refine_patterns": True
                },
                confidence=0.9,
                impact_assessment={
                    "detection_improvement": -0.05,  # Slight reduction in sensitivity
                    "false_positive_risk": -0.3,    # Significant FP reduction
                    "performance_impact": 0.02
                }
            )
            
            with self._update_lock:
                self._pending_updates.append(update)
            
            self.logger.info(f"Generated false positive reduction update for {detection_type}")
            
        except Exception as e:
            self.logger.error(f"Error generating false positive reduction update: {e}")
    
    def _generate_performance_optimization_update(self, current_rate: float, previous_rate: float):
        """Generate update to optimize performance"""
        try:
            degradation_pct = ((previous_rate - current_rate) / previous_rate) * 100
            
            update = SecurityUpdate(
                update_id=str(uuid.uuid4()),
                update_type="performance_optimization",
                trigger=EvolutionTrigger.PERFORMANCE_DEGRADATION,
                timestamp=datetime.now(timezone.utc),
                description=f"Optimize performance (degradation: {degradation_pct:.1f}%)",
                changes={
                    "optimize_patterns": True,
                    "reduce_analysis_frequency": 0.1,
                    "implement_caching": True,
                    "parallel_processing": True
                },
                confidence=0.7,
                impact_assessment={
                    "detection_improvement": -0.02,  # Slight reduction for performance
                    "false_positive_risk": 0.01,
                    "performance_impact": -0.2      # Significant performance improvement
                }
            )
            
            with self._update_lock:
                self._pending_updates.append(update)
            
            self.logger.info(f"Generated performance optimization update (degradation: {degradation_pct:.1f}%)")
            
        except Exception as e:
            self.logger.error(f"Error generating performance optimization update: {e}")
    
    def _generate_security_updates(self):
        """Generate security updates based on current analysis"""
        try:
            # Check if we have too many pending updates
            with self._update_lock:
                if len(self._pending_updates) >= self._evolution_config['max_pending_updates']:
                    self.logger.warning(f"Maximum pending updates reached: {len(self._pending_updates)}")
                    return
            
            # Additional update generation logic can be added here
            # For example, analyzing recent detection patterns for new threats
            
        except Exception as e:
            self.logger.error(f"Error generating security updates: {e}")
    
    def _apply_pending_updates(self):
        """Apply pending security updates"""
        try:
            with self._update_lock:
                updates_to_apply = [u for u in self._pending_updates if not u.applied]
            
            for update in updates_to_apply:
                try:
                    success = self._apply_security_update(update)
                    update.applied = True
                    update.success = success
                    
                    if success:
                        self.logger.info(f"Successfully applied security update: {update.description}")
                        
                        # Log the update
                        self.crypto_logger.log_monitoring_event(
                            f"security_update_applied_{update.update_type}",
                            "adaptive_security",
                            {
                                "update_id": update.update_id,
                                "update_type": update.update_type,
                                "trigger": update.trigger.value,
                                "confidence": update.confidence,
                                "description": update.description
                            },
                            update.confidence
                        )
                    else:
                        self.logger.error(f"Failed to apply security update: {update.description}")
                
                except Exception as e:
                    self.logger.error(f"Error applying security update {update.update_id}: {e}")
                    update.applied = True
                    update.success = False
            
            # Move applied updates to applied list
            with self._update_lock:
                applied_updates = [u for u in self._pending_updates if u.applied]
                self._applied_updates.extend(applied_updates)
                self._pending_updates = [u for u in self._pending_updates if not u.applied]
            
        except Exception as e:
            self.logger.error(f"Error applying pending updates: {e}")
    
    def _apply_security_update(self, update: SecurityUpdate) -> bool:
        """Apply a specific security update"""
        try:
            # Store rollback data before applying changes
            if self._evolution_config['rollback_enabled']:
                update.rollback_data = self._create_rollback_data(update)
            
            # Apply update based on type
            if update.update_type == "persistence_mechanism":
                return self._apply_persistence_mechanism_update(update)
            elif update.update_type == "exfiltration_pattern":
                return self._apply_exfiltration_pattern_update(update)
            elif update.update_type == "evasion_technique":
                return self._apply_evasion_technique_update(update)
            elif update.update_type == "detection_pattern":
                return self._apply_detection_pattern_update(update)
            elif update.update_type == "effectiveness_improvement":
                return self._apply_effectiveness_improvement_update(update)
            elif update.update_type == "false_positive_reduction":
                return self._apply_false_positive_reduction_update(update)
            elif update.update_type == "performance_optimization":
                return self._apply_performance_optimization_update(update)
            else:
                self.logger.warning(f"Unknown update type: {update.update_type}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error applying security update: {e}")
            return False
    
    def _create_rollback_data(self, update: SecurityUpdate) -> Dict[str, Any]:
        """Create rollback data for an update"""
        try:
            # Get current detection rules for rollback
            current_rules = self.advanced_detector.get_detection_rules()
            
            rollback_data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "update_id": update.update_id,
                "previous_rules": current_rules,
                "previous_thresholds": getattr(self.advanced_detector, '_thresholds', {}),
                "previous_config": getattr(self.advanced_detector, '_detection_params', {})
            }
            
            return rollback_data
            
        except Exception as e:
            self.logger.error(f"Error creating rollback data: {e}")
            return {}
    
    def _apply_persistence_mechanism_update(self, update: SecurityUpdate) -> bool:
        """Apply persistence mechanism update"""
        try:
            changes = update.changes
            patterns = changes.get("add_patterns", [])
            
            if patterns:
                # Create new persistence mechanism
                new_mechanism = PersistenceMechanism(
                    mechanism_id=f"intel_{update.update_id[:8]}",
                    mechanism_type="threat_intelligence",
                    detection_patterns=patterns,
                    persistence_indicators=[changes.get("threat_name", "unknown")],
                    cleanup_difficulty="medium",
                    stealth_level=0.7,
                    impact_level=ThreatLevel.HIGH
                )
                
                # Update the advanced detector
                new_patterns = {
                    "persistence_mechanisms": [new_mechanism.__dict__]
                }
                
                return self.advanced_detector.update_patterns(new_patterns)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error applying persistence mechanism update: {e}")
            return False
    
    def _apply_exfiltration_pattern_update(self, update: SecurityUpdate) -> bool:
        """Apply exfiltration pattern update"""
        try:
            changes = update.changes
            patterns = changes.get("add_patterns", [])
            
            if patterns:
                # Create new exfiltration pattern
                new_pattern = ExfiltrationPattern(
                    pattern_id=f"intel_{update.update_id[:8]}",
                    exfiltration_method="threat_intelligence",
                    detection_signatures=patterns,
                    data_volume_threshold=1000,
                    time_pattern_indicators=["threat_intelligence_pattern"],
                    network_indicators=["external_communication"],
                    stealth_indicators=["obfuscated_queries"]
                )
                
                # Update the advanced detector
                new_patterns = {
                    "exfiltration_patterns": [new_pattern.__dict__]
                }
                
                return self.advanced_detector.update_patterns(new_patterns)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error applying exfiltration pattern update: {e}")
            return False
    
    def _apply_evasion_technique_update(self, update: SecurityUpdate) -> bool:
        """Apply evasion technique update"""
        try:
            changes = update.changes
            patterns = changes.get("add_patterns", [])
            
            if patterns:
                # Create new evasion technique
                new_technique = EvasionTechnique(
                    technique_id=f"intel_{update.update_id[:8]}",
                    technique_name="threat_intelligence_evasion",
                    evasion_method="intelligence_based",
                    detection_countermeasures=["pattern_analysis", "behavioral_monitoring"],
                    behavioral_indicators=["threat_intelligence_pattern"],
                    timing_indicators=["irregular_patterns"],
                    obfuscation_patterns=patterns
                )
                
                # Update the advanced detector
                new_patterns = {
                    "evasion_techniques": [new_technique.__dict__]
                }
                
                return self.advanced_detector.update_patterns(new_patterns)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error applying evasion technique update: {e}")
            return False
    
    def _apply_detection_pattern_update(self, update: SecurityUpdate) -> bool:
        """Apply general detection pattern update"""
        try:
            changes = update.changes
            indicator = changes.get("add_indicator", {})
            
            if indicator:
                indicator_type = indicator.get("type", "unknown")
                indicator_value = indicator.get("value", "")
                
                # Apply based on indicator type
                if indicator_type == "query_pattern":
                    # Add as evasion technique pattern
                    new_technique = EvasionTechnique(
                        technique_id=f"pattern_{update.update_id[:8]}",
                        technique_name="intelligence_pattern",
                        evasion_method="pattern_based",
                        detection_countermeasures=["pattern_matching"],
                        behavioral_indicators=["intelligence_indicator"],
                        timing_indicators=[],
                        obfuscation_patterns=[indicator_value]
                    )
                    
                    new_patterns = {
                        "evasion_techniques": [new_technique.__dict__]
                    }
                    
                    return self.advanced_detector.update_patterns(new_patterns)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error applying detection pattern update: {e}")
            return False
    
    def _apply_effectiveness_improvement_update(self, update: SecurityUpdate) -> bool:
        """Apply effectiveness improvement update"""
        try:
            changes = update.changes
            detection_type = changes.get("detection_type", "")
            threshold_adjustments = changes.get("adjust_thresholds", {})
            
            # Get current thresholds
            current_thresholds = getattr(self.advanced_detector, '_thresholds', {})
            
            # Apply threshold adjustments
            for adjustment, value in threshold_adjustments.items():
                if adjustment == "reduce_confidence_threshold":
                    if detection_type == "persistence_mechanisms":
                        current_thresholds['persistence_confidence'] = max(0.1, 
                            current_thresholds.get('persistence_confidence', 0.85) - value)
                    elif detection_type == "exfiltration_patterns":
                        current_thresholds['exfiltration_rate_threshold'] = max(0.1,
                            current_thresholds.get('exfiltration_rate_threshold', 0.6) - value)
                    elif detection_type == "evasion_techniques":
                        current_thresholds['evasion_confidence'] = max(0.1,
                            current_thresholds.get('evasion_confidence', 0.5) - value)
            
            # Update thresholds
            new_patterns = {
                "thresholds": current_thresholds
            }
            
            return self.advanced_detector.update_patterns(new_patterns)
            
        except Exception as e:
            self.logger.error(f"Error applying effectiveness improvement update: {e}")
            return False
    
    def _apply_false_positive_reduction_update(self, update: SecurityUpdate) -> bool:
        """Apply false positive reduction update"""
        try:
            changes = update.changes
            detection_type = changes.get("detection_type", "")
            threshold_adjustments = changes.get("adjust_thresholds", {})
            
            # Get current thresholds
            current_thresholds = getattr(self.advanced_detector, '_thresholds', {})
            
            # Apply threshold adjustments
            for adjustment, value in threshold_adjustments.items():
                if adjustment == "increase_confidence_threshold":
                    if detection_type == "persistence_mechanisms":
                        current_thresholds['persistence_confidence'] = min(0.95, 
                            current_thresholds.get('persistence_confidence', 0.85) + value)
                    elif detection_type == "exfiltration_patterns":
                        current_thresholds['exfiltration_rate_threshold'] = min(0.95,
                            current_thresholds.get('exfiltration_rate_threshold', 0.6) + value)
                    elif detection_type == "evasion_techniques":
                        current_thresholds['evasion_confidence'] = min(0.95,
                            current_thresholds.get('evasion_confidence', 0.5) + value)
            
            # Update thresholds
            new_patterns = {
                "thresholds": current_thresholds
            }
            
            return self.advanced_detector.update_patterns(new_patterns)
            
        except Exception as e:
            self.logger.error(f"Error applying false positive reduction update: {e}")
            return False
    
    def _apply_performance_optimization_update(self, update: SecurityUpdate) -> bool:
        """Apply performance optimization update"""
        try:
            changes = update.changes
            
            # Get current analysis windows
            current_windows = getattr(self.advanced_detector, '_analysis_windows', {})
            
            # Apply optimizations
            if changes.get("reduce_analysis_frequency"):
                reduction_factor = changes["reduce_analysis_frequency"]
                for window_name in current_windows:
                    if "analysis" in window_name:
                        current_windows[window_name] = int(current_windows[window_name] * (1 + reduction_factor))
            
            # Update analysis windows
            new_patterns = {
                "analysis_windows": current_windows
            }
            
            return self.advanced_detector.update_patterns(new_patterns)
            
        except Exception as e:
            self.logger.error(f"Error applying performance optimization update: {e}")
            return False
    
    def _cleanup_old_data(self):
        """Clean up old data to prevent memory leaks"""
        try:
            current_time = datetime.now(timezone.utc)
            cutoff_time = current_time - timedelta(hours=48)
            
            # Clean up old applied updates
            with self._update_lock:
                self._applied_updates = [
                    update for update in self._applied_updates
                    if update.timestamp >= cutoff_time
                ]
            
            # Clean up old performance metrics
            for metric_name in list(self._performance_metrics.keys()):
                if len(self._performance_metrics[metric_name]) > 1000:
                    self._performance_metrics[metric_name] = self._performance_metrics[metric_name][-500:]
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {e}")
    
    # Public interface methods
    
    def add_threat_intelligence_feed(self, feed: ThreatIntelligenceFeed) -> bool:
        """Add a new threat intelligence feed"""
        try:
            with self._feed_lock:
                self._intelligence_feeds[feed.feed_id] = feed
            
            self.logger.info(f"Added threat intelligence feed: {feed.feed_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding threat intelligence feed: {e}")
            return False
    
    def remove_threat_intelligence_feed(self, feed_id: str) -> bool:
        """Remove a threat intelligence feed"""
        try:
            with self._feed_lock:
                if feed_id in self._intelligence_feeds:
                    del self._intelligence_feeds[feed_id]
                    self.logger.info(f"Removed threat intelligence feed: {feed_id}")
                    return True
                else:
                    self.logger.warning(f"Threat intelligence feed not found: {feed_id}")
                    return False
            
        except Exception as e:
            self.logger.error(f"Error removing threat intelligence feed: {e}")
            return False
    
    def force_threat_intelligence_update(self, feed_id: Optional[str] = None) -> bool:
        """Force an immediate threat intelligence update"""
        try:
            with self._feed_lock:
                feeds_to_update = []
                
                if feed_id:
                    if feed_id in self._intelligence_feeds:
                        feeds_to_update.append(self._intelligence_feeds[feed_id])
                    else:
                        self.logger.error(f"Feed not found: {feed_id}")
                        return False
                else:
                    feeds_to_update = list(self._intelligence_feeds.values())
                
                for feed in feeds_to_update:
                    if feed.active:
                        intelligence_data = self._fetch_threat_intelligence(feed)
                        if intelligence_data:
                            updates = self._process_threat_intelligence(feed.feed_id, intelligence_data)
                            
                            with self._update_lock:
                                self._pending_updates.extend(updates)
                            
                            feed.last_update = datetime.now(timezone.utc)
                            
                            self.logger.info(f"Force updated feed {feed.feed_name}: {len(updates)} updates generated")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error forcing threat intelligence update: {e}")
            return False
    
    def get_pending_updates(self) -> List[Dict[str, Any]]:
        """Get list of pending security updates"""
        try:
            with self._update_lock:
                return [
                    {
                        "update_id": update.update_id,
                        "update_type": update.update_type,
                        "trigger": update.trigger.value,
                        "timestamp": update.timestamp.isoformat(),
                        "description": update.description,
                        "confidence": update.confidence,
                        "impact_assessment": update.impact_assessment,
                        "applied": update.applied
                    }
                    for update in self._pending_updates
                ]
        except Exception as e:
            self.logger.error(f"Error getting pending updates: {e}")
            return []
    
    def get_applied_updates(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get list of recently applied security updates"""
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
            
            with self._update_lock:
                return [
                    {
                        "update_id": update.update_id,
                        "update_type": update.update_type,
                        "trigger": update.trigger.value,
                        "timestamp": update.timestamp.isoformat(),
                        "description": update.description,
                        "confidence": update.confidence,
                        "success": update.success,
                        "impact_assessment": update.impact_assessment
                    }
                    for update in self._applied_updates
                    if update.timestamp >= cutoff_time
                ]
        except Exception as e:
            self.logger.error(f"Error getting applied updates: {e}")
            return []
    
    def rollback_update(self, update_id: str) -> bool:
        """Rollback a previously applied security update"""
        try:
            if not self._evolution_config['rollback_enabled']:
                self.logger.error("Rollback is disabled in configuration")
                return False
            
            # Find the update to rollback
            update_to_rollback = None
            with self._update_lock:
                for update in self._applied_updates:
                    if update.update_id == update_id and update.success:
                        update_to_rollback = update
                        break
            
            if not update_to_rollback:
                self.logger.error(f"Update not found or not successfully applied: {update_id}")
                return False
            
            if not update_to_rollback.rollback_data:
                self.logger.error(f"No rollback data available for update: {update_id}")
                return False
            
            # Perform rollback
            rollback_data = update_to_rollback.rollback_data
            previous_rules = rollback_data.get("previous_rules", {})
            
            # Restore previous detection rules
            success = self.advanced_detector.update_patterns(previous_rules)
            
            if success:
                self.logger.info(f"Successfully rolled back update: {update_id}")
                
                # Log the rollback
                self.crypto_logger.log_monitoring_event(
                    f"security_update_rollback_{update_to_rollback.update_type}",
                    "adaptive_security",
                    {
                        "update_id": update_id,
                        "update_type": update_to_rollback.update_type,
                        "rollback_timestamp": datetime.now(timezone.utc).isoformat()
                    },
                    1.0
                )
            else:
                self.logger.error(f"Failed to rollback update: {update_id}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error rolling back update {update_id}: {e}")
            return False
    
    def get_evolution_statistics(self) -> Dict[str, Any]:
        """Get adaptive security evolution statistics"""
        try:
            with self._update_lock:
                pending_count = len(self._pending_updates)
                applied_count = len(self._applied_updates)
                successful_count = sum(1 for u in self._applied_updates if u.success)
            
            with self._feed_lock:
                active_feeds = sum(1 for f in self._intelligence_feeds.values() if f.active)
                total_feeds = len(self._intelligence_feeds)
            
            return {
                "active": self._active,
                "pending_updates": pending_count,
                "applied_updates": applied_count,
                "successful_updates": successful_count,
                "success_rate": successful_count / applied_count if applied_count > 0 else 0.0,
                "active_intelligence_feeds": active_feeds,
                "total_intelligence_feeds": total_feeds,
                "detection_effectiveness": dict(self._detection_effectiveness),
                "false_positive_rates": dict(self._false_positive_rates),
                "evolution_config": self._evolution_config
            }
            
        except Exception as e:
            self.logger.error(f"Error getting evolution statistics: {e}")
            return {}
    
    def is_healthy(self) -> bool:
        """Check if the adaptive security evolution system is healthy"""
        try:
            # Basic health check - system initialized
            basic_health = True  # System is healthy if properly initialized
            
            # If active, also check thread health
            if self._active:
                return (basic_health and 
                       self._evolution_thread and 
                       self._evolution_thread.is_alive())
            
            return basic_health
        except Exception as e:
            self.logger.error(f"Error checking evolution system health: {e}")
            return False