"""
Emergency Protection Protocols

This module implements emergency system lockdown procedures, automated remediation
for persistent threats, and imminent compromise protection mechanisms as specified
in requirements 6.4 and 6.5.
"""

import logging
import threading
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import hashlib
import secrets

from .interfaces import (
    ThreatDetection, ThreatLevel, ComponentType, ResponseAction, InfrastructureEvent
)


class EmergencyLevel(Enum):
    """Emergency protection levels"""
    NONE = "none"
    ELEVATED = "elevated"
    HIGH = "high"
    CRITICAL = "critical"
    LOCKDOWN = "lockdown"


class ProtectionStatus(Enum):
    """Protection mechanism status"""
    INACTIVE = "inactive"
    ACTIVE = "active"
    TRIGGERED = "triggered"
    FAILED = "failed"


@dataclass
class EmergencyProtocol:
    """Emergency protection protocol configuration"""
    protocol_id: str
    name: str
    trigger_conditions: Dict[str, Any]
    protection_actions: List[str]
    activation_threshold: float
    cooldown_period: int  # seconds
    auto_restore: bool
    restore_conditions: Dict[str, Any]


@dataclass
class PersistentThreat:
    """Persistent threat tracking"""
    threat_id: str
    first_detected: datetime
    last_seen: datetime
    occurrence_count: int
    threat_indicators: Dict[str, Any]
    affected_components: List[ComponentType]
    remediation_attempts: List[str]
    persistence_score: float


@dataclass
class SystemLockdown:
    """System lockdown state"""
    lockdown_id: str
    initiated_time: datetime
    initiating_threat: str
    locked_components: List[ComponentType]
    lockdown_level: EmergencyLevel
    unlock_conditions: Dict[str, Any]
    emergency_contacts_notified: bool


class EmergencyProtectionSystem:
    """
    Implements emergency protection protocols for UBA infrastructure.
    
    Provides system lockdown, persistent threat remediation, and imminent
    compromise protection capabilities.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the emergency protection system.
        
        Args:
            config: Configuration dictionary containing protection parameters
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._lock = threading.RLock()
        
        # Emergency state tracking
        self._current_emergency_level = EmergencyLevel.NONE
        self._active_lockdowns: Dict[str, SystemLockdown] = {}
        self._persistent_threats: Dict[str, PersistentThreat] = {}
        self._protection_protocols: Dict[str, EmergencyProtocol] = {}
        self._emergency_history: List[Dict[str, Any]] = []
        
        # Protection thresholds
        self.emergency_thresholds = config.get('emergency_thresholds', {
            'imminent_compromise_score': 0.9,
            'persistent_threat_count': 3,
            'critical_component_compromise': 2,
            'lockdown_trigger_score': 0.95
        })
        
        # Lockdown configuration
        self.lockdown_config = config.get('lockdown_config', {
            'auto_lockdown_enabled': True,
            'lockdown_timeout_minutes': 60,
            'emergency_unlock_code': None,
            'preserve_audit_logs': True,
            'notify_emergency_contacts': True
        })
        
        # Remediation configuration
        self.remediation_config = config.get('remediation_config', {
            'max_remediation_attempts': 5,
            'remediation_cooldown_minutes': 10,
            'escalation_threshold': 3,
            'auto_remediation_enabled': True
        })
        
        # Initialize default protection protocols
        self._initialize_default_protocols()
        
        self.logger.info("Emergency Protection System initialized")
    
    def assess_emergency_level(self, threats: List[ThreatDetection]) -> EmergencyLevel:
        """
        Assess the current emergency level based on active threats.
        
        Args:
            threats: List of current threat detections
            
        Returns:
            EmergencyLevel: Current emergency level
        """
        if not threats:
            return EmergencyLevel.NONE
        
        # Calculate aggregate threat score
        total_score = 0.0
        critical_count = 0
        high_count = 0
        
        for threat in threats:
            if threat.severity == ThreatLevel.CRITICAL:
                critical_count += 1
                total_score += 1.0
            elif threat.severity == ThreatLevel.HIGH:
                high_count += 1
                total_score += 0.7
            elif threat.severity == ThreatLevel.MEDIUM:
                total_score += 0.4
            elif threat.severity == ThreatLevel.LOW:
                total_score += 0.1
        
        # Determine emergency level
        if (critical_count >= self.emergency_thresholds['critical_component_compromise'] or
            total_score >= self.emergency_thresholds['lockdown_trigger_score']):
            return EmergencyLevel.LOCKDOWN
        elif critical_count > 0 or total_score >= self.emergency_thresholds['imminent_compromise_score']:
            return EmergencyLevel.CRITICAL
        elif high_count >= 2 or total_score >= 0.7:
            return EmergencyLevel.HIGH
        elif high_count > 0 or total_score >= 0.4:
            return EmergencyLevel.ELEVATED
        else:
            return EmergencyLevel.NONE
    
    def initiate_emergency_lockdown(self, triggering_threat: ThreatDetection) -> SystemLockdown:
        """
        Initiate emergency system lockdown.
        
        Args:
            triggering_threat: The threat that triggered the lockdown
            
        Returns:
            SystemLockdown: Details of the initiated lockdown
        """
        with self._lock:
            lockdown_id = self._generate_lockdown_id()
            
            try:
                # Determine components to lock down
                components_to_lock = self._determine_lockdown_scope(triggering_threat)
                
                # Create lockdown configuration
                lockdown = SystemLockdown(
                    lockdown_id=lockdown_id,
                    initiated_time=datetime.now(),
                    initiating_threat=triggering_threat.detection_id,
                    locked_components=components_to_lock,
                    lockdown_level=EmergencyLevel.LOCKDOWN,
                    unlock_conditions=self._create_unlock_conditions(triggering_threat),
                    emergency_contacts_notified=False
                )
                
                # Execute lockdown procedures
                success = self._execute_lockdown_procedures(lockdown)
                
                if success:
                    self._active_lockdowns[lockdown_id] = lockdown
                    self._current_emergency_level = EmergencyLevel.LOCKDOWN
                    
                    # Notify emergency contacts
                    if self.lockdown_config['notify_emergency_contacts']:
                        self._notify_emergency_contacts(lockdown)
                        lockdown.emergency_contacts_notified = True
                    
                    self.logger.critical(f"Emergency lockdown {lockdown_id} initiated due to threat {triggering_threat.detection_id}")
                else:
                    self.logger.error(f"Failed to execute lockdown procedures for {lockdown_id}")
                
                return lockdown
                
            except Exception as e:
                self.logger.error(f"Error initiating emergency lockdown: {str(e)}")
                raise
    
    def remediate_persistent_threat(self, threat: ThreatDetection) -> bool:
        """
        Implement automated remediation for persistent threats.
        
        Args:
            threat: The persistent threat to remediate
            
        Returns:
            bool: True if remediation was successful
        """
        with self._lock:
            try:
                # Track or update persistent threat
                persistent_threat = self._track_persistent_threat(threat)
                
                # Check if we've exceeded remediation attempts
                if len(persistent_threat.remediation_attempts) >= self.remediation_config['max_remediation_attempts']:
                    self.logger.warning(f"Max remediation attempts reached for threat {threat.detection_id}")
                    return self._escalate_persistent_threat(persistent_threat)
                
                # Determine remediation strategy
                remediation_strategy = self._determine_remediation_strategy(persistent_threat)
                
                # Execute remediation
                success = self._execute_remediation_strategy(remediation_strategy, persistent_threat)
                
                if success:
                    persistent_threat.remediation_attempts.append(f"{datetime.now()}: {remediation_strategy}")
                    self.logger.info(f"Successfully remediated persistent threat {threat.detection_id}")
                else:
                    self.logger.warning(f"Remediation failed for persistent threat {threat.detection_id}")
                
                return success
                
            except Exception as e:
                self.logger.error(f"Error remediating persistent threat {threat.detection_id}: {str(e)}")
                return False
    
    def protect_against_imminent_compromise(self, threat_indicators: Dict[str, Any]) -> bool:
        """
        Implement protection mechanisms against imminent system compromise.
        
        Args:
            threat_indicators: Indicators suggesting imminent compromise
            
        Returns:
            bool: True if protection measures were successfully implemented
        """
        try:
            # Calculate imminent compromise score
            compromise_score = self._calculate_compromise_score(threat_indicators)
            
            if compromise_score < self.emergency_thresholds['imminent_compromise_score']:
                return True  # No immediate action needed
            
            # Implement protective measures
            protection_measures = []
            
            # 1. Isolate critical components
            if compromise_score >= 0.9:
                protection_measures.append('isolate_critical_components')
            
            # 2. Rotate all credentials
            if 'credential_indicators' in threat_indicators:
                protection_measures.append('emergency_credential_rotation')
            
            # 3. Enable enhanced monitoring
            protection_measures.append('enable_enhanced_monitoring')
            
            # 4. Prepare for potential lockdown
            if compromise_score >= self.emergency_thresholds['lockdown_trigger_score']:
                protection_measures.append('prepare_emergency_lockdown')
            
            # Execute protection measures
            success = self._execute_protection_measures(protection_measures, threat_indicators)
            
            if success:
                self.logger.warning(f"Imminent compromise protection activated (score: {compromise_score})")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error implementing imminent compromise protection: {str(e)}")
            return False
    
    def check_unlock_conditions(self, lockdown_id: str) -> bool:
        """
        Check if conditions are met to unlock a system lockdown.
        
        Args:
            lockdown_id: ID of the lockdown to check
            
        Returns:
            bool: True if unlock conditions are met
        """
        with self._lock:
            lockdown = self._active_lockdowns.get(lockdown_id)
            if not lockdown:
                return False
            
            try:
                # Check time-based unlock conditions
                if 'timeout_minutes' in lockdown.unlock_conditions:
                    timeout = timedelta(minutes=lockdown.unlock_conditions['timeout_minutes'])
                    if datetime.now() - lockdown.initiated_time >= timeout:
                        return True
                
                # Check threat resolution conditions
                if 'threat_resolved' in lockdown.unlock_conditions:
                    # In a real implementation, this would check if the original threat is resolved
                    return False  # Placeholder
                
                # Check manual unlock conditions
                if 'manual_unlock' in lockdown.unlock_conditions:
                    return lockdown.unlock_conditions['manual_unlock']
                
                return False
                
            except Exception as e:
                self.logger.error(f"Error checking unlock conditions for {lockdown_id}: {str(e)}")
                return False
    
    def unlock_system(self, lockdown_id: str, unlock_code: Optional[str] = None) -> bool:
        """
        Unlock a system lockdown.
        
        Args:
            lockdown_id: ID of the lockdown to unlock
            unlock_code: Optional emergency unlock code
            
        Returns:
            bool: True if unlock was successful
        """
        with self._lock:
            try:
                lockdown = self._active_lockdowns.get(lockdown_id)
                if not lockdown:
                    self.logger.error(f"Lockdown {lockdown_id} not found")
                    return False
                
                # Verify unlock authorization
                if unlock_code:
                    if not self._verify_unlock_code(unlock_code):
                        self.logger.error("Invalid emergency unlock code")
                        return False
                elif not self.check_unlock_conditions(lockdown_id):
                    self.logger.error(f"Unlock conditions not met for {lockdown_id}")
                    return False
                
                # Execute unlock procedures
                success = self._execute_unlock_procedures(lockdown)
                
                if success:
                    del self._active_lockdowns[lockdown_id]
                    
                    # Update emergency level if no other lockdowns
                    if not self._active_lockdowns:
                        self._current_emergency_level = EmergencyLevel.NONE
                    
                    self.logger.info(f"System lockdown {lockdown_id} successfully unlocked")
                
                return success
                
            except Exception as e:
                self.logger.error(f"Error unlocking system {lockdown_id}: {str(e)}")
                return False
    
    def get_emergency_status(self) -> Dict[str, Any]:
        """
        Get current emergency protection status.
        
        Returns:
            Dict containing emergency status information
        """
        with self._lock:
            return {
                'current_emergency_level': self._current_emergency_level.value,
                'active_lockdowns': {k: asdict(v) for k, v in self._active_lockdowns.items()},
                'persistent_threats': {k: asdict(v) for k, v in self._persistent_threats.items()},
                'protection_protocols_count': len(self._protection_protocols),
                'emergency_history_count': len(self._emergency_history)
            }
    
    def _initialize_default_protocols(self):
        """Initialize default emergency protection protocols."""
        # Critical infrastructure compromise protocol
        critical_protocol = EmergencyProtocol(
            protocol_id="critical_infrastructure_compromise",
            name="Critical Infrastructure Compromise Response",
            trigger_conditions={
                'threat_severity': ThreatLevel.CRITICAL,
                'affected_components': [ComponentType.DATABASE, ComponentType.USER_ACCOUNT]
            },
            protection_actions=['isolate_components', 'rotate_credentials', 'enable_shadow_monitoring'],
            activation_threshold=0.8,
            cooldown_period=300,
            auto_restore=False,
            restore_conditions={'manual_approval': True}
        )
        
        self._protection_protocols[critical_protocol.protocol_id] = critical_protocol
    
    def _track_persistent_threat(self, threat: ThreatDetection) -> PersistentThreat:
        """Track or update a persistent threat."""
        threat_key = f"{threat.threat_type}_{hash(str(threat.attack_indicators))}"
        
        if threat_key in self._persistent_threats:
            persistent = self._persistent_threats[threat_key]
            persistent.last_seen = datetime.now()
            persistent.occurrence_count += 1
            persistent.persistence_score = min(1.0, persistent.occurrence_count * 0.2)
        else:
            persistent = PersistentThreat(
                threat_id=threat_key,
                first_detected=datetime.now(),
                last_seen=datetime.now(),
                occurrence_count=1,
                threat_indicators=threat.attack_indicators,
                affected_components=threat.affected_components,
                remediation_attempts=[],
                persistence_score=0.2
            )
            self._persistent_threats[threat_key] = persistent
        
        return persistent
    
    def _determine_lockdown_scope(self, threat: ThreatDetection) -> List[ComponentType]:
        """Determine which components to include in lockdown."""
        # Start with affected components
        components = list(threat.affected_components)
        
        # Add critical components if threat is severe enough
        if threat.severity == ThreatLevel.CRITICAL:
            critical_components = [ComponentType.DATABASE, ComponentType.USER_ACCOUNT]
            for comp in critical_components:
                if comp not in components:
                    components.append(comp)
        
        return components
    
    def _create_unlock_conditions(self, threat: ThreatDetection) -> Dict[str, Any]:
        """Create unlock conditions based on threat characteristics."""
        conditions = {
            'timeout_minutes': self.lockdown_config['lockdown_timeout_minutes'],
            'threat_resolved': False,
            'manual_unlock': False
        }
        
        # Add specific conditions based on threat type
        if 'credential_compromise' in threat.attack_indicators:
            conditions['credentials_rotated'] = False
        
        return conditions
    
    def _execute_lockdown_procedures(self, lockdown: SystemLockdown) -> bool:
        """Execute the actual lockdown procedures."""
        try:
            for component in lockdown.locked_components:
                # In a real implementation, this would actually lock down components
                self.logger.info(f"Locking down component: {component.value}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error executing lockdown procedures: {str(e)}")
            return False
    
    def _execute_unlock_procedures(self, lockdown: SystemLockdown) -> bool:
        """Execute the unlock procedures."""
        try:
            for component in lockdown.locked_components:
                # In a real implementation, this would restore component access
                self.logger.info(f"Unlocking component: {component.value}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error executing unlock procedures: {str(e)}")
            return False
    
    def _determine_remediation_strategy(self, persistent_threat: PersistentThreat) -> str:
        """Determine the appropriate remediation strategy."""
        if persistent_threat.persistence_score >= 0.8:
            return "aggressive_remediation"
        elif persistent_threat.persistence_score >= 0.6:
            return "enhanced_remediation"
        else:
            return "standard_remediation"
    
    def _execute_remediation_strategy(self, strategy: str, threat: PersistentThreat) -> bool:
        """Execute the remediation strategy."""
        # In a real implementation, this would execute specific remediation actions
        self.logger.info(f"Executing {strategy} for persistent threat {threat.threat_id}")
        return True
    
    def _escalate_persistent_threat(self, threat: PersistentThreat) -> bool:
        """Escalate a persistent threat that couldn't be remediated."""
        self.logger.critical(f"Escalating persistent threat {threat.threat_id} - max remediation attempts exceeded")
        # In a real implementation, this would trigger manual intervention
        return False
    
    def _calculate_compromise_score(self, indicators: Dict[str, Any]) -> float:
        """Calculate imminent compromise score based on indicators."""
        score = 0.0
        
        # Weight different indicators
        if 'admin_access_attempts' in indicators:
            score += min(0.3, indicators['admin_access_attempts'] * 0.1)
        
        if 'credential_indicators' in indicators:
            score += 0.4
        
        if 'system_file_modifications' in indicators:
            score += 0.3
        
        if 'network_anomalies' in indicators:
            score += 0.2
        
        return min(1.0, score)
    
    def _execute_protection_measures(self, measures: List[str], indicators: Dict[str, Any]) -> bool:
        """Execute protection measures against imminent compromise."""
        try:
            for measure in measures:
                self.logger.info(f"Executing protection measure: {measure}")
                # In a real implementation, each measure would have specific actions
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error executing protection measures: {str(e)}")
            return False
    
    def _notify_emergency_contacts(self, lockdown: SystemLockdown):
        """Notify emergency contacts about system lockdown."""
        # In a real implementation, this would send notifications
        self.logger.critical(f"Emergency contacts notified about lockdown {lockdown.lockdown_id}")
    
    def _verify_unlock_code(self, unlock_code: str) -> bool:
        """Verify emergency unlock code."""
        expected_code = self.lockdown_config.get('emergency_unlock_code')
        return expected_code and unlock_code == expected_code
    
    def _generate_lockdown_id(self) -> str:
        """Generate a unique lockdown ID."""
        timestamp = str(int(time.time() * 1000))
        random_part = secrets.token_hex(4)
        return f"lockdown_{timestamp}_{random_part}"