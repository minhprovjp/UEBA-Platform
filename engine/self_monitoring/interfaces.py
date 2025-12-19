"""
Base interfaces for the UBA Self-Monitoring System

These interfaces define the contracts for monitoring, detection, response, and integrity
validation components in the self-monitoring architecture.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
from enum import Enum


class ThreatLevel(Enum):
    """Threat severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ComponentType(Enum):
    """UBA infrastructure component types"""
    DATABASE = "uba_db"
    USER_ACCOUNT = "uba_user"
    PERFORMANCE_SCHEMA = "performance_schema"
    AUDIT_LOG = "uba_persistent_log"
    MONITORING_SERVICE = "monitoring_service"


@dataclass
class InfrastructureEvent:
    """Infrastructure event data model"""
    event_id: str
    timestamp: datetime
    event_type: str
    source_ip: str
    user_account: str
    target_component: ComponentType
    action_details: Dict[str, Any]
    risk_score: float
    integrity_hash: str


@dataclass
class ThreatDetection:
    """Threat detection result data model"""
    detection_id: str
    timestamp: datetime
    threat_type: str
    severity: ThreatLevel
    affected_components: List[ComponentType]
    attack_indicators: Dict[str, Any]
    confidence_score: float
    response_actions: List[str]
    evidence_chain: List[str]


@dataclass
class ResponseAction:
    """Automated response action data model"""
    action_id: str
    timestamp: datetime
    action_type: str
    target: str
    parameters: Dict[str, Any]
    success: bool
    error_message: Optional[str] = None


class MonitoringInterface(ABC):
    """Base interface for monitoring components"""
    
    @abstractmethod
    def start_monitoring(self) -> bool:
        """Start the monitoring process"""
        pass
    
    @abstractmethod
    def stop_monitoring(self) -> bool:
        """Stop the monitoring process"""
        pass
    
    @abstractmethod
    def get_events(self, start_time: datetime, end_time: datetime) -> List[InfrastructureEvent]:
        """Retrieve events within a time range"""
        pass
    
    @abstractmethod
    def is_healthy(self) -> bool:
        """Check if the monitoring component is healthy"""
        pass


class DetectionInterface(ABC):
    """Base interface for threat detection components"""
    
    @abstractmethod
    def analyze_events(self, events: List[InfrastructureEvent]) -> List[ThreatDetection]:
        """Analyze events for threats"""
        pass
    
    @abstractmethod
    def update_patterns(self, new_patterns: Dict[str, Any]) -> bool:
        """Update threat detection patterns"""
        pass
    
    @abstractmethod
    def get_detection_rules(self) -> Dict[str, Any]:
        """Get current detection rules"""
        pass


class ResponseInterface(ABC):
    """Base interface for automated response components"""
    
    @abstractmethod
    def execute_response(self, threat: ThreatDetection) -> ResponseAction:
        """Execute automated response to a threat"""
        pass
    
    @abstractmethod
    def validate_action(self, action: ResponseAction) -> bool:
        """Validate if an action can be safely executed"""
        pass
    
    @abstractmethod
    def rollback_action(self, action_id: str) -> bool:
        """Rollback a previously executed action"""
        pass


class IntegrityInterface(ABC):
    """Base interface for data integrity validation"""
    
    @abstractmethod
    def create_checksum(self, data: Any) -> str:
        """Create cryptographic checksum for data"""
        pass
    
    @abstractmethod
    def verify_integrity(self, data: Any, checksum: str) -> bool:
        """Verify data integrity using checksum"""
        pass
    
    @abstractmethod
    def detect_tampering(self, data_id: str) -> Tuple[bool, Optional[str]]:
        """Detect if data has been tampered with"""
        pass
    
    @abstractmethod
    def create_audit_trail(self, event: InfrastructureEvent) -> str:
        """Create tamper-evident audit trail entry"""
        pass


class ShadowMonitoringInterface(ABC):
    """Interface for shadow monitoring components"""
    
    @abstractmethod
    def monitor_primary_system(self) -> bool:
        """Monitor the health of primary monitoring systems"""
        pass
    
    @abstractmethod
    def detect_primary_compromise(self) -> List[ThreatDetection]:
        """Detect if primary monitoring has been compromised"""
        pass
    
    @abstractmethod
    def activate_backup_monitoring(self) -> bool:
        """Activate backup monitoring when primary fails"""
        pass


class ConfigurationInterface(ABC):
    """Interface for configuration management"""
    
    @abstractmethod
    def load_config(self) -> Dict[str, Any]:
        """Load monitoring configuration"""
        pass
    
    @abstractmethod
    def save_config(self, config: Dict[str, Any]) -> bool:
        """Save monitoring configuration"""
        pass
    
    @abstractmethod
    def validate_config(self, config: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate configuration parameters"""
        pass
    
    @abstractmethod
    def get_secure_defaults(self) -> Dict[str, Any]:
        """Get secure default configuration"""
        pass