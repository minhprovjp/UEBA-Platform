"""
UBA Self-Monitoring System

This module provides comprehensive monitoring capabilities for the UBA infrastructure itself,
including detection of attacks against the monitoring system components.
"""

__version__ = "1.0.0"
__author__ = "IA_03"

# Core interfaces
from .interfaces import (
    MonitoringInterface,
    DetectionInterface,
    ResponseInterface,
    IntegrityInterface,
    ShadowMonitoringInterface,
    ConfigurationInterface,
    InfrastructureEvent,
    ThreatDetection,
    ResponseAction,
    ThreatLevel,
    ComponentType
)

# Core components
from .config_manager import SelfMonitoringConfig
from .crypto_logger import CryptoLogger
from .infrastructure_monitor import InfrastructureMonitor
from .shadow_monitor import ShadowMonitor
from .behavioral_anomaly_detector import BehavioralAnomalyDetector
from .attack_pattern_recognition import AttackPatternRecognitionEngine
from .advanced_threat_detector import AdvancedThreatDetector
from .threat_response_orchestrator import ThreatResponseOrchestrator
from .integrity_validator import IntegrityValidator
from .unified_dashboard import UnifiedDashboard
from .alert_manager import AlertManager
from .adaptive_security_evolution import AdaptiveSecurityEvolution
from .coverage_extension import CoverageExtensionSystem
from .emergency_protection import EmergencyProtectionSystem

# Integration components
from .integration_orchestrator import IntegrationOrchestrator
from .system_startup import SystemStartup

__all__ = [
    # Interfaces
    'MonitoringInterface',
    'DetectionInterface', 
    'ResponseInterface',
    'IntegrityInterface',
    'ShadowMonitoringInterface',
    'ConfigurationInterface',
    'InfrastructureEvent',
    'ThreatDetection',
    'ResponseAction',
    'ThreatLevel',
    'ComponentType',
    
    # Core components
    'SelfMonitoringConfig',
    'CryptoLogger',
    'InfrastructureMonitor',
    'ShadowMonitor',
    'BehavioralAnomalyDetector',
    'AttackPatternRecognitionEngine',
    'AdvancedThreatDetector',
    'ThreatResponseOrchestrator',
    'IntegrityValidator',
    'UnifiedDashboard',
    'AlertManager',
    'AdaptiveSecurityEvolution',
    'CoverageExtensionSystem',
    'EmergencyProtectionSystem',
    
    # Integration components
    'IntegrationOrchestrator',
    'SystemStartup'
]