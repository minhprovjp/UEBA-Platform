"""
Comprehensive Monitoring and Alerting System for UBA Self-Monitoring

This module integrates all monitoring components to provide a unified monitoring
and alerting system with real-time security status visualization, cross-component
event correlation, and comprehensive alert management.
"""

import json
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
from enum import Enum

from .interfaces import (
    InfrastructureEvent, ThreatDetection, ThreatLevel, ComponentType,
    MonitoringInterface, DetectionInterface
)
from .alert_manager import AlertManager, Alert, AlertPriority, NotificationRule, EscalationRule
from .unified_dashboard import UnifiedDashboard, SecurityStatus, EventCorrelation
from .coverage_extension import CoverageExtensionSystem, BlindSpot, CoverageMetrics


class SystemStatus(Enum):
    """Overall system status"""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    OFFLINE = "offline"


@dataclass
class MonitoringSystemHealth:
    """Monitoring system health data model"""
    timestamp: datetime
    overall_status: SystemStatus
    active_monitors: int
    total_monitors: int
    active_detectors: int
    total_detectors: int
    alert_manager_healthy: bool
    dashboard_healthy: bool
    coverage_system_healthy: bool
    last_event_time: Optional[datetime]
    system_uptime: timedelta


class MonitoringAlertingSystem:
    """
    Comprehensive monitoring and alerting system that integrates all UBA
    self-monitoring components into a unified security oversight platform.
    """
    
    def __init__(self, monitoring_components: List[MonitoringInterface],
                 detection_components: List[DetectionInterface],
                 config: Dict[str, Any] = None):
        """
        Initialize the comprehensive monitoring and alerting system.
        
        Args:
            monitoring_components: List of monitoring components
            detection_components: List of detection components
            config: System configuration dictionary
        """
        self.monitoring_components = monitoring_components
        self.detection_components = detection_components
        self.config = config or {}
        
        # Initialize subsystems
        self.alert_manager = AlertManager(self.config.get('alert_manager', {}))
        self.dashboard = UnifiedDashboard(monitoring_components, detection_components)
        self.coverage_system = CoverageExtensionSystem(monitoring_components, detection_components)
        
        # System state
        self.system_start_time = datetime.now()
        self.is_running = False
        self._main_thread = None
        self._lock = threading.RLock()
        
        # Event processing
        self.event_queue = deque(maxlen=10000)
        self.processed_events = set()  # Track processed event IDs
        
        # System health tracking
        self.health_history = deque(maxlen=1000)
        self.last_health_check = datetime.now()
        
        # Performance metrics
        self.metrics = {
            'events_processed': 0,
            'threats_detected': 0,
            'alerts_generated': 0,
            'blind_spots_identified': 0,
            'coverage_expansions': 0
        }
        
        self.logger = logging.getLogger(__name__)
    
    def start_monitoring_system(self) -> bool:
        """
        Start the comprehensive monitoring and alerting system.
        
        Returns:
            bool: True if system started successfully
        """
        try:
            self.logger.info("Starting comprehensive monitoring and alerting system...")
            
            # Start subsystems
            if not self.alert_manager.start_alert_manager():
                raise Exception("Failed to start alert manager")
            
            if not self.dashboard.start_dashboard():
                raise Exception("Failed to start unified dashboard")
            
            if not self.coverage_system.start_coverage_system():
                raise Exception("Failed to start coverage extension system")
            
            # Start main processing thread
            self.is_running = True
            self._main_thread = threading.Thread(target=self._main_processing_loop, daemon=True)
            self._main_thread.start()
            
            # Configure default alert rules
            self._configure_default_alert_rules()
            
            self.logger.info("Comprehensive monitoring and alerting system started successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start monitoring system: {e}")
            self.stop_monitoring_system()
            return False
    
    def stop_monitoring_system(self) -> bool:
        """
        Stop the comprehensive monitoring and alerting system.
        
        Returns:
            bool: True if system stopped successfully
        """
        try:
            self.logger.info("Stopping comprehensive monitoring and alerting system...")
            
            self.is_running = False
            
            # Stop main thread
            if self._main_thread:
                self._main_thread.join(timeout=10.0)
            
            # Stop subsystems
            self.coverage_system.stop_coverage_system()
            self.dashboard.stop_dashboard()
            self.alert_manager.stop_alert_manager()
            
            self.logger.info("Comprehensive monitoring and alerting system stopped successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to stop monitoring system: {e}")
            return False
    
    def get_system_health(self) -> MonitoringSystemHealth:
        """
        Get comprehensive system health status.
        
        Returns:
            MonitoringSystemHealth: Current system health status
        """
        current_time = datetime.now()
        
        # Check component health
        active_monitors = sum(1 for comp in self.monitoring_components if comp.is_healthy())
        active_detectors = sum(1 for comp in self.detection_components if comp.is_healthy())
        
        # Determine overall status
        monitor_ratio = active_monitors / len(self.monitoring_components) if self.monitoring_components else 1.0
        detector_ratio = active_detectors / len(self.detection_components) if self.detection_components else 1.0
        
        if monitor_ratio >= 0.8 and detector_ratio >= 0.8:
            overall_status = SystemStatus.HEALTHY
        elif monitor_ratio >= 0.5 and detector_ratio >= 0.5:
            overall_status = SystemStatus.WARNING
        elif monitor_ratio > 0 or detector_ratio > 0:
            overall_status = SystemStatus.CRITICAL
        else:
            overall_status = SystemStatus.OFFLINE
        
        # Get last event time
        last_event_time = None
        if self.event_queue:
            last_event_time = max(event.timestamp for event in self.event_queue)
        
        health = MonitoringSystemHealth(
            timestamp=current_time,
            overall_status=overall_status,
            active_monitors=active_monitors,
            total_monitors=len(self.monitoring_components),
            active_detectors=active_detectors,
            total_detectors=len(self.detection_components),
            alert_manager_healthy=True,  # Assume healthy if running
            dashboard_healthy=True,      # Assume healthy if running
            coverage_system_healthy=True, # Assume healthy if running
            last_event_time=last_event_time,
            system_uptime=current_time - self.system_start_time
        )
        
        # Store health history
        with self._lock:
            self.health_history.append(health)
            self.last_health_check = current_time
        
        return health
    
    def get_comprehensive_status(self) -> Dict[str, Any]:
        """
        Get comprehensive system status including all subsystems.
        
        Returns:
            Dict[str, Any]: Complete system status
        """
        system_health = self.get_system_health()
        security_status = self.dashboard.get_security_status()
        alert_stats = self.alert_manager.get_alert_statistics()
        coverage_metrics = self.coverage_system.get_coverage_metrics()
        
        return {
            'timestamp': datetime.now().isoformat(),
            'system_health': asdict(system_health),
            'security_status': asdict(security_status),
            'alert_statistics': alert_stats,
            'coverage_overview': {
                'total_components': len(coverage_metrics),
                'average_coverage': sum(m.coverage_percentage for m in coverage_metrics.values()) / len(coverage_metrics) if coverage_metrics else 0.0,
                'blind_spots': len(self.coverage_system.blind_spots)
            },
            'performance_metrics': dict(self.metrics),
            'uptime_seconds': (datetime.now() - self.system_start_time).total_seconds()
        }
    
    def process_infrastructure_event(self, event: InfrastructureEvent) -> bool:
        """
        Process a new infrastructure event through the monitoring pipeline.
        
        Args:
            event: Infrastructure event to process
            
        Returns:
            bool: True if event was processed successfully
        """
        try:
            # Avoid duplicate processing
            if event.event_id in self.processed_events:
                return True
            
            with self._lock:
                self.event_queue.append(event)
                self.processed_events.add(event.event_id)
                self.metrics['events_processed'] += 1
            
            # Log infrastructure interaction for coverage analysis
            if hasattr(event, 'source_component'):
                self.coverage_system.log_infrastructure_interaction(
                    source=event.source_component,
                    target=event.target_component,
                    interaction_type=event.event_type,
                    details=event.action_details
                )
            
            self.logger.debug(f"Processed infrastructure event {event.event_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to process infrastructure event {event.event_id}: {e}")
            return False
    
    def generate_comprehensive_report(self, time_range: timedelta = None) -> Dict[str, Any]:
        """
        Generate a comprehensive monitoring and security report.
        
        Args:
            time_range: Time range for the report (default: last 24 hours)
            
        Returns:
            Dict[str, Any]: Comprehensive monitoring report
        """
        if time_range is None:
            time_range = timedelta(hours=24)
        
        cutoff_time = datetime.now() - time_range
        
        # Collect data from all subsystems
        system_status = self.get_comprehensive_status()
        
        # Get recent events
        recent_events = [event for event in self.event_queue if event.timestamp > cutoff_time]
        
        # Get correlations
        correlations = self.dashboard.get_event_correlations(time_range)
        
        # Get active alerts
        active_alerts = self.alert_manager.get_active_alerts()
        
        # Get coverage report
        coverage_report = self.coverage_system.generate_coverage_report()
        
        report = {
            'report_id': f"comprehensive_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'generated_at': datetime.now().isoformat(),
            'time_range': {
                'start': cutoff_time.isoformat(),
                'end': datetime.now().isoformat(),
                'duration_hours': time_range.total_seconds() / 3600
            },
            'system_status': system_status,
            'event_summary': {
                'total_events': len(recent_events),
                'events_by_component': self._summarize_events_by_component(recent_events),
                'high_risk_events': len([e for e in recent_events if e.risk_score > 0.7])
            },
            'threat_analysis': {
                'active_threats': len(active_alerts),
                'correlations_found': len(correlations),
                'threat_timeline': self._build_threat_timeline(recent_events, correlations)
            },
            'coverage_analysis': coverage_report,
            'recommendations': self._generate_recommendations(system_status, recent_events, active_alerts)
        }
        
        return report
    
    def acknowledge_system_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """
        Acknowledge a system alert.
        
        Args:
            alert_id: ID of the alert to acknowledge
            acknowledged_by: User acknowledging the alert
            
        Returns:
            bool: True if acknowledged successfully
        """
        return self.alert_manager.acknowledge_alert(alert_id, acknowledged_by)
    
    def resolve_system_alert(self, alert_id: str, resolved_by: str, notes: str = "") -> bool:
        """
        Resolve a system alert.
        
        Args:
            alert_id: ID of the alert to resolve
            resolved_by: User resolving the alert
            notes: Resolution notes
            
        Returns:
            bool: True if resolved successfully
        """
        return self.alert_manager.resolve_alert(alert_id, resolved_by, notes)
    
    def expand_monitoring_coverage(self, blind_spot_id: str) -> bool:
        """
        Manually trigger monitoring coverage expansion for a specific blind spot.
        
        Args:
            blind_spot_id: ID of the blind spot to address
            
        Returns:
            bool: True if expansion was successful
        """
        if blind_spot_id in self.coverage_system.blind_spots:
            blind_spot = self.coverage_system.blind_spots[blind_spot_id]
            success = self.coverage_system.expand_monitoring_coverage(blind_spot)
            
            if success:
                self.metrics['coverage_expansions'] += 1
            
            return success
        
        return False
    
    def _main_processing_loop(self):
        """Main processing loop for the monitoring system."""
        while self.is_running:
            try:
                self._process_event_queue()
                self._analyze_threats()
                self._check_system_health()
                self._process_coverage_analysis()
                
                time.sleep(10)  # Process every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Error in main processing loop: {e}")
                time.sleep(30)  # Wait longer on error
    
    def _process_event_queue(self):
        """Process events from the event queue."""
        # Get recent events for analysis
        recent_events = list(self.event_queue)[-100:]  # Last 100 events
        
        if not recent_events:
            return
        
        # Run threat detection on recent events
        for detector in self.detection_components:
            try:
                if detector.is_healthy():
                    threats = detector.analyze_events(recent_events)
                    
                    for threat in threats:
                        self._handle_threat_detection(threat, recent_events)
                        
            except Exception as e:
                self.logger.warning(f"Threat detection failed: {e}")
    
    def _analyze_threats(self):
        """Analyze detected threats and generate alerts."""
        # This is handled in _handle_threat_detection
        pass
    
    def _handle_threat_detection(self, threat: ThreatDetection, related_events: List[InfrastructureEvent]):
        """
        Handle a detected threat by creating alerts and taking appropriate actions.
        
        Args:
            threat: Detected threat
            related_events: Events related to the threat
        """
        try:
            # Create alert for the threat
            alert = self.alert_manager.create_alert(threat, related_events)
            self.metrics['threats_detected'] += 1
            self.metrics['alerts_generated'] += 1
            
            # Log the threat detection
            self.logger.warning(f"Threat detected: {threat.threat_type} on {threat.affected_components} "
                              f"(confidence: {threat.confidence_score:.2f})")
            
            # If critical threat, perform additional actions
            if threat.severity == ThreatLevel.CRITICAL:
                self._handle_critical_threat(threat, alert)
                
        except Exception as e:
            self.logger.error(f"Failed to handle threat detection: {e}")
    
    def _handle_critical_threat(self, threat: ThreatDetection, alert: Alert):
        """
        Handle critical threats with immediate response actions.
        
        Args:
            threat: Critical threat detection
            alert: Generated alert for the threat
        """
        self.logger.critical(f"CRITICAL THREAT DETECTED: {threat.threat_type}")
        
        # Immediate actions for critical threats could include:
        # - Emergency notifications
        # - Automatic system isolation
        # - Backup system activation
        # - Forensic data collection
        
        # For now, just log the critical threat
        critical_log = {
            'timestamp': datetime.now().isoformat(),
            'threat_id': threat.detection_id,
            'threat_type': threat.threat_type,
            'affected_components': [c.value for c in threat.affected_components],
            'confidence_score': threat.confidence_score,
            'alert_id': alert.alert_id,
            'response_actions': threat.response_actions
        }
        
        self.logger.critical(f"Critical threat log: {json.dumps(critical_log)}")
    
    def _check_system_health(self):
        """Check overall system health and generate alerts if needed."""
        health = self.get_system_health()
        
        # Generate alerts for system health issues
        if health.overall_status == SystemStatus.CRITICAL:
            self.logger.error("System health is CRITICAL - multiple components failing")
        elif health.overall_status == SystemStatus.WARNING:
            self.logger.warning("System health is WARNING - some components degraded")
        
        # Check for stale events (no events in last hour)
        if health.last_event_time and datetime.now() - health.last_event_time > timedelta(hours=1):
            self.logger.warning("No events received in the last hour - possible monitoring issue")
    
    def _process_coverage_analysis(self):
        """Process coverage analysis and handle blind spots."""
        try:
            # Identify new blind spots
            blind_spots = self.coverage_system.identify_blind_spots()
            
            for blind_spot in blind_spots:
                self.metrics['blind_spots_identified'] += 1
                
                # Log blind spot identification
                self.logger.info(f"Blind spot identified: {blind_spot.description} "
                               f"(risk: {blind_spot.risk_level})")
                
                # Auto-expand coverage for high-risk blind spots
                if blind_spot.risk_level in ["High", "Critical"]:
                    if self.coverage_system.expand_monitoring_coverage(blind_spot):
                        self.metrics['coverage_expansions'] += 1
                        self.logger.info(f"Auto-expanded coverage for blind spot {blind_spot.blind_spot_id}")
                        
        except Exception as e:
            self.logger.error(f"Coverage analysis failed: {e}")
    
    def _configure_default_alert_rules(self):
        """Configure default alert and escalation rules."""
        # Critical threat notification rule
        critical_rule = NotificationRule(
            rule_id="critical_threats",
            name="Critical Threat Notifications",
            priority_threshold=AlertPriority.CRITICAL,
            channels=[],  # Would be configured based on available channels
            recipients=["security-team@company.com"],
            conditions={}
        )
        self.alert_manager.add_notification_rule(critical_rule)
        
        # High priority escalation rule
        escalation_rule = EscalationRule(
            rule_id="high_priority_escalation",
            name="High Priority Alert Escalation",
            trigger_after=timedelta(minutes=15),
            max_escalations=2,
            escalation_targets=["security-manager@company.com"],
            conditions={"min_priority": AlertPriority.HIGH}
        )
        self.alert_manager.add_escalation_rule(escalation_rule)
    
    def _summarize_events_by_component(self, events: List[InfrastructureEvent]) -> Dict[str, int]:
        """Summarize events by component type."""
        summary = defaultdict(int)
        for event in events:
            summary[event.target_component.value] += 1
        return dict(summary)
    
    def _build_threat_timeline(self, events: List[InfrastructureEvent], 
                              correlations: List[EventCorrelation]) -> List[Dict[str, Any]]:
        """Build a timeline of threat-related activities."""
        timeline = []
        
        # Add high-risk events to timeline
        for event in events:
            if event.risk_score > 0.7:
                timeline.append({
                    'timestamp': event.timestamp.isoformat(),
                    'type': 'high_risk_event',
                    'description': f"{event.event_type} on {event.target_component.value}",
                    'risk_score': event.risk_score
                })
        
        # Add correlations to timeline
        for correlation in correlations:
            timeline.append({
                'timestamp': correlation.timestamp.isoformat(),
                'type': 'correlation',
                'description': f"Correlated activity across {len(correlation.affected_components)} components",
                'correlation_score': correlation.correlation_score
            })
        
        # Sort by timestamp
        timeline.sort(key=lambda x: x['timestamp'])
        
        return timeline
    
    def _generate_recommendations(self, system_status: Dict[str, Any], 
                                 events: List[InfrastructureEvent],
                                 alerts: List[Alert]) -> List[str]:
        """Generate recommendations based on current system state."""
        recommendations = []
        
        # System health recommendations
        health = system_status['system_health']
        if health['overall_status'] != 'healthy':
            recommendations.append("Investigate and resolve unhealthy monitoring components")
        
        # Coverage recommendations
        coverage = system_status['coverage_overview']
        if coverage['average_coverage'] < 0.8:
            recommendations.append("Improve monitoring coverage - current average below 80%")
        
        if coverage['blind_spots'] > 5:
            recommendations.append(f"Address {coverage['blind_spots']} identified monitoring blind spots")
        
        # Alert recommendations
        if len(alerts) > 10:
            recommendations.append("High number of active alerts - review alert rules and thresholds")
        
        # Event pattern recommendations
        high_risk_events = len([e for e in events if e.risk_score > 0.7])
        if high_risk_events > 20:
            recommendations.append("High number of risk events detected - investigate potential security incidents")
        
        return recommendations