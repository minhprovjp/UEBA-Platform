"""
Unified Analysis Dashboard for UBA Self-Monitoring System

This module provides real-time security status visualization, cross-component event
correlation displays, and forensic analysis tools for comprehensive monitoring oversight.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import threading
import time

from .interfaces import (
    InfrastructureEvent, ThreatDetection, ThreatLevel, ComponentType,
    MonitoringInterface, DetectionInterface
)


@dataclass
class SecurityStatus:
    """Real-time security status data model"""
    timestamp: datetime
    overall_threat_level: ThreatLevel
    active_threats: int
    monitored_components: int
    healthy_components: int
    recent_events: int
    correlation_alerts: int


@dataclass
class EventCorrelation:
    """Cross-component event correlation data model"""
    correlation_id: str
    timestamp: datetime
    related_events: List[str]  # event_ids
    affected_components: List[ComponentType]
    correlation_score: float
    attack_chain: List[str]
    risk_assessment: str


@dataclass
class ForensicAnalysis:
    """Forensic analysis result data model"""
    analysis_id: str
    timestamp: datetime
    query_parameters: Dict[str, Any]
    matched_events: List[InfrastructureEvent]
    threat_timeline: List[Tuple[datetime, str]]
    evidence_chain: List[str]
    investigation_notes: str


class UnifiedDashboard:
    """
    Unified analysis dashboard providing real-time security visualization,
    event correlation, and forensic analysis capabilities.
    """
    
    def __init__(self, monitoring_components: List[MonitoringInterface],
                 detection_components: List[DetectionInterface]):
        """
        Initialize the unified dashboard.
        
        Args:
            monitoring_components: List of monitoring components to aggregate data from
            detection_components: List of detection components for threat analysis
        """
        self.monitoring_components = monitoring_components
        self.detection_components = detection_components
        
        # Real-time data storage
        self.recent_events = deque(maxlen=1000)  # Last 1000 events
        self.active_threats = {}  # threat_id -> ThreatDetection
        self.correlations = {}  # correlation_id -> EventCorrelation
        
        # Component health tracking
        self.component_health = {}
        self.last_health_check = datetime.now()
        
        # Threading for real-time updates
        self._running = False
        self._update_thread = None
        self._lock = threading.RLock()
        
        # Forensic analysis cache
        self.forensic_cache = {}
        
        self.logger = logging.getLogger(__name__)
    
    def start_dashboard(self) -> bool:
        """
        Start the unified dashboard with real-time updates.
        
        Returns:
            bool: True if dashboard started successfully
        """
        try:
            self._running = True
            self._update_thread = threading.Thread(target=self._update_loop, daemon=True)
            self._update_thread.start()
            
            self.logger.info("Unified dashboard started successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start unified dashboard: {e}")
            return False
    
    def stop_dashboard(self) -> bool:
        """
        Stop the unified dashboard.
        
        Returns:
            bool: True if dashboard stopped successfully
        """
        try:
            self._running = False
            if self._update_thread:
                self._update_thread.join(timeout=5.0)
            
            self.logger.info("Unified dashboard stopped successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to stop unified dashboard: {e}")
            return False
    
    def get_security_status(self) -> SecurityStatus:
        """
        Get current real-time security status.
        
        Returns:
            SecurityStatus: Current security status overview
        """
        with self._lock:
            # Calculate overall threat level
            threat_levels = [threat.severity for threat in self.active_threats.values()]
            if ThreatLevel.CRITICAL in threat_levels:
                overall_level = ThreatLevel.CRITICAL
            elif ThreatLevel.HIGH in threat_levels:
                overall_level = ThreatLevel.HIGH
            elif ThreatLevel.MEDIUM in threat_levels:
                overall_level = ThreatLevel.MEDIUM
            else:
                overall_level = ThreatLevel.LOW
            
            # Count healthy components
            healthy_count = sum(1 for health in self.component_health.values() if health)
            total_components = len(self.component_health)
            
            # Count recent events (last hour)
            one_hour_ago = datetime.now() - timedelta(hours=1)
            recent_count = sum(1 for event in self.recent_events 
                             if event.timestamp > one_hour_ago)
            
            return SecurityStatus(
                timestamp=datetime.now(),
                overall_threat_level=overall_level,
                active_threats=len(self.active_threats),
                monitored_components=total_components,
                healthy_components=healthy_count,
                recent_events=recent_count,
                correlation_alerts=len(self.correlations)
            )
    
    def get_event_correlations(self, time_window: timedelta = None) -> List[EventCorrelation]:
        """
        Get cross-component event correlations.
        
        Args:
            time_window: Time window for correlation analysis (default: last 24 hours)
            
        Returns:
            List[EventCorrelation]: List of event correlations
        """
        if time_window is None:
            time_window = timedelta(hours=24)
        
        cutoff_time = datetime.now() - time_window
        
        with self._lock:
            return [correlation for correlation in self.correlations.values()
                   if correlation.timestamp > cutoff_time]
    
    def perform_forensic_analysis(self, query_params: Dict[str, Any]) -> ForensicAnalysis:
        """
        Perform forensic analysis based on query parameters.
        
        Args:
            query_params: Analysis parameters (time_range, components, threat_types, etc.)
            
        Returns:
            ForensicAnalysis: Forensic analysis results
        """
        analysis_id = f"forensic_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Extract query parameters
        start_time = query_params.get('start_time', datetime.now() - timedelta(days=1))
        end_time = query_params.get('end_time', datetime.now())
        target_components = query_params.get('components', [])
        threat_types = query_params.get('threat_types', [])
        
        # Collect matching events
        matched_events = []
        with self._lock:
            for event in self.recent_events:
                if start_time <= event.timestamp <= end_time:
                    if not target_components or event.target_component in target_components:
                        matched_events.append(event)
        
        # Build threat timeline
        threat_timeline = []
        for event in sorted(matched_events, key=lambda x: x.timestamp):
            if event.risk_score > 0.5:  # High-risk events
                threat_timeline.append((event.timestamp, 
                                      f"{event.event_type} on {event.target_component.value}"))
        
        # Build evidence chain
        evidence_chain = []
        for event in matched_events:
            if event.risk_score > 0.7:  # Very high-risk events
                evidence_chain.append(f"Event {event.event_id}: {event.action_details}")
        
        analysis = ForensicAnalysis(
            analysis_id=analysis_id,
            timestamp=datetime.now(),
            query_parameters=query_params,
            matched_events=matched_events,
            threat_timeline=threat_timeline,
            evidence_chain=evidence_chain,
            investigation_notes=f"Analysis of {len(matched_events)} events from {start_time} to {end_time}"
        )
        
        # Cache the analysis
        self.forensic_cache[analysis_id] = analysis
        
        self.logger.info(f"Forensic analysis {analysis_id} completed with {len(matched_events)} events")
        return analysis
    
    def get_component_health_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Get detailed health status for all monitored components.
        
        Returns:
            Dict[str, Dict[str, Any]]: Component health details
        """
        health_status = {}
        
        for i, component in enumerate(self.monitoring_components):
            component_name = f"monitor_{i}"
            try:
                is_healthy = component.is_healthy()
                health_status[component_name] = {
                    'healthy': is_healthy,
                    'last_check': self.last_health_check,
                    'component_type': type(component).__name__
                }
            except Exception as e:
                health_status[component_name] = {
                    'healthy': False,
                    'last_check': self.last_health_check,
                    'error': str(e),
                    'component_type': type(component).__name__
                }
        
        return health_status
    
    def get_threat_summary(self) -> Dict[str, Any]:
        """
        Get summary of active threats and their details.
        
        Returns:
            Dict[str, Any]: Threat summary information
        """
        with self._lock:
            threat_summary = {
                'total_threats': len(self.active_threats),
                'by_severity': defaultdict(int),
                'by_component': defaultdict(int),
                'recent_threats': []
            }
            
            for threat in self.active_threats.values():
                threat_summary['by_severity'][threat.severity.value] += 1
                for component in threat.affected_components:
                    threat_summary['by_component'][component.value] += 1
                
                # Add recent threats (last 6 hours)
                if threat.timestamp > datetime.now() - timedelta(hours=6):
                    threat_summary['recent_threats'].append({
                        'id': threat.detection_id,
                        'type': threat.threat_type,
                        'severity': threat.severity.value,
                        'timestamp': threat.timestamp.isoformat(),
                        'confidence': threat.confidence_score
                    })
            
            return dict(threat_summary)
    
    def export_dashboard_data(self, format_type: str = 'json') -> str:
        """
        Export dashboard data for external analysis or reporting.
        
        Args:
            format_type: Export format ('json', 'csv')
            
        Returns:
            str: Exported data in requested format
        """
        dashboard_data = {
            'security_status': asdict(self.get_security_status()),
            'threat_summary': self.get_threat_summary(),
            'component_health': self.get_component_health_status(),
            'correlations': [asdict(corr) for corr in self.get_event_correlations()],
            'export_timestamp': datetime.now().isoformat()
        }
        
        if format_type.lower() == 'json':
            return json.dumps(dashboard_data, indent=2, default=str)
        else:
            # For now, only JSON export is implemented
            return json.dumps(dashboard_data, indent=2, default=str)
    
    def _update_loop(self):
        """Internal update loop for real-time dashboard updates."""
        while self._running:
            try:
                self._update_events()
                self._update_threats()
                self._update_correlations()
                self._update_component_health()
                
                time.sleep(5)  # Update every 5 seconds
                
            except Exception as e:
                self.logger.error(f"Error in dashboard update loop: {e}")
                time.sleep(10)  # Wait longer on error
    
    def _update_events(self):
        """Update recent events from monitoring components."""
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=5)  # Last 5 minutes
        
        for component in self.monitoring_components:
            try:
                if component.is_healthy():
                    events = component.get_events(start_time, end_time)
                    with self._lock:
                        for event in events:
                            # Avoid duplicates
                            if not any(e.event_id == event.event_id for e in self.recent_events):
                                self.recent_events.append(event)
            except Exception as e:
                self.logger.warning(f"Failed to get events from component: {e}")
    
    def _update_threats(self):
        """Update active threats from detection components."""
        # Get recent events for threat analysis
        recent_events_list = list(self.recent_events)
        
        for detector in self.detection_components:
            try:
                threats = detector.analyze_events(recent_events_list)
                with self._lock:
                    for threat in threats:
                        self.active_threats[threat.detection_id] = threat
                        
                        # Remove old threats (older than 24 hours)
                        cutoff_time = datetime.now() - timedelta(hours=24)
                        self.active_threats = {
                            tid: t for tid, t in self.active_threats.items()
                            if t.timestamp > cutoff_time
                        }
            except Exception as e:
                self.logger.warning(f"Failed to analyze threats: {e}")
    
    def _update_correlations(self):
        """Update event correlations based on recent activity."""
        with self._lock:
            # Simple correlation based on time proximity and component overlap
            events_by_time = defaultdict(list)
            
            # Group events by 5-minute windows
            for event in list(self.recent_events)[-100:]:  # Last 100 events
                time_bucket = event.timestamp.replace(second=0, microsecond=0)
                time_bucket = time_bucket.replace(minute=(time_bucket.minute // 5) * 5)
                events_by_time[time_bucket].append(event)
            
            # Find correlations in time buckets with multiple events
            for time_bucket, events in events_by_time.items():
                if len(events) > 1:
                    correlation_id = f"corr_{time_bucket.strftime('%Y%m%d_%H%M')}"
                    
                    if correlation_id not in self.correlations:
                        components = list(set(event.target_component for event in events))
                        correlation_score = min(1.0, len(events) / 10.0)  # Simple scoring
                        
                        self.correlations[correlation_id] = EventCorrelation(
                            correlation_id=correlation_id,
                            timestamp=time_bucket,
                            related_events=[event.event_id for event in events],
                            affected_components=components,
                            correlation_score=correlation_score,
                            attack_chain=[f"Event {event.event_id}" for event in events],
                            risk_assessment="Medium" if correlation_score > 0.5 else "Low"
                        )
    
    def _update_component_health(self):
        """Update component health status."""
        self.last_health_check = datetime.now()
        
        for i, component in enumerate(self.monitoring_components):
            component_name = f"monitor_{i}"
            try:
                self.component_health[component_name] = component.is_healthy()
            except Exception as e:
                self.component_health[component_name] = False
                self.logger.warning(f"Health check failed for {component_name}: {e}")