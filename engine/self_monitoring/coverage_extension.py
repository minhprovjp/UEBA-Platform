"""
Monitoring Coverage Extension System for UBA Self-Monitoring

This module implements blind spot identification algorithms, automatic monitoring
coverage expansion, and comprehensive infrastructure interaction logging.
"""

import json
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
from enum import Enum

from .interfaces import (
    InfrastructureEvent, ComponentType, MonitoringInterface,
    DetectionInterface, ThreatDetection
)


class CoverageStatus(Enum):
    """Monitoring coverage status"""
    FULL = "full"
    PARTIAL = "partial"
    MINIMAL = "minimal"
    NONE = "none"


class BlindSpotType(Enum):
    """Types of monitoring blind spots"""
    TEMPORAL = "temporal"  # Time gaps in monitoring
    COMPONENT = "component"  # Unmonitored components
    INTERACTION = "interaction"  # Unmonitored interactions
    BEHAVIORAL = "behavioral"  # Unmonitored behavior patterns
    NETWORK = "network"  # Network communication gaps


@dataclass
class BlindSpot:
    """Blind spot identification data model"""
    blind_spot_id: str
    timestamp: datetime
    blind_spot_type: BlindSpotType
    affected_components: List[ComponentType]
    description: str
    risk_level: str
    detection_method: str
    recommended_actions: List[str]
    coverage_gap_percentage: float


@dataclass
class CoverageMetrics:
    """Coverage metrics data model"""
    component: ComponentType
    coverage_percentage: float
    monitored_interactions: int
    total_interactions: int
    last_activity: datetime
    blind_spots: List[str]  # blind_spot_ids
    status: CoverageStatus


@dataclass
class InteractionPattern:
    """Infrastructure interaction pattern data model"""
    pattern_id: str
    source_component: ComponentType
    target_component: ComponentType
    interaction_type: str
    frequency: float  # interactions per hour
    typical_times: List[str]  # hour patterns
    risk_indicators: Dict[str, Any]
    monitoring_status: str


class CoverageExtensionSystem:
    """
    Monitoring coverage extension system that identifies blind spots,
    automatically expands monitoring coverage, and logs comprehensive
    infrastructure interactions.
    """
    
    def __init__(self, monitoring_components: List[MonitoringInterface],
                 detection_components: List[DetectionInterface]):
        """
        Initialize the coverage extension system.
        
        Args:
            monitoring_components: List of monitoring components
            detection_components: List of detection components
        """
        self.monitoring_components = monitoring_components
        self.detection_components = detection_components
        
        # Coverage tracking
        self.coverage_metrics = {}  # component -> CoverageMetrics
        self.blind_spots = {}  # blind_spot_id -> BlindSpot
        self.interaction_patterns = {}  # pattern_id -> InteractionPattern
        
        # Infrastructure interaction logging
        self.interaction_log = deque(maxlen=50000)  # Comprehensive interaction log
        self.component_interactions = defaultdict(list)  # component -> interactions
        
        # Coverage analysis
        self.coverage_analysis_history = deque(maxlen=1000)
        self.expansion_history = []
        
        # Threading for continuous monitoring
        self._running = False
        self._analysis_thread = None
        self._expansion_thread = None
        self._lock = threading.RLock()
        
        # Configuration
        self.config = {
            'analysis_interval': 300,  # 5 minutes
            'expansion_interval': 900,  # 15 minutes
            'blind_spot_threshold': 0.8,  # 80% coverage threshold
            'interaction_timeout': 3600,  # 1 hour timeout for interactions
            'max_expansion_attempts': 5
        }
        
        self.logger = logging.getLogger(__name__)
    
    def start_coverage_system(self) -> bool:
        """
        Start the coverage extension system.
        
        Returns:
            bool: True if started successfully
        """
        try:
            self._running = True
            
            # Start background analysis threads
            self._analysis_thread = threading.Thread(target=self._analysis_loop, daemon=True)
            self._expansion_thread = threading.Thread(target=self._expansion_loop, daemon=True)
            
            self._analysis_thread.start()
            self._expansion_thread.start()
            
            # Initialize coverage metrics
            self._initialize_coverage_metrics()
            
            self.logger.info("Coverage extension system started successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start coverage extension system: {e}")
            return False
    
    def stop_coverage_system(self) -> bool:
        """
        Stop the coverage extension system.
        
        Returns:
            bool: True if stopped successfully
        """
        try:
            self._running = False
            
            if self._analysis_thread:
                self._analysis_thread.join(timeout=5.0)
            if self._expansion_thread:
                self._expansion_thread.join(timeout=5.0)
            
            self.logger.info("Coverage extension system stopped successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to stop coverage extension system: {e}")
            return False
    
    def identify_blind_spots(self) -> List[BlindSpot]:
        """
        Identify monitoring blind spots in the current coverage.
        
        Returns:
            List[BlindSpot]: List of identified blind spots
        """
        blind_spots = []
        current_time = datetime.now()
        
        with self._lock:
            # Analyze temporal blind spots
            temporal_spots = self._identify_temporal_blind_spots()
            blind_spots.extend(temporal_spots)
            
            # Analyze component blind spots
            component_spots = self._identify_component_blind_spots()
            blind_spots.extend(component_spots)
            
            # Analyze interaction blind spots
            interaction_spots = self._identify_interaction_blind_spots()
            blind_spots.extend(interaction_spots)
            
            # Analyze behavioral blind spots
            behavioral_spots = self._identify_behavioral_blind_spots()
            blind_spots.extend(behavioral_spots)
            
            # Update blind spots storage
            for spot in blind_spots:
                self.blind_spots[spot.blind_spot_id] = spot
        
        self.logger.info(f"Identified {len(blind_spots)} monitoring blind spots")
        return blind_spots
    
    def get_coverage_metrics(self) -> Dict[ComponentType, CoverageMetrics]:
        """
        Get current coverage metrics for all components.
        
        Returns:
            Dict[ComponentType, CoverageMetrics]: Coverage metrics by component
        """
        with self._lock:
            return dict(self.coverage_metrics)
    
    def log_infrastructure_interaction(self, source: ComponentType, target: ComponentType,
                                     interaction_type: str, details: Dict[str, Any]):
        """
        Log a comprehensive infrastructure interaction.
        
        Args:
            source: Source component of the interaction
            target: Target component of the interaction
            interaction_type: Type of interaction
            details: Detailed information about the interaction
        """
        interaction_id = f"int_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        
        interaction_event = {
            'interaction_id': interaction_id,
            'timestamp': datetime.now(),
            'source': source,
            'target': target,
            'type': interaction_type,
            'details': details,
            'logged_by': 'coverage_extension_system'
        }
        
        with self._lock:
            self.interaction_log.append(interaction_event)
            self.component_interactions[source].append(interaction_event)
            self.component_interactions[target].append(interaction_event)
        
        # Update interaction patterns
        self._update_interaction_patterns(source, target, interaction_type)
        
        self.logger.debug(f"Logged infrastructure interaction {interaction_id}")
    
    def expand_monitoring_coverage(self, blind_spot: BlindSpot) -> bool:
        """
        Automatically expand monitoring coverage to address a blind spot.
        
        Args:
            blind_spot: Blind spot to address
            
        Returns:
            bool: True if coverage expansion was successful
        """
        try:
            expansion_id = f"exp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            success = False
            
            if blind_spot.blind_spot_type == BlindSpotType.COMPONENT:
                success = self._expand_component_monitoring(blind_spot)
            elif blind_spot.blind_spot_type == BlindSpotType.TEMPORAL:
                success = self._expand_temporal_monitoring(blind_spot)
            elif blind_spot.blind_spot_type == BlindSpotType.INTERACTION:
                success = self._expand_interaction_monitoring(blind_spot)
            elif blind_spot.blind_spot_type == BlindSpotType.BEHAVIORAL:
                success = self._expand_behavioral_monitoring(blind_spot)
            
            # Record expansion attempt
            expansion_record = {
                'expansion_id': expansion_id,
                'timestamp': datetime.now(),
                'blind_spot_id': blind_spot.blind_spot_id,
                'blind_spot_type': blind_spot.blind_spot_type.value,
                'success': success,
                'method': f'expand_{blind_spot.blind_spot_type.value}_monitoring'
            }
            
            self.expansion_history.append(expansion_record)
            
            if success:
                self.logger.info(f"Successfully expanded monitoring coverage for blind spot {blind_spot.blind_spot_id}")
            else:
                self.logger.warning(f"Failed to expand monitoring coverage for blind spot {blind_spot.blind_spot_id}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error expanding monitoring coverage: {e}")
            return False
    
    def get_interaction_patterns(self, component: ComponentType = None) -> List[InteractionPattern]:
        """
        Get infrastructure interaction patterns.
        
        Args:
            component: Optional component to filter patterns
            
        Returns:
            List[InteractionPattern]: List of interaction patterns
        """
        with self._lock:
            patterns = list(self.interaction_patterns.values())
            
            if component:
                patterns = [p for p in patterns 
                           if p.source_component == component or p.target_component == component]
            
            return patterns
    
    def get_comprehensive_interaction_log(self, time_range: timedelta = None) -> List[Dict[str, Any]]:
        """
        Get comprehensive infrastructure interaction log.
        
        Args:
            time_range: Time range for log entries (default: last 24 hours)
            
        Returns:
            List[Dict[str, Any]]: Comprehensive interaction log
        """
        if time_range is None:
            time_range = timedelta(hours=24)
        
        cutoff_time = datetime.now() - time_range
        
        with self._lock:
            return [interaction for interaction in self.interaction_log
                   if interaction['timestamp'] > cutoff_time]
    
    def generate_coverage_report(self) -> Dict[str, Any]:
        """
        Generate a comprehensive coverage report.
        
        Returns:
            Dict[str, Any]: Coverage report
        """
        with self._lock:
            report = {
                'timestamp': datetime.now().isoformat(),
                'overall_coverage': self._calculate_overall_coverage(),
                'component_coverage': {
                    component.value: asdict(metrics) 
                    for component, metrics in self.coverage_metrics.items()
                },
                'blind_spots': {
                    spot_id: asdict(spot) 
                    for spot_id, spot in self.blind_spots.items()
                },
                'interaction_patterns': {
                    pattern_id: asdict(pattern)
                    for pattern_id, pattern in self.interaction_patterns.items()
                },
                'expansion_history': self.expansion_history[-10:],  # Last 10 expansions
                'total_interactions_logged': len(self.interaction_log)
            }
            
            return report
    
    def _initialize_coverage_metrics(self):
        """Initialize coverage metrics for all components."""
        for component in ComponentType:
            self.coverage_metrics[component] = CoverageMetrics(
                component=component,
                coverage_percentage=0.0,
                monitored_interactions=0,
                total_interactions=0,
                last_activity=datetime.now(),
                blind_spots=[],
                status=CoverageStatus.NONE
            )
    
    def _analysis_loop(self):
        """Background analysis loop for coverage monitoring."""
        while self._running:
            try:
                self._analyze_coverage()
                self._update_coverage_metrics()
                self.identify_blind_spots()
                
                time.sleep(self.config['analysis_interval'])
                
            except Exception as e:
                self.logger.error(f"Error in coverage analysis loop: {e}")
                time.sleep(60)  # Wait longer on error
    
    def _expansion_loop(self):
        """Background expansion loop for automatic coverage extension."""
        while self._running:
            try:
                self._process_automatic_expansion()
                time.sleep(self.config['expansion_interval'])
                
            except Exception as e:
                self.logger.error(f"Error in coverage expansion loop: {e}")
                time.sleep(120)  # Wait longer on error
    
    def _analyze_coverage(self):
        """Analyze current monitoring coverage."""
        current_time = datetime.now()
        analysis_window = timedelta(hours=1)
        
        # Collect recent events from all monitoring components
        all_events = []
        for component in self.monitoring_components:
            try:
                if component.is_healthy():
                    events = component.get_events(current_time - analysis_window, current_time)
                    all_events.extend(events)
            except Exception as e:
                self.logger.warning(f"Failed to get events for coverage analysis: {e}")
        
        # Analyze coverage based on events
        component_activity = defaultdict(int)
        for event in all_events:
            component_activity[event.target_component] += 1
        
        # Store analysis results
        analysis_result = {
            'timestamp': current_time,
            'total_events': len(all_events),
            'component_activity': dict(component_activity),
            'monitoring_components_healthy': sum(1 for c in self.monitoring_components if c.is_healthy())
        }
        
        self.coverage_analysis_history.append(analysis_result)
    
    def _update_coverage_metrics(self):
        """Update coverage metrics based on recent analysis."""
        if not self.coverage_analysis_history:
            return
        
        latest_analysis = self.coverage_analysis_history[-1]
        
        for component in ComponentType:
            activity_count = latest_analysis['component_activity'].get(component, 0)
            
            # Simple coverage calculation based on activity
            if activity_count > 50:
                coverage = 1.0
                status = CoverageStatus.FULL
            elif activity_count > 20:
                coverage = 0.8
                status = CoverageStatus.PARTIAL
            elif activity_count > 5:
                coverage = 0.5
                status = CoverageStatus.MINIMAL
            else:
                coverage = 0.1
                status = CoverageStatus.NONE
            
            self.coverage_metrics[component].coverage_percentage = coverage
            self.coverage_metrics[component].monitored_interactions = activity_count
            self.coverage_metrics[component].status = status
            self.coverage_metrics[component].last_activity = latest_analysis['timestamp']
    
    def _identify_temporal_blind_spots(self) -> List[BlindSpot]:
        """Identify temporal blind spots in monitoring coverage."""
        blind_spots = []
        
        # Look for time gaps in monitoring
        if len(self.coverage_analysis_history) >= 2:
            recent_analyses = list(self.coverage_analysis_history)[-10:]  # Last 10 analyses
            
            for i in range(1, len(recent_analyses)):
                prev_analysis = recent_analyses[i-1]
                curr_analysis = recent_analyses[i]
                
                time_gap = curr_analysis['timestamp'] - prev_analysis['timestamp']
                expected_gap = timedelta(seconds=self.config['analysis_interval'])
                
                if time_gap > expected_gap * 2:  # Significant gap
                    blind_spot = BlindSpot(
                        blind_spot_id=f"temporal_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        timestamp=datetime.now(),
                        blind_spot_type=BlindSpotType.TEMPORAL,
                        affected_components=list(ComponentType),
                        description=f"Monitoring gap of {time_gap.total_seconds():.0f} seconds detected",
                        risk_level="Medium",
                        detection_method="temporal_analysis",
                        recommended_actions=["Investigate monitoring system health", "Check for system outages"],
                        coverage_gap_percentage=min(100.0, (time_gap.total_seconds() / 3600) * 100)
                    )
                    blind_spots.append(blind_spot)
        
        return blind_spots
    
    def _identify_component_blind_spots(self) -> List[BlindSpot]:
        """Identify component blind spots in monitoring coverage."""
        blind_spots = []
        
        for component, metrics in self.coverage_metrics.items():
            if metrics.coverage_percentage < self.config['blind_spot_threshold']:
                blind_spot = BlindSpot(
                    blind_spot_id=f"component_{component.value}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    timestamp=datetime.now(),
                    blind_spot_type=BlindSpotType.COMPONENT,
                    affected_components=[component],
                    description=f"Low monitoring coverage ({metrics.coverage_percentage:.1%}) for {component.value}",
                    risk_level="High" if metrics.coverage_percentage < 0.3 else "Medium",
                    detection_method="coverage_analysis",
                    recommended_actions=[
                        f"Increase monitoring for {component.value}",
                        "Deploy additional monitoring agents",
                        "Review monitoring configuration"
                    ],
                    coverage_gap_percentage=(1.0 - metrics.coverage_percentage) * 100
                )
                blind_spots.append(blind_spot)
        
        return blind_spots
    
    def _identify_interaction_blind_spots(self) -> List[BlindSpot]:
        """Identify interaction blind spots in monitoring coverage."""
        blind_spots = []
        
        # Analyze interaction patterns for gaps
        expected_interactions = [
            (ComponentType.DATABASE, ComponentType.USER_ACCOUNT),
            (ComponentType.USER_ACCOUNT, ComponentType.PERFORMANCE_SCHEMA),
            (ComponentType.MONITORING_SERVICE, ComponentType.AUDIT_LOG)
        ]
        
        for source, target in expected_interactions:
            pattern_key = f"{source.value}_{target.value}"
            
            if pattern_key not in self.interaction_patterns:
                blind_spot = BlindSpot(
                    blind_spot_id=f"interaction_{pattern_key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    timestamp=datetime.now(),
                    blind_spot_type=BlindSpotType.INTERACTION,
                    affected_components=[source, target],
                    description=f"Missing interaction monitoring between {source.value} and {target.value}",
                    risk_level="Medium",
                    detection_method="interaction_analysis",
                    recommended_actions=[
                        f"Enable interaction monitoring between {source.value} and {target.value}",
                        "Review interaction logging configuration"
                    ],
                    coverage_gap_percentage=100.0
                )
                blind_spots.append(blind_spot)
        
        return blind_spots
    
    def _identify_behavioral_blind_spots(self) -> List[BlindSpot]:
        """Identify behavioral blind spots in monitoring coverage."""
        blind_spots = []
        
        # Look for components with activity but no behavioral analysis
        for component, metrics in self.coverage_metrics.items():
            if metrics.monitored_interactions > 0:
                # Check if we have behavioral patterns for this component
                has_behavioral_monitoring = any(
                    pattern.source_component == component or pattern.target_component == component
                    for pattern in self.interaction_patterns.values()
                )
                
                if not has_behavioral_monitoring:
                    blind_spot = BlindSpot(
                        blind_spot_id=f"behavioral_{component.value}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        timestamp=datetime.now(),
                        blind_spot_type=BlindSpotType.BEHAVIORAL,
                        affected_components=[component],
                        description=f"No behavioral analysis for active component {component.value}",
                        risk_level="Low",
                        detection_method="behavioral_analysis",
                        recommended_actions=[
                            f"Enable behavioral monitoring for {component.value}",
                            "Configure baseline behavior patterns"
                        ],
                        coverage_gap_percentage=50.0
                    )
                    blind_spots.append(blind_spot)
        
        return blind_spots
    
    def _update_interaction_patterns(self, source: ComponentType, target: ComponentType, interaction_type: str):
        """Update interaction patterns based on logged interactions."""
        pattern_key = f"{source.value}_{target.value}_{interaction_type}"
        
        current_time = datetime.now()
        current_hour = current_time.strftime('%H')
        
        if pattern_key in self.interaction_patterns:
            pattern = self.interaction_patterns[pattern_key]
            pattern.frequency += 1.0 / 3600  # Increment frequency (per hour)
            
            if current_hour not in pattern.typical_times:
                pattern.typical_times.append(current_hour)
        else:
            pattern = InteractionPattern(
                pattern_id=pattern_key,
                source_component=source,
                target_component=target,
                interaction_type=interaction_type,
                frequency=1.0 / 3600,  # Initial frequency
                typical_times=[current_hour],
                risk_indicators={},
                monitoring_status="active"
            )
            self.interaction_patterns[pattern_key] = pattern
    
    def _expand_component_monitoring(self, blind_spot: BlindSpot) -> bool:
        """Expand monitoring coverage for component blind spots."""
        # This would integrate with monitoring component configuration
        # For now, simulate expansion by updating coverage metrics
        for component in blind_spot.affected_components:
            if component in self.coverage_metrics:
                self.coverage_metrics[component].coverage_percentage = min(1.0, 
                    self.coverage_metrics[component].coverage_percentage + 0.2)
                
                if self.coverage_metrics[component].coverage_percentage >= self.config['blind_spot_threshold']:
                    self.coverage_metrics[component].status = CoverageStatus.FULL
        
        return True
    
    def _expand_temporal_monitoring(self, blind_spot: BlindSpot) -> bool:
        """Expand monitoring coverage for temporal blind spots."""
        # This would adjust monitoring intervals and add redundancy
        # For now, log the expansion attempt
        self.logger.info(f"Expanding temporal monitoring coverage for blind spot {blind_spot.blind_spot_id}")
        return True
    
    def _expand_interaction_monitoring(self, blind_spot: BlindSpot) -> bool:
        """Expand monitoring coverage for interaction blind spots."""
        # This would enable additional interaction logging
        # For now, create placeholder interaction patterns
        for i, component in enumerate(blind_spot.affected_components):
            if i < len(blind_spot.affected_components) - 1:
                source = component
                target = blind_spot.affected_components[i + 1]
                self._update_interaction_patterns(source, target, "expanded_monitoring")
        
        return True
    
    def _expand_behavioral_monitoring(self, blind_spot: BlindSpot) -> bool:
        """Expand monitoring coverage for behavioral blind spots."""
        # This would enable behavioral analysis for components
        # For now, create basic behavioral patterns
        for component in blind_spot.affected_components:
            pattern_key = f"behavioral_{component.value}_baseline"
            
            if pattern_key not in self.interaction_patterns:
                pattern = InteractionPattern(
                    pattern_id=pattern_key,
                    source_component=component,
                    target_component=component,
                    interaction_type="behavioral_baseline",
                    frequency=1.0,
                    typical_times=["09", "10", "11", "14", "15", "16"],  # Business hours
                    risk_indicators={"baseline_established": True},
                    monitoring_status="behavioral_analysis"
                )
                self.interaction_patterns[pattern_key] = pattern
        
        return True
    
    def _process_automatic_expansion(self):
        """Process automatic coverage expansion for high-priority blind spots."""
        high_priority_spots = [
            spot for spot in self.blind_spots.values()
            if spot.risk_level in ["High", "Critical"] and 
            spot.coverage_gap_percentage > 50.0
        ]
        
        for spot in high_priority_spots[:3]:  # Process up to 3 high-priority spots
            if len([exp for exp in self.expansion_history 
                   if exp['blind_spot_id'] == spot.blind_spot_id]) < self.config['max_expansion_attempts']:
                self.expand_monitoring_coverage(spot)
    
    def _calculate_overall_coverage(self) -> float:
        """Calculate overall monitoring coverage percentage."""
        if not self.coverage_metrics:
            return 0.0
        
        total_coverage = sum(metrics.coverage_percentage for metrics in self.coverage_metrics.values())
        return total_coverage / len(self.coverage_metrics)