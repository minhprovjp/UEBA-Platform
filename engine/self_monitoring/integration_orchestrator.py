"""
Integration Orchestrator for UBA Self-Monitoring System

This module integrates all monitoring, detection, and response components into a
unified self-monitoring system with end-to-end data flow and processing pipeline.
"""

import logging
import threading
import time
import json
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass, asdict
from pathlib import Path
import queue
import uuid

try:
    from .interfaces import (
        MonitoringInterface, DetectionInterface, ResponseInterface,
        IntegrityInterface, ShadowMonitoringInterface, ConfigurationInterface,
        InfrastructureEvent, ThreatDetection, ResponseAction, ThreatLevel,
        ComponentType
    )
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
    from .performance_optimizer import PerformanceOptimizer, create_performance_optimizer
except ImportError:
    # For direct execution or testing
    from interfaces import (
        MonitoringInterface, DetectionInterface, ResponseInterface,
        IntegrityInterface, ShadowMonitoringInterface, ConfigurationInterface,
        InfrastructureEvent, ThreatDetection, ResponseAction, ThreatLevel,
        ComponentType
    )
    from config_manager import SelfMonitoringConfig
    from crypto_logger import CryptoLogger
    from infrastructure_monitor import InfrastructureMonitor
    from shadow_monitor import ShadowMonitor
    from behavioral_anomaly_detector import BehavioralAnomalyDetector
    from attack_pattern_recognition import AttackPatternRecognitionEngine
    from advanced_threat_detector import AdvancedThreatDetector
    from threat_response_orchestrator import ThreatResponseOrchestrator
    from integrity_validator import IntegrityValidator
    from unified_dashboard import UnifiedDashboard
    from alert_manager import AlertManager
    from adaptive_security_evolution import AdaptiveSecurityEvolution
    from coverage_extension import CoverageExtensionSystem
    from emergency_protection import EmergencyProtectionSystem
    from performance_optimizer import PerformanceOptimizer, create_performance_optimizer


@dataclass
class SystemStatus:
    """Overall system status"""
    timestamp: datetime
    monitoring_active: bool
    shadow_monitoring_active: bool
    detection_active: bool
    response_active: bool
    integrity_validation_active: bool
    dashboard_active: bool
    overall_health: str
    active_threats: int
    processed_events: int
    response_actions_executed: int


@dataclass
class DataFlowMetrics:
    """Data flow and processing metrics"""
    events_ingested: int
    events_processed: int
    threats_detected: int
    responses_triggered: int
    integrity_checks_performed: int
    correlation_analyses_completed: int
    processing_latency_ms: float
    throughput_events_per_second: float


class IntegrationOrchestrator:
    """
    Main orchestrator that integrates all UBA self-monitoring components
    into a unified system with end-to-end data flow and processing pipeline.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the integration orchestrator.
        
        Args:
            config_path: Path to configuration file
        """
        self.logger = logging.getLogger(__name__)
        
        # Configuration management
        self.config_manager = SelfMonitoringConfig(config_path)
        self.config = self.config_manager.load_config()
        
        # Core components
        self.crypto_logger = CryptoLogger()
        
        # Initialize all monitoring components
        self._initialize_monitoring_components()
        
        # Initialize detection components
        self._initialize_detection_components()
        
        # Initialize response components
        self._initialize_response_components()
        
        # Initialize management components
        self._initialize_management_components()
        
        # Initialize performance optimizer
        self._initialize_performance_optimizer()
        
        # System state
        self._system_running = False
        self._orchestrator_thread = None
        self._stop_event = threading.Event()
        
        # Data flow queues
        self._event_queue = queue.Queue(maxsize=10000)
        self._threat_queue = queue.Queue(maxsize=1000)
        self._response_queue = queue.Queue(maxsize=500)
        
        # Processing threads
        self._processing_threads = []
        
        # Metrics and statistics
        self._metrics = DataFlowMetrics(0, 0, 0, 0, 0, 0, 0.0, 0.0)
        self._last_metrics_update = datetime.now()
        
        # Component health tracking
        self._component_health = {}
        self._last_health_check = datetime.now()
        
        self.logger.info("Integration orchestrator initialized")
    
    def _initialize_monitoring_components(self):
        """Initialize all monitoring components"""
        try:
            # Primary infrastructure monitor
            self.infrastructure_monitor = InfrastructureMonitor(self.config_manager)
            
            # Shadow monitoring system
            self.shadow_monitor = ShadowMonitor(self.config_manager)
            
            # Set up shadow monitor to watch primary monitor
            # Note: Shadow monitor will be started with primary monitor reference later
            
            self.logger.info("Monitoring components initialized")
            
        except Exception as e:
            self.logger.error(f"Error initializing monitoring components: {e}")
            raise
    
    def _initialize_detection_components(self):
        """Initialize all detection components"""
        try:
            detection_config = self.config.get('detection', {})
            
            # Behavioral anomaly detector
            self.behavioral_detector = BehavioralAnomalyDetector(detection_config)
            
            # Attack pattern recognition
            self.attack_pattern_detector = AttackPatternRecognitionEngine(detection_config)
            
            # Advanced threat detector
            self.advanced_threat_detector = AdvancedThreatDetector(detection_config)
            
            # Adaptive security evolution
            self.adaptive_security = AdaptiveSecurityEvolution(detection_config)
            
            self.logger.info("Detection components initialized")
            
        except Exception as e:
            self.logger.error(f"Error initializing detection components: {e}")
            raise
    
    def _initialize_response_components(self):
        """Initialize all response components"""
        try:
            response_config = self.config.get('response', {})
            
            # Threat response orchestrator
            self.response_orchestrator = ThreatResponseOrchestrator(response_config)
            
            # Emergency protection system
            self.emergency_protection = EmergencyProtectionSystem(response_config)
            
            # Integrity validator
            self.integrity_validator = IntegrityValidator(self.crypto_logger)
            
            self.logger.info("Response components initialized")
            
        except Exception as e:
            self.logger.error(f"Error initializing response components: {e}")
            raise
    
    def _initialize_management_components(self):
        """Initialize management and visualization components"""
        try:
            # Unified dashboard
            monitoring_components = [self.infrastructure_monitor, self.shadow_monitor]
            detection_components = [
                self.behavioral_detector, 
                self.attack_pattern_detector,
                self.advanced_threat_detector
            ]
            
            self.dashboard = UnifiedDashboard(monitoring_components, detection_components)
            
            # Alert manager
            alert_config = self.config.get('alerting', {})
            self.alert_manager = AlertManager(alert_config)
            
            # Coverage extension system
            self.coverage_extension = CoverageExtensionSystem(monitoring_components, detection_components)
            
            self.logger.info("Management components initialized")
            
        except Exception as e:
            self.logger.error(f"Error initializing management components: {e}")
            raise
    
    def _initialize_performance_optimizer(self):
        """Initialize performance optimization components"""
        try:
            performance_config = self.config.get('performance', {})
            self.performance_optimizer = create_performance_optimizer(performance_config)
            
            self.logger.info("Performance optimizer initialized")
            
        except Exception as e:
            self.logger.error(f"Error initializing performance optimizer: {e}")
            raise
    
    def start_system(self) -> bool:
        """
        Start the complete UBA self-monitoring system.
        
        Returns:
            bool: True if system started successfully, False otherwise
        """
        try:
            if self._system_running:
                self.logger.warning("System is already running")
                return True
            
            self.logger.info("Starting UBA self-monitoring system...")
            
            # Start monitoring components
            if not self._start_monitoring_components():
                return False
            
            # Start detection components
            if not self._start_detection_components():
                return False
            
            # Start response components
            if not self._start_response_components():
                return False
            
            # Start management components
            if not self._start_management_components():
                return False
            
            # Start data processing pipeline
            if not self._start_processing_pipeline():
                return False
            
            # Start main orchestration loop
            self._system_running = True
            self._stop_event.clear()
            self._orchestrator_thread = threading.Thread(
                target=self._orchestration_loop, 
                daemon=True
            )
            self._orchestrator_thread.start()
            
            self.logger.info("UBA self-monitoring system started successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting system: {e}")
            self.stop_system()
            return False
    
    def _start_monitoring_components(self) -> bool:
        """Start all monitoring components"""
        try:
            # Start primary infrastructure monitor
            if not self.infrastructure_monitor.start_monitoring():
                self.logger.error("Failed to start infrastructure monitor")
                return False
            
            # Start shadow monitor
            if not self.shadow_monitor.start_monitoring(self.infrastructure_monitor):
                self.logger.error("Failed to start shadow monitor")
                return False
            
            self.logger.info("Monitoring components started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting monitoring components: {e}")
            return False
    
    def _start_detection_components(self) -> bool:
        """Start all detection components"""
        try:
            # Detection components are typically passive and don't need explicit starting
            # They process events as they come through the pipeline
            
            # Initialize adaptive security evolution
            self.adaptive_security.initialize_evolution_system()
            
            self.logger.info("Detection components started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting detection components: {e}")
            return False
    
    def _start_response_components(self) -> bool:
        """Start all response components"""
        try:
            # Response components are typically reactive and don't need explicit starting
            # They respond to threats as they are detected
            
            self.logger.info("Response components started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting response components: {e}")
            return False
    
    def _start_management_components(self) -> bool:
        """Start management and visualization components"""
        try:
            # Start unified dashboard
            if not self.dashboard.start_dashboard():
                self.logger.error("Failed to start unified dashboard")
                return False
            
            # Start alert manager
            if not self.alert_manager.start_alert_processing():
                self.logger.error("Failed to start alert manager")
                return False
            
            # Start coverage extension
            if not self.coverage_extension.start_coverage_monitoring():
                self.logger.error("Failed to start coverage extension")
                return False
            
            self.logger.info("Management components started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting management components: {e}")
            return False
    
    def _start_processing_pipeline(self) -> bool:
        """Start the data processing pipeline"""
        try:
            # Start event processing thread
            event_processor = threading.Thread(
                target=self._process_events_loop,
                daemon=True
            )
            event_processor.start()
            self._processing_threads.append(event_processor)
            
            # Start threat processing thread
            threat_processor = threading.Thread(
                target=self._process_threats_loop,
                daemon=True
            )
            threat_processor.start()
            self._processing_threads.append(threat_processor)
            
            # Start response processing thread
            response_processor = threading.Thread(
                target=self._process_responses_loop,
                daemon=True
            )
            response_processor.start()
            self._processing_threads.append(response_processor)
            
            self.logger.info("Data processing pipeline started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting processing pipeline: {e}")
            return False
    
    def stop_system(self) -> bool:
        """
        Stop the complete UBA self-monitoring system.
        
        Returns:
            bool: True if system stopped successfully, False otherwise
        """
        try:
            if not self._system_running:
                self.logger.warning("System is not running")
                return True
            
            self.logger.info("Stopping UBA self-monitoring system...")
            
            # Signal stop to all threads
            self._stop_event.set()
            self._system_running = False
            
            # Stop orchestration thread
            if self._orchestrator_thread and self._orchestrator_thread.is_alive():
                self._orchestrator_thread.join(timeout=10)
            
            # Stop processing threads
            for thread in self._processing_threads:
                if thread.is_alive():
                    thread.join(timeout=5)
            
            # Stop management components
            self._stop_management_components()
            
            # Stop response components
            self._stop_response_components()
            
            # Stop detection components
            self._stop_detection_components()
            
            # Stop monitoring components
            self._stop_monitoring_components()
            
            # Stop performance optimizer
            if hasattr(self, 'performance_optimizer'):
                self.performance_optimizer.shutdown()
            
            self.logger.info("UBA self-monitoring system stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping system: {e}")
            return False
    
    def _stop_monitoring_components(self):
        """Stop all monitoring components"""
        try:
            self.infrastructure_monitor.stop_monitoring()
            # Shadow monitor doesn't have explicit stop method in current implementation
            self.logger.info("Monitoring components stopped")
        except Exception as e:
            self.logger.error(f"Error stopping monitoring components: {e}")
    
    def _stop_detection_components(self):
        """Stop all detection components"""
        try:
            # Detection components are typically passive
            self.logger.info("Detection components stopped")
        except Exception as e:
            self.logger.error(f"Error stopping detection components: {e}")
    
    def _stop_response_components(self):
        """Stop all response components"""
        try:
            # Response components are typically reactive
            self.logger.info("Response components stopped")
        except Exception as e:
            self.logger.error(f"Error stopping response components: {e}")
    
    def _stop_management_components(self):
        """Stop management and visualization components"""
        try:
            self.dashboard.stop_dashboard()
            self.alert_manager.stop_alert_processing()
            self.coverage_extension.stop_coverage_monitoring()
            self.logger.info("Management components stopped")
        except Exception as e:
            self.logger.error(f"Error stopping management components: {e}")
    
    def _orchestration_loop(self):
        """Main orchestration loop"""
        health_check_interval = self.config.get('system', {}).get('health_check_interval', 60)
        metrics_update_interval = self.config.get('system', {}).get('metrics_update_interval', 30)
        
        last_health_check = datetime.now()
        last_metrics_update = datetime.now()
        
        while not self._stop_event.is_set():
            try:
                current_time = datetime.now()
                
                # Perform health checks
                if (current_time - last_health_check).seconds >= health_check_interval:
                    self._perform_health_checks()
                    last_health_check = current_time
                
                # Update metrics
                if (current_time - last_metrics_update).seconds >= metrics_update_interval:
                    self._update_metrics()
                    last_metrics_update = current_time
                
                # Check for system-wide issues
                self._check_system_integrity()
                
                # Coordinate component interactions
                self._coordinate_components()
                
            except Exception as e:
                self.logger.error(f"Error in orchestration loop: {e}")
            
            # Wait before next iteration
            self._stop_event.wait(10)
    
    def _process_events_loop(self):
        """Process infrastructure events"""
        while not self._stop_event.is_set():
            try:
                # Get events from monitoring components
                events = self._collect_events_from_monitors()
                
                for event in events:
                    # Validate event integrity
                    if self.integrity_validator.verify_integrity(
                        event.__dict__, event.integrity_hash
                    ):
                        # Add to processing queue
                        self._event_queue.put(event)
                        self._metrics.events_ingested += 1
                    else:
                        self.logger.warning(f"Event integrity validation failed: {event.event_id}")
                
                # Process events through detection pipeline
                self._process_event_batch()
                
            except Exception as e:
                self.logger.error(f"Error in event processing loop: {e}")
            
            time.sleep(1)
    
    def _process_threats_loop(self):
        """Process detected threats"""
        while not self._stop_event.is_set():
            try:
                if not self._threat_queue.empty():
                    threat = self._threat_queue.get_nowait()
                    
                    # Log threat detection
                    self.crypto_logger.log_monitoring_event(
                        f"threat_detected_{threat.threat_type}",
                        threat.affected_components[0].value if threat.affected_components else "unknown",
                        asdict(threat),
                        threat.confidence_score
                    )
                    
                    # Send to alert manager
                    self.alert_manager.process_threat_alert(threat)
                    
                    # Determine response actions
                    if threat.severity in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                        response = self.response_orchestrator.execute_response(threat)
                        self._response_queue.put(response)
                    
                    self._metrics.threats_detected += 1
                
            except queue.Empty:
                pass
            except Exception as e:
                self.logger.error(f"Error in threat processing loop: {e}")
            
            time.sleep(0.5)
    
    def _process_responses_loop(self):
        """Process automated responses"""
        while not self._stop_event.is_set():
            try:
                if not self._response_queue.empty():
                    response = self._response_queue.get_nowait()
                    
                    # Log response execution
                    self.crypto_logger.log_monitoring_event(
                        f"response_executed_{response.action_type}",
                        response.target,
                        asdict(response)
                    )
                    
                    # Update dashboard
                    self.dashboard.update_response_status(response)
                    
                    self._metrics.responses_triggered += 1
                
            except queue.Empty:
                pass
            except Exception as e:
                self.logger.error(f"Error in response processing loop: {e}")
            
            time.sleep(0.5)
    
    def _collect_events_from_monitors(self) -> List[InfrastructureEvent]:
        """Collect events from all monitoring components"""
        events = []
        
        try:
            # Get events from infrastructure monitor
            end_time = datetime.now()
            start_time = end_time - timedelta(seconds=30)
            
            infra_events = self.infrastructure_monitor.get_events(start_time, end_time)
            events.extend(infra_events)
            
            # Get events from shadow monitor
            shadow_detections = self.shadow_monitor.detect_primary_compromise()
            for detection in shadow_detections:
                # Convert threat detection to infrastructure event for processing
                event = InfrastructureEvent(
                    event_id=detection.detection_id,
                    timestamp=detection.timestamp,
                    event_type=f"shadow_detection_{detection.threat_type}",
                    source_ip="shadow_monitor",
                    user_account="system",
                    target_component=detection.affected_components[0] if detection.affected_components else ComponentType.MONITORING_SERVICE,
                    action_details=detection.attack_indicators,
                    risk_score=detection.confidence_score,
                    integrity_hash=""
                )
                event.integrity_hash = self.crypto_logger.create_checksum(event.__dict__)
                events.append(event)
            
            # Optimize events using performance optimizer
            if hasattr(self, 'performance_optimizer'):
                events = self.performance_optimizer.optimize_event_processing(events)
            
        except Exception as e:
            self.logger.error(f"Error collecting events from monitors: {e}")
        
        return events
    
    def _process_event_batch(self):
        """Process a batch of events through detection pipeline"""
        try:
            batch_size = self.config.get('processing', {}).get('batch_size', 100)
            events_batch = []
            
            # Collect batch of events
            for _ in range(batch_size):
                if self._event_queue.empty():
                    break
                events_batch.append(self._event_queue.get_nowait())
            
            if not events_batch:
                return
            
            # Process through detection components with performance optimization
            all_threats = []
            
            # Use performance optimizer for threat detection if available
            if hasattr(self, 'performance_optimizer'):
                # Behavioral anomaly detection
                behavioral_threats = self.performance_optimizer.optimize_threat_detection(
                    events_batch, self.behavioral_detector.analyze_events
                )
                all_threats.extend(behavioral_threats)
                
                # Attack pattern recognition
                pattern_threats = self.performance_optimizer.optimize_threat_detection(
                    events_batch, self.attack_pattern_detector.analyze_events
                )
                all_threats.extend(pattern_threats)
                
                # Advanced threat detection
                advanced_threats = self.performance_optimizer.optimize_threat_detection(
                    events_batch, self.advanced_threat_detector.analyze_events
                )
                all_threats.extend(advanced_threats)
            else:
                # Fallback to normal processing
                behavioral_threats = self.behavioral_detector.analyze_events(events_batch)
                all_threats.extend(behavioral_threats)
                
                pattern_threats = self.attack_pattern_detector.analyze_events(events_batch)
                all_threats.extend(pattern_threats)
                
                advanced_threats = self.advanced_threat_detector.analyze_events(events_batch)
                all_threats.extend(advanced_threats)
            
            # Queue threats for response processing
            for threat in all_threats:
                self._threat_queue.put(threat)
            
            # Update processing metrics
            self._metrics.events_processed += len(events_batch)
            
        except Exception as e:
            self.logger.error(f"Error processing event batch: {e}")
    
    def _perform_health_checks(self):
        """Perform health checks on all components"""
        try:
            current_time = datetime.now()
            
            # Check monitoring components
            self._component_health['infrastructure_monitor'] = {
                'healthy': self.infrastructure_monitor.is_healthy(),
                'last_check': current_time
            }
            
            # Check shadow monitor (simplified check)
            self._component_health['shadow_monitor'] = {
                'healthy': True,  # Shadow monitor doesn't have explicit health check
                'last_check': current_time
            }
            
            # Check dashboard
            self._component_health['dashboard'] = {
                'healthy': self.dashboard.is_healthy() if hasattr(self.dashboard, 'is_healthy') else True,
                'last_check': current_time
            }
            
            # Check alert manager
            self._component_health['alert_manager'] = {
                'healthy': self.alert_manager.is_healthy() if hasattr(self.alert_manager, 'is_healthy') else True,
                'last_check': current_time
            }
            
            self._last_health_check = current_time
            
        except Exception as e:
            self.logger.error(f"Error performing health checks: {e}")
    
    def _update_metrics(self):
        """Update system metrics"""
        try:
            current_time = datetime.now()
            time_diff = (current_time - self._last_metrics_update).total_seconds()
            
            if time_diff > 0:
                self._metrics.throughput_events_per_second = self._metrics.events_processed / time_diff
            
            self._last_metrics_update = current_time
            
        except Exception as e:
            self.logger.error(f"Error updating metrics: {e}")
    
    def _check_system_integrity(self):
        """Check overall system integrity"""
        try:
            # Check for component failures
            failed_components = [
                name for name, health in self._component_health.items()
                if not health.get('healthy', False)
            ]
            
            if failed_components:
                self.logger.warning(f"Failed components detected: {failed_components}")
                
                # Trigger emergency protocols if critical components fail
                critical_components = ['infrastructure_monitor', 'shadow_monitor']
                if any(comp in failed_components for comp in critical_components):
                    self.emergency_protection.activate_emergency_protocols()
            
        except Exception as e:
            self.logger.error(f"Error checking system integrity: {e}")
    
    def _coordinate_components(self):
        """Coordinate interactions between components"""
        try:
            # Update adaptive security with latest threat intelligence
            if hasattr(self.adaptive_security, 'update_threat_intelligence'):
                threat_intel = self._gather_threat_intelligence()
                self.adaptive_security.update_threat_intelligence(threat_intel)
            
            # Update coverage extension with monitoring gaps
            if hasattr(self.coverage_extension, 'analyze_coverage_gaps'):
                self.coverage_extension.analyze_coverage_gaps()
            
        except Exception as e:
            self.logger.error(f"Error coordinating components: {e}")
    
    def _gather_threat_intelligence(self) -> Dict[str, Any]:
        """Gather threat intelligence from various sources"""
        try:
            return {
                'recent_threats': len(self._threat_queue.queue),
                'attack_patterns': self.attack_pattern_detector.get_detection_rules(),
                'behavioral_baselines': self.behavioral_detector.get_detection_rules(),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error gathering threat intelligence: {e}")
            return {}
    
    def get_system_status(self) -> SystemStatus:
        """Get overall system status"""
        try:
            return SystemStatus(
                timestamp=datetime.now(),
                monitoring_active=self.infrastructure_monitor.is_healthy(),
                shadow_monitoring_active=True,  # Simplified
                detection_active=True,
                response_active=True,
                integrity_validation_active=True,
                dashboard_active=self._component_health.get('dashboard', {}).get('healthy', False),
                overall_health="healthy" if self._system_running else "stopped",
                active_threats=self._threat_queue.qsize(),
                processed_events=self._metrics.events_processed,
                response_actions_executed=self._metrics.responses_triggered
            )
        except Exception as e:
            self.logger.error(f"Error getting system status: {e}")
            return SystemStatus(
                timestamp=datetime.now(),
                monitoring_active=False,
                shadow_monitoring_active=False,
                detection_active=False,
                response_active=False,
                integrity_validation_active=False,
                dashboard_active=False,
                overall_health="error",
                active_threats=0,
                processed_events=0,
                response_actions_executed=0
            )
    
    def get_data_flow_metrics(self) -> DataFlowMetrics:
        """Get data flow and processing metrics"""
        # Enhance metrics with performance optimizer data if available
        if hasattr(self, 'performance_optimizer'):
            perf_metrics = self.performance_optimizer.get_performance_metrics()
            self._metrics.throughput_events_per_second = perf_metrics.get('events_per_second', 0.0)
        
        return self._metrics
    
    def is_healthy(self) -> bool:
        """Check if the overall system is healthy"""
        try:
            if not self._system_running:
                return False
            
            # Check critical components
            critical_health = [
                self._component_health.get('infrastructure_monitor', {}).get('healthy', False),
                self._component_health.get('shadow_monitor', {}).get('healthy', False)
            ]
            
            return all(critical_health)
            
        except Exception as e:
            self.logger.error(f"Error checking system health: {e}")
            return False
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get comprehensive performance metrics"""
        try:
            if hasattr(self, 'performance_optimizer'):
                return self.performance_optimizer.get_performance_metrics()
            else:
                return {
                    'performance_optimizer_available': False,
                    'basic_metrics': self._metrics.__dict__
                }
        except Exception as e:
            self.logger.error(f"Error getting performance metrics: {e}")
            return {}
    
    def optimize_system_performance(self):
        """Trigger system performance optimization"""
        try:
            if hasattr(self, 'performance_optimizer'):
                self.performance_optimizer.optimize_memory_usage()
                self.logger.info("System performance optimization completed")
        except Exception as e:
            self.logger.error(f"Error optimizing system performance: {e}")