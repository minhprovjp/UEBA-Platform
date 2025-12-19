"""
Alert Management System for UBA Self-Monitoring

This module provides priority-based alert classification and routing, escalation
procedures, automated notifications, and alert aggregation and filtering capabilities.
"""

import json
import logging
import threading
import time
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Set
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict, deque

# Import existing email alert system
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from email_alert import send_email_alert
except ImportError:
    print("Warning: Could not import existing email_alert module")
    send_email_alert = None

from .interfaces import ThreatDetection, ThreatLevel, ComponentType, InfrastructureEvent


class AlertPriority(Enum):
    """Alert priority levels"""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


class AlertStatus(Enum):
    """Alert status states"""
    NEW = "new"
    ACKNOWLEDGED = "acknowledged"
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"
    ESCALATED = "escalated"


class NotificationChannel(Enum):
    """Notification delivery channels"""
    EMAIL = "email"
    SMS = "sms"
    WEBHOOK = "webhook"
    DASHBOARD = "dashboard"
    LOG = "log"


@dataclass
class Alert:
    """Alert data model"""
    alert_id: str
    timestamp: datetime
    priority: AlertPriority
    status: AlertStatus
    title: str
    description: str
    source_threat: Optional[ThreatDetection]
    source_events: List[str]  # event_ids
    affected_components: List[ComponentType]
    tags: List[str]
    assigned_to: Optional[str] = None
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    resolved_at: Optional[datetime] = None
    escalation_count: int = 0
    last_escalation: Optional[datetime] = None


@dataclass
class NotificationRule:
    """Notification rule configuration"""
    rule_id: str
    name: str
    priority_threshold: AlertPriority
    channels: List[NotificationChannel]
    recipients: List[str]
    conditions: Dict[str, Any]
    enabled: bool = True


@dataclass
class EscalationRule:
    """Escalation rule configuration"""
    rule_id: str
    name: str
    trigger_after: timedelta
    max_escalations: int
    escalation_targets: List[str]
    conditions: Dict[str, Any]
    enabled: bool = True


class AlertManager:
    """
    Alert management system providing priority-based classification, routing,
    escalation, and notification capabilities.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the alert manager.
        
        Args:
            config: Configuration dictionary for alert management
        """
        self.config = config or {}
        
        # Alert storage
        self.active_alerts = {}  # alert_id -> Alert
        self.alert_history = deque(maxlen=10000)  # Historical alerts
        
        # Rules and configuration
        self.notification_rules = {}  # rule_id -> NotificationRule
        self.escalation_rules = {}  # rule_id -> EscalationRule
        
        # Alert aggregation
        self.aggregation_windows = {}  # window_key -> List[Alert]
        self.suppression_rules = {}  # rule_id -> suppression config
        
        # Threading for background processing
        self._running = False
        self._processor_thread = None
        self._escalation_thread = None
        self._lock = threading.RLock()
        
        # Notification handlers
        self.notification_handlers = {
            NotificationChannel.EMAIL: self._send_email_notification,
            NotificationChannel.LOG: self._send_log_notification,
            NotificationChannel.DASHBOARD: self._send_dashboard_notification
        }
        
        # Statistics
        self.alert_stats = defaultdict(int)
        
        self.logger = logging.getLogger(__name__)
        
        # Load default configuration
        self._load_default_config()
    
    def start_alert_manager(self) -> bool:
        """
        Start the alert management system.
        
        Returns:
            bool: True if started successfully
        """
        try:
            self._running = True
            
            # Start background processing threads
            self._processor_thread = threading.Thread(target=self._processing_loop, daemon=True)
            self._escalation_thread = threading.Thread(target=self._escalation_loop, daemon=True)
            
            self._processor_thread.start()
            self._escalation_thread.start()
            
            self.logger.info("Alert manager started successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start alert manager: {e}")
            return False
    
    def stop_alert_manager(self) -> bool:
        """
        Stop the alert management system.
        
        Returns:
            bool: True if stopped successfully
        """
        try:
            self._running = False
            
            if self._processor_thread:
                self._processor_thread.join(timeout=5.0)
            if self._escalation_thread:
                self._escalation_thread.join(timeout=5.0)
            
            self.logger.info("Alert manager stopped successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to stop alert manager: {e}")
            return False
    
    def create_alert(self, threat: ThreatDetection, events: List[InfrastructureEvent]) -> Alert:
        """
        Create a new alert from a threat detection.
        
        Args:
            threat: Threat detection that triggered the alert
            events: Related infrastructure events
            
        Returns:
            Alert: Created alert object
        """
        alert_id = f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{threat.detection_id[:8]}"
        
        # Determine priority based on threat severity
        priority_mapping = {
            ThreatLevel.LOW: AlertPriority.LOW,
            ThreatLevel.MEDIUM: AlertPriority.MEDIUM,
            ThreatLevel.HIGH: AlertPriority.HIGH,
            ThreatLevel.CRITICAL: AlertPriority.CRITICAL
        }
        
        priority = priority_mapping.get(threat.severity, AlertPriority.MEDIUM)
        
        # Generate alert title and description
        title = f"{threat.threat_type} detected on {', '.join([c.value for c in threat.affected_components])}"
        description = f"Threat confidence: {threat.confidence_score:.2f}. "
        description += f"Attack indicators: {', '.join(threat.attack_indicators.keys())}. "
        description += f"Recommended actions: {', '.join(threat.response_actions)}"
        
        # Generate tags
        tags = [threat.threat_type, threat.severity.value]
        tags.extend([component.value for component in threat.affected_components])
        
        alert = Alert(
            alert_id=alert_id,
            timestamp=datetime.now(),
            priority=priority,
            status=AlertStatus.NEW,
            title=title,
            description=description,
            source_threat=threat,
            source_events=[event.event_id for event in events],
            affected_components=threat.affected_components,
            tags=tags
        )
        
        # Check for aggregation/suppression
        if not self._should_suppress_alert(alert):
            with self._lock:
                self.active_alerts[alert_id] = alert
                self.alert_stats['total_created'] += 1
                self.alert_stats[f'priority_{priority.name.lower()}'] += 1
            
            # Trigger notifications
            self._process_alert_notifications(alert)
            
            self.logger.info(f"Created alert {alert_id} with priority {priority.name}")
        else:
            self.logger.info(f"Alert {alert_id} suppressed due to aggregation rules")
        
        return alert
    
    def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """
        Acknowledge an alert.
        
        Args:
            alert_id: ID of the alert to acknowledge
            acknowledged_by: User who acknowledged the alert
            
        Returns:
            bool: True if acknowledged successfully
        """
        with self._lock:
            if alert_id in self.active_alerts:
                alert = self.active_alerts[alert_id]
                alert.status = AlertStatus.ACKNOWLEDGED
                alert.acknowledged_by = acknowledged_by
                alert.acknowledged_at = datetime.now()
                
                self.alert_stats['total_acknowledged'] += 1
                self.logger.info(f"Alert {alert_id} acknowledged by {acknowledged_by}")
                return True
        
        return False
    
    def resolve_alert(self, alert_id: str, resolved_by: str, resolution_notes: str = "") -> bool:
        """
        Resolve an alert.
        
        Args:
            alert_id: ID of the alert to resolve
            resolved_by: User who resolved the alert
            resolution_notes: Optional resolution notes
            
        Returns:
            bool: True if resolved successfully
        """
        with self._lock:
            if alert_id in self.active_alerts:
                alert = self.active_alerts[alert_id]
                alert.status = AlertStatus.RESOLVED
                alert.resolved_by = resolved_by
                alert.resolved_at = datetime.now()
                
                # Move to history
                self.alert_history.append(alert)
                del self.active_alerts[alert_id]
                
                self.alert_stats['total_resolved'] += 1
                self.logger.info(f"Alert {alert_id} resolved by {resolved_by}")
                return True
        
        return False
    
    def get_active_alerts(self, filters: Dict[str, Any] = None) -> List[Alert]:
        """
        Get active alerts with optional filtering.
        
        Args:
            filters: Optional filters (priority, status, components, etc.)
            
        Returns:
            List[Alert]: Filtered list of active alerts
        """
        with self._lock:
            alerts = list(self.active_alerts.values())
        
        if not filters:
            return alerts
        
        # Apply filters
        if 'priority' in filters:
            priority_filter = filters['priority']
            if isinstance(priority_filter, str):
                priority_filter = AlertPriority[priority_filter.upper()]
            alerts = [a for a in alerts if a.priority == priority_filter]
        
        if 'status' in filters:
            status_filter = filters['status']
            if isinstance(status_filter, str):
                status_filter = AlertStatus[status_filter.upper()]
            alerts = [a for a in alerts if a.status == status_filter]
        
        if 'components' in filters:
            component_filter = set(filters['components'])
            alerts = [a for a in alerts if any(c in component_filter for c in a.affected_components)]
        
        if 'tags' in filters:
            tag_filter = set(filters['tags'])
            alerts = [a for a in alerts if any(t in tag_filter for t in a.tags)]
        
        return alerts
    
    def add_notification_rule(self, rule: NotificationRule) -> bool:
        """
        Add a notification rule.
        
        Args:
            rule: Notification rule to add
            
        Returns:
            bool: True if added successfully
        """
        try:
            with self._lock:
                self.notification_rules[rule.rule_id] = rule
            
            self.logger.info(f"Added notification rule {rule.rule_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add notification rule: {e}")
            return False
    
    def add_escalation_rule(self, rule: EscalationRule) -> bool:
        """
        Add an escalation rule.
        
        Args:
            rule: Escalation rule to add
            
        Returns:
            bool: True if added successfully
        """
        try:
            with self._lock:
                self.escalation_rules[rule.rule_id] = rule
            
            self.logger.info(f"Added escalation rule {rule.rule_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add escalation rule: {e}")
            return False
    
    def get_alert_statistics(self) -> Dict[str, Any]:
        """
        Get alert management statistics.
        
        Returns:
            Dict[str, Any]: Alert statistics
        """
        with self._lock:
            stats = dict(self.alert_stats)
            stats['active_alerts'] = len(self.active_alerts)
            stats['alert_history_size'] = len(self.alert_history)
            
            # Priority breakdown of active alerts
            priority_breakdown = defaultdict(int)
            for alert in self.active_alerts.values():
                priority_breakdown[alert.priority.name.lower()] += 1
            
            stats['active_by_priority'] = dict(priority_breakdown)
            
            return stats
    
    def _load_default_config(self):
        """Load default alert management configuration."""
        # Default notification rule for critical alerts
        critical_rule = NotificationRule(
            rule_id="default_critical",
            name="Critical Alert Notifications",
            priority_threshold=AlertPriority.CRITICAL,
            channels=[NotificationChannel.EMAIL, NotificationChannel.LOG],
            recipients=["admin@company.com"],
            conditions={}
        )
        self.notification_rules[critical_rule.rule_id] = critical_rule
        
        # Default escalation rule
        escalation_rule = EscalationRule(
            rule_id="default_escalation",
            name="Unacknowledged Alert Escalation",
            trigger_after=timedelta(minutes=30),
            max_escalations=3,
            escalation_targets=["security-team@company.com"],
            conditions={"min_priority": AlertPriority.HIGH}
        )
        self.escalation_rules[escalation_rule.rule_id] = escalation_rule
    
    def _should_suppress_alert(self, alert: Alert) -> bool:
        """
        Check if an alert should be suppressed due to aggregation rules.
        
        Args:
            alert: Alert to check for suppression
            
        Returns:
            bool: True if alert should be suppressed
        """
        # Simple suppression: don't create duplicate alerts for same threat type
        # within 5 minutes on same components
        suppression_window = timedelta(minutes=5)
        cutoff_time = datetime.now() - suppression_window
        
        with self._lock:
            for existing_alert in self.active_alerts.values():
                if (existing_alert.timestamp > cutoff_time and
                    existing_alert.source_threat and
                    alert.source_threat and
                    existing_alert.source_threat.threat_type == alert.source_threat.threat_type and
                    set(existing_alert.affected_components) == set(alert.affected_components)):
                    return True
        
        return False
    
    def _process_alert_notifications(self, alert: Alert):
        """
        Process notifications for a new alert.
        
        Args:
            alert: Alert to process notifications for
        """
        for rule in self.notification_rules.values():
            if not rule.enabled:
                continue
            
            # Check if alert meets rule criteria
            if alert.priority.value >= rule.priority_threshold.value:
                # Check additional conditions
                if self._check_rule_conditions(alert, rule.conditions):
                    self._send_notifications(alert, rule)
    
    def _check_rule_conditions(self, alert: Alert, conditions: Dict[str, Any]) -> bool:
        """
        Check if alert meets rule conditions.
        
        Args:
            alert: Alert to check
            conditions: Rule conditions to evaluate
            
        Returns:
            bool: True if conditions are met
        """
        # Simple condition checking - can be extended
        if 'components' in conditions:
            required_components = set(conditions['components'])
            alert_components = set([c.value for c in alert.affected_components])
            if not required_components.intersection(alert_components):
                return False
        
        if 'tags' in conditions:
            required_tags = set(conditions['tags'])
            if not required_tags.intersection(set(alert.tags)):
                return False
        
        return True
    
    def _send_notifications(self, alert: Alert, rule: NotificationRule):
        """
        Send notifications for an alert according to a rule.
        
        Args:
            alert: Alert to send notifications for
            rule: Notification rule to follow
        """
        for channel in rule.channels:
            if channel in self.notification_handlers:
                try:
                    self.notification_handlers[channel](alert, rule.recipients)
                except Exception as e:
                    self.logger.error(f"Failed to send {channel.value} notification: {e}")
    
    def _send_email_notification(self, alert: Alert, recipients: List[str]):
        """Send email notification for an alert using existing email system."""
        if not send_email_alert:
            self.logger.warning("Email alert system not available, skipping email notification")
            return
            
        # Email configuration from config
        smtp_config = self.config.get('smtp', {})
        if not smtp_config:
            self.logger.warning("SMTP not configured, skipping email notification")
            return
        
        try:
            subject = f"[UBA Self-Monitoring Alert - {alert.priority.name}] {alert.title}"
            
            text_content = f"""
UBA Self-Monitoring Alert

Alert ID: {alert.alert_id}
Priority: {alert.priority.name}
Timestamp: {alert.timestamp}
Components: {', '.join([c.value for c in alert.affected_components])}

Description:
{alert.description}

Please investigate and acknowledge this alert in the monitoring dashboard.
            """
            
            html_content = f"""
<html>
<body>
<h2 style="color: {'#dc2626' if alert.priority == AlertPriority.CRITICAL else '#f59e0b' if alert.priority == AlertPriority.HIGH else '#3b82f6'};">
    UBA Self-Monitoring Alert
</h2>
<table style="border-collapse: collapse; width: 100%;">
    <tr><td style="padding: 8px; border: 1px solid #ddd;"><strong>Alert ID:</strong></td><td style="padding: 8px; border: 1px solid #ddd;">{alert.alert_id}</td></tr>
    <tr><td style="padding: 8px; border: 1px solid #ddd;"><strong>Priority:</strong></td><td style="padding: 8px; border: 1px solid #ddd;">{alert.priority.name}</td></tr>
    <tr><td style="padding: 8px; border: 1px solid #ddd;"><strong>Timestamp:</strong></td><td style="padding: 8px; border: 1px solid #ddd;">{alert.timestamp}</td></tr>
    <tr><td style="padding: 8px; border: 1px solid #ddd;"><strong>Components:</strong></td><td style="padding: 8px; border: 1px solid #ddd;">{', '.join([c.value for c in alert.affected_components])}</td></tr>
</table>
<h3>Description:</h3>
<p>{alert.description}</p>
<p><em>Please investigate and acknowledge this alert in the monitoring dashboard.</em></p>
</body>
</html>
            """
            
            # Use existing email alert system
            result = send_email_alert(
                subject=subject,
                text_content=text_content,
                html_content=html_content,
                to_recipients=recipients,
                smtp_server=smtp_config.get('host', 'localhost'),
                smtp_port=smtp_config.get('port', 587),
                sender_email=smtp_config.get('username', 'uba-monitoring@company.com'),
                sender_password=smtp_config.get('password', ''),
                use_tls=smtp_config.get('use_tls', True)
            )
            
            if result is True:
                self.logger.info(f"Email notification sent for alert {alert.alert_id}")
            else:
                self.logger.error(f"Failed to send email notification: {result}")
            
        except Exception as e:
            self.logger.error(f"Failed to send email notification: {e}")
    
    def _send_log_notification(self, alert: Alert, recipients: List[str]):
        """Send log notification for an alert."""
        log_message = f"ALERT: {alert.title} | Priority: {alert.priority.name} | " \
                     f"Components: {', '.join([c.value for c in alert.affected_components])} | " \
                     f"ID: {alert.alert_id}"
        
        self.logger.warning(log_message)
    
    def _send_dashboard_notification(self, alert: Alert, recipients: List[str]):
        """Send dashboard notification for an alert."""
        # This would integrate with the dashboard system
        # For now, just log that a dashboard notification would be sent
        self.logger.info(f"Dashboard notification for alert {alert.alert_id}")
    
    def _processing_loop(self):
        """Background processing loop for alert management."""
        while self._running:
            try:
                self._process_aggregation()
                self._cleanup_old_alerts()
                time.sleep(30)  # Process every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in alert processing loop: {e}")
                time.sleep(60)  # Wait longer on error
    
    def _escalation_loop(self):
        """Background escalation loop for unacknowledged alerts."""
        while self._running:
            try:
                self._process_escalations()
                time.sleep(60)  # Check escalations every minute
                
            except Exception as e:
                self.logger.error(f"Error in escalation loop: {e}")
                time.sleep(120)  # Wait longer on error
    
    def _process_aggregation(self):
        """Process alert aggregation and suppression."""
        # Implementation for alert aggregation logic
        pass
    
    def _cleanup_old_alerts(self):
        """Clean up old resolved alerts from history."""
        # Keep only last 30 days of history
        cutoff_time = datetime.now() - timedelta(days=30)
        
        # Filter alert history
        self.alert_history = deque(
            [alert for alert in self.alert_history if alert.resolved_at and alert.resolved_at > cutoff_time],
            maxlen=10000
        )
    
    def _process_escalations(self):
        """Process alert escalations for unacknowledged alerts."""
        current_time = datetime.now()
        
        with self._lock:
            for alert in list(self.active_alerts.values()):
                if alert.status == AlertStatus.NEW:
                    # Check escalation rules
                    for rule in self.escalation_rules.values():
                        if not rule.enabled:
                            continue
                        
                        # Check if alert meets escalation criteria
                        if self._should_escalate_alert(alert, rule, current_time):
                            self._escalate_alert(alert, rule)
    
    def _should_escalate_alert(self, alert: Alert, rule: EscalationRule, current_time: datetime) -> bool:
        """
        Check if an alert should be escalated.
        
        Args:
            alert: Alert to check
            rule: Escalation rule to evaluate
            current_time: Current timestamp
            
        Returns:
            bool: True if alert should be escalated
        """
        # Check if enough time has passed
        if current_time - alert.timestamp < rule.trigger_after:
            return False
        
        # Check if max escalations reached
        if alert.escalation_count >= rule.max_escalations:
            return False
        
        # Check if already escalated recently
        if (alert.last_escalation and 
            current_time - alert.last_escalation < rule.trigger_after):
            return False
        
        # Check rule conditions
        if 'min_priority' in rule.conditions:
            min_priority = rule.conditions['min_priority']
            if alert.priority.value < min_priority.value:
                return False
        
        return True
    
    def _escalate_alert(self, alert: Alert, rule: EscalationRule):
        """
        Escalate an alert according to an escalation rule.
        
        Args:
            alert: Alert to escalate
            rule: Escalation rule to follow
        """
        alert.status = AlertStatus.ESCALATED
        alert.escalation_count += 1
        alert.last_escalation = datetime.now()
        
        # Send escalation notifications
        escalation_rule = NotificationRule(
            rule_id=f"escalation_{rule.rule_id}",
            name=f"Escalation for {rule.name}",
            priority_threshold=AlertPriority.LOW,  # Always send escalation
            channels=[NotificationChannel.EMAIL, NotificationChannel.LOG],
            recipients=rule.escalation_targets,
            conditions={}
        )
        
        self._send_notifications(alert, escalation_rule)
        
        self.alert_stats['total_escalated'] += 1
        self.logger.warning(f"Alert {alert.alert_id} escalated (count: {alert.escalation_count})")