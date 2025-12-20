"""
Data Models for Dynamic SQL Generation System

Defines the core data structures for context-aware SQL generation
including user context, business context, temporal patterns, and
Vietnamese cultural factors.
"""

import json
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from datetime import datetime


class ExpertiseLevel(Enum):
    """User expertise levels for query complexity adaptation"""
    NOVICE = "novice"
    INTERMEDIATE = "intermediate"
    ADVANCED = "advanced"
    EXPERT = "expert"


class WorkflowType(Enum):
    """Vietnamese business workflow types"""
    SALES_PROCESS = "sales_process"
    CUSTOMER_SERVICE = "customer_service"
    FINANCIAL_REPORTING = "financial_reporting"
    HR_MANAGEMENT = "hr_management"
    INVENTORY_MANAGEMENT = "inventory_management"
    MARKETING_CAMPAIGN = "marketing_campaign"
    ADMINISTRATIVE = "administrative"
    MAINTENANCE = "maintenance"


class BusinessEvent(Enum):
    """Vietnamese business events that affect query patterns"""
    MONTH_END_CLOSING = "month_end_closing"
    QUARTER_END_REPORTING = "quarter_end_reporting"
    TET_PREPARATION = "tet_preparation"
    HOLIDAY_PERIOD = "holiday_period"
    AUDIT_PERIOD = "audit_period"
    BUDGET_PLANNING = "budget_planning"
    PERFORMANCE_REVIEW = "performance_review"
    NORMAL_OPERATIONS = "normal_operations"


class BusinessCyclePhase(Enum):
    """Vietnamese business cycle phases"""
    PEAK_SEASON = "peak_season"
    LOW_SEASON = "low_season"
    TRANSITION = "transition"
    HOLIDAY_SEASON = "holiday_season"


class SensitivityLevel(Enum):
    """Data sensitivity levels for compliance"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


@dataclass
class QueryHistory:
    """Individual query history record"""
    query: str
    timestamp: datetime
    success: bool
    execution_time: float
    complexity_level: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'query': self.query,
            'timestamp': self.timestamp.isoformat(),
            'success': self.success,
            'execution_time': self.execution_time,
            'complexity_level': self.complexity_level
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueryHistory':
        """Create from dictionary for JSON deserialization"""
        return cls(
            query=data['query'],
            timestamp=datetime.fromisoformat(data['timestamp']),
            success=data['success'],
            execution_time=data['execution_time'],
            complexity_level=data['complexity_level']
        )


@dataclass
class UserContext:
    """User context for query generation"""
    username: str
    role: str
    department: str
    expertise_level: ExpertiseLevel
    session_history: List[QueryHistory]
    work_intensity: float
    stress_level: float
    
    def validate(self) -> bool:
        """Validate user context data integrity"""
        if not self.username or not isinstance(self.username, str):
            return False
        if not self.role or not isinstance(self.role, str):
            return False
        if not self.department or not isinstance(self.department, str):
            return False
        if not isinstance(self.expertise_level, ExpertiseLevel):
            return False
        if not isinstance(self.session_history, list):
            return False
        if not (0.0 <= self.work_intensity <= 2.0):
            return False
        if not (0.0 <= self.stress_level <= 1.0):
            return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'username': self.username,
            'role': self.role,
            'department': self.department,
            'expertise_level': self.expertise_level.value,
            'session_history': [h.to_dict() for h in self.session_history],
            'work_intensity': self.work_intensity,
            'stress_level': self.stress_level
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UserContext':
        """Create from dictionary for JSON deserialization"""
        return cls(
            username=data['username'],
            role=data['role'],
            department=data['department'],
            expertise_level=ExpertiseLevel(data['expertise_level']),
            session_history=[QueryHistory.from_dict(h) for h in data['session_history']],
            work_intensity=data['work_intensity'],
            stress_level=data['stress_level']
        )


@dataclass(frozen=True)
class ComplianceRule:
    """Vietnamese compliance rule"""
    rule_id: str
    description: str
    applies_to_roles: tuple  # Changed from List to tuple for hashability
    data_types: tuple  # Changed from List to tuple for hashability
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'rule_id': self.rule_id,
            'description': self.description,
            'applies_to_roles': list(self.applies_to_roles),
            'data_types': list(self.data_types)
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ComplianceRule':
        """Create from dictionary for JSON deserialization"""
        return cls(
            rule_id=data['rule_id'],
            description=data['description'],
            applies_to_roles=tuple(data['applies_to_roles']),
            data_types=tuple(data['data_types'])
        )


@dataclass
class BusinessContext:
    """Business context for query generation"""
    current_workflow: WorkflowType
    business_event: Optional[BusinessEvent]
    department_interactions: List[str]
    compliance_requirements: List[ComplianceRule]
    data_sensitivity_level: SensitivityLevel
    
    def validate(self) -> bool:
        """Validate business context data integrity"""
        if not isinstance(self.current_workflow, WorkflowType):
            return False
        if self.business_event and not isinstance(self.business_event, BusinessEvent):
            return False
        if not isinstance(self.department_interactions, list):
            return False
        if not isinstance(self.compliance_requirements, list):
            return False
        if not isinstance(self.data_sensitivity_level, SensitivityLevel):
            return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'current_workflow': self.current_workflow.value,
            'business_event': self.business_event.value if self.business_event else None,
            'department_interactions': self.department_interactions,
            'compliance_requirements': [rule.to_dict() for rule in self.compliance_requirements],
            'data_sensitivity_level': self.data_sensitivity_level.value
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BusinessContext':
        """Create from dictionary for JSON deserialization"""
        return cls(
            current_workflow=WorkflowType(data['current_workflow']),
            business_event=BusinessEvent(data['business_event']) if data['business_event'] else None,
            department_interactions=data['department_interactions'],
            compliance_requirements=[ComplianceRule.from_dict(rule) for rule in data['compliance_requirements']],
            data_sensitivity_level=SensitivityLevel(data['data_sensitivity_level'])
        )


@dataclass
class TemporalContext:
    """Temporal context for Vietnamese business patterns"""
    current_hour: int
    is_work_hours: bool
    is_lunch_break: bool
    is_vietnamese_holiday: bool
    business_cycle_phase: BusinessCyclePhase
    seasonal_factor: float
    
    def validate(self) -> bool:
        """Validate temporal context data integrity"""
        if not (0 <= self.current_hour <= 23):
            return False
        if not isinstance(self.is_work_hours, bool):
            return False
        if not isinstance(self.is_lunch_break, bool):
            return False
        if not isinstance(self.is_vietnamese_holiday, bool):
            return False
        if not isinstance(self.business_cycle_phase, BusinessCyclePhase):
            return False
        if not (0.0 <= self.seasonal_factor <= 2.0):
            return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'current_hour': self.current_hour,
            'is_work_hours': self.is_work_hours,
            'is_lunch_break': self.is_lunch_break,
            'is_vietnamese_holiday': self.is_vietnamese_holiday,
            'business_cycle_phase': self.business_cycle_phase.value,
            'seasonal_factor': self.seasonal_factor
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TemporalContext':
        """Create from dictionary for JSON deserialization"""
        return cls(
            current_hour=data['current_hour'],
            is_work_hours=data['is_work_hours'],
            is_lunch_break=data['is_lunch_break'],
            is_vietnamese_holiday=data['is_vietnamese_holiday'],
            business_cycle_phase=BusinessCyclePhase(data['business_cycle_phase']),
            seasonal_factor=data['seasonal_factor']
        )


@dataclass
class CulturalConstraints:
    """Vietnamese cultural constraints"""
    hierarchy_level: int
    respect_seniority: bool
    work_overtime_acceptable: bool
    tet_preparation_mode: bool
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CulturalConstraints':
        """Create from dictionary for JSON deserialization"""
        return cls(**data)


@dataclass
class CulturalContext:
    """Cultural context for Vietnamese business practices"""
    cultural_constraints: CulturalConstraints
    vietnamese_holidays: List[str]
    business_etiquette: Dict[str, Any]
    language_preferences: Dict[str, str]
    
    def validate(self) -> bool:
        """Validate cultural context data integrity"""
        if not isinstance(self.cultural_constraints, CulturalConstraints):
            return False
        if not isinstance(self.vietnamese_holidays, list):
            return False
        if not isinstance(self.business_etiquette, dict):
            return False
        if not isinstance(self.language_preferences, dict):
            return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'cultural_constraints': self.cultural_constraints.to_dict(),
            'vietnamese_holidays': self.vietnamese_holidays,
            'business_etiquette': self.business_etiquette,
            'language_preferences': self.language_preferences
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CulturalContext':
        """Create from dictionary for JSON deserialization"""
        return cls(
            cultural_constraints=CulturalConstraints.from_dict(data['cultural_constraints']),
            vietnamese_holidays=data['vietnamese_holidays'],
            business_etiquette=data['business_etiquette'],
            language_preferences=data['language_preferences']
        )


@dataclass
class Relationship:
    """Database relationship definition"""
    from_table: str
    to_table: str
    relationship_type: str
    foreign_key: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Relationship':
        """Create from dictionary for JSON deserialization"""
        return cls(**data)


@dataclass
class ConstraintViolation:
    """Database constraint violation record"""
    constraint_type: str
    table_name: str
    column_name: str
    violation_count: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConstraintViolation':
        """Create from dictionary for JSON deserialization"""
        return cls(**data)


@dataclass
class Modification:
    """Database modification record"""
    table_name: str
    operation: str
    timestamp: datetime
    affected_rows: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'table_name': self.table_name,
            'operation': self.operation,
            'timestamp': self.timestamp.isoformat(),
            'affected_rows': self.affected_rows
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Modification':
        """Create from dictionary for JSON deserialization"""
        return cls(
            table_name=data['table_name'],
            operation=data['operation'],
            timestamp=datetime.fromisoformat(data['timestamp']),
            affected_rows=data['affected_rows']
        )


@dataclass
class PerformanceMetrics:
    """Database performance metrics"""
    avg_query_time: float
    slow_query_count: int
    connection_count: int
    cache_hit_ratio: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PerformanceMetrics':
        """Create from dictionary for JSON deserialization"""
        return cls(**data)


@dataclass
class DatabaseState:
    """Current database state for context-aware generation"""
    entity_counts: Dict[str, int]
    relationship_map: Dict[str, List[Relationship]]
    constraint_violations: List[ConstraintViolation]
    recent_modifications: List[Modification]
    performance_metrics: PerformanceMetrics
    
    def validate(self) -> bool:
        """Validate database state data integrity"""
        if not isinstance(self.entity_counts, dict):
            return False
        if not isinstance(self.relationship_map, dict):
            return False
        if not isinstance(self.constraint_violations, list):
            return False
        if not isinstance(self.recent_modifications, list):
            return False
        if not isinstance(self.performance_metrics, PerformanceMetrics):
            return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'entity_counts': self.entity_counts,
            'relationship_map': {k: [r.to_dict() for r in v] for k, v in self.relationship_map.items()},
            'constraint_violations': [cv.to_dict() for cv in self.constraint_violations],
            'recent_modifications': [mod.to_dict() for mod in self.recent_modifications],
            'performance_metrics': self.performance_metrics.to_dict()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DatabaseState':
        """Create from dictionary for JSON deserialization"""
        return cls(
            entity_counts=data['entity_counts'],
            relationship_map={k: [Relationship.from_dict(r) for r in v] for k, v in data['relationship_map'].items()},
            constraint_violations=[ConstraintViolation.from_dict(cv) for cv in data['constraint_violations']],
            recent_modifications=[Modification.from_dict(mod) for mod in data['recent_modifications']],
            performance_metrics=PerformanceMetrics.from_dict(data['performance_metrics'])
        )


@dataclass
class QueryContext:
    """Main query context containing all contextual information"""
    user_context: UserContext
    database_state: DatabaseState
    business_context: BusinessContext
    temporal_context: TemporalContext
    cultural_context: CulturalContext
    
    def validate(self) -> bool:
        """Validate all context data integrity"""
        return (
            self.user_context.validate() and
            self.database_state.validate() and
            self.business_context.validate() and
            self.temporal_context.validate() and
            self.cultural_context.validate()
        )
    
    def to_json(self) -> str:
        """Serialize to JSON string"""
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'user_context': self.user_context.to_dict(),
            'database_state': self.database_state.to_dict(),
            'business_context': self.business_context.to_dict(),
            'temporal_context': self.temporal_context.to_dict(),
            'cultural_context': self.cultural_context.to_dict()
        }
    
    @classmethod
    def from_json(cls, json_str: str) -> 'QueryContext':
        """Deserialize from JSON string"""
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueryContext':
        """Create from dictionary for JSON deserialization"""
        return cls(
            user_context=UserContext.from_dict(data['user_context']),
            database_state=DatabaseState.from_dict(data['database_state']),
            business_context=BusinessContext.from_dict(data['business_context']),
            temporal_context=TemporalContext.from_dict(data['temporal_context']),
            cultural_context=CulturalContext.from_dict(data['cultural_context'])
        )