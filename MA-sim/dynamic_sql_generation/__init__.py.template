"""
Dynamic SQL Generation System for Vietnamese Company Simulation

This package provides intelligent, context-aware SQL query generation
that replaces static templates with dynamic generation based on:
- Current database state and business context
- User behavior patterns and Vietnamese cultural factors
- Temporal patterns and business workflows
- Query complexity adaptation based on user expertise
"""

__version__ = "1.0.0"
__author__ = "Dynamic SQL Generation Team"

from .models import (
    QueryContext,
    UserContext,
    BusinessContext,
    TemporalContext,
    CulturalContext,
    DatabaseState,
    ExpertiseLevel,
    WorkflowType,
    BusinessEvent,
    BusinessCyclePhase,
    SensitivityLevel
)

from .vietnamese_patterns import VietnameseBusinessPatterns
from .config import ConfigurationManager, get_config_manager
from .context_engine import QueryContextEngine, WorkflowContext, RelationshipMap
from .complexity_engine import QueryComplexityEngine, ComplexityLevel, ComplexityAssessment, QueryGenerationStrategy
from .generator import DynamicSQLGenerator, QueryPattern, GenerationResult

__all__ = [
    # Core Models
    'QueryContext',
    'UserContext', 
    'BusinessContext',
    'TemporalContext',
    'CulturalContext',
    'DatabaseState',
    
    # Enums
    'ExpertiseLevel',
    'WorkflowType',
    'BusinessEvent',
    'BusinessCyclePhase',
    'SensitivityLevel',
    'ComplexityLevel',
    
    # Main Components
    'DynamicSQLGenerator',
    'QueryContextEngine',
    'QueryComplexityEngine',
    'VietnameseBusinessPatterns',
    'ConfigurationManager',
    'get_config_manager',
    
    # Helper Classes
    'WorkflowContext',
    'RelationshipMap',
    'ComplexityAssessment',
    'QueryGenerationStrategy',
    'QueryPattern',
    'GenerationResult'
]