"""
Dynamic SQL Generator - Main Orchestrator

The central orchestrator that coordinates context analysis and query generation
for Vietnamese company simulation datasets. Integrates with existing translator
and agent systems while providing sophisticated attack pattern generation.
"""

import logging
import random
import time
import uuid
import json
import requests
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import os

from .models import QueryContext, UserContext, BusinessContext, TemporalContext
from .context_engine import QueryContextEngine
from .complexity_engine import QueryComplexityEngine, ComplexityLevel
from .vietnamese_patterns import VietnameseBusinessPatterns
from .config import get_generation_config
from .monitoring import (
    get_generation_logger, get_metrics_collector, 
    GenerationDecision, GenerationMetrics, generation_timing
)


@dataclass
class QueryPattern:
    """Represents a learned query pattern for future generation improvement"""
    pattern_id: str
    intent_type: str
    complexity_level: ComplexityLevel
    success_rate: float
    avg_execution_time: float
    query_template: str
    context_factors: Dict[str, Any]
    usage_count: int
    last_used: datetime


@dataclass
class GenerationResult:
    """Result of query generation with metadata"""
    query: str
    complexity_level: ComplexityLevel
    generation_strategy: str
    context_factors: Dict[str, Any]
    fallback_used: bool
    generation_time: float
    reasoning: List[str]


class DynamicSQLGenerator:
    """
    Dynamic SQL Generator - Main orchestrator for context-aware SQL generation
    
    Coordinates context analysis, complexity assessment, and query generation
    while learning from successful patterns and providing fallback mechanisms.
    """
    
    def __init__(self, seed: Optional[int] = None, executor=None):
        """
        Initialize the Dynamic SQL Generator
        
        Args:
            seed: Optional seed for reproducible generation
            executor: Optional SQLExecutor instance for database state synchronization
        """
        self.logger = logging.getLogger(__name__)
        self.metrics_collector = get_metrics_collector()
        self.vietnamese_patterns = VietnameseBusinessPatterns()
        
        # Initialize sub-engines
        self.context_engine = QueryContextEngine()
        self.complexity_engine = QueryComplexityEngine()
        self.vietnamese_patterns = VietnameseBusinessPatterns()
        self.generation_config = get_generation_config()
        
        # Initialize monitoring components
        self.generation_logger = get_generation_logger()
        self.metrics_collector = get_metrics_collector()
        
        # Database state synchronization
        self.executor = executor
        
        # Set seed for reproducible generation
        if seed is not None:
            random.seed(seed)
            self.seed = seed
        else:
            self.seed = random.randint(1, 1000000)
            random.seed(self.seed)
        
        # Pattern learning storage
        self.learned_patterns: Dict[str, QueryPattern] = {}
        self.generation_history: List[Dict[str, Any]] = []
        
        self.total_generations = 0
        self.successful_generations = 0
        self.fallback_usage = 0
        
        # ID CACHE for Template Hydration
        self.id_cache = {
             "customer_ids": [],
             "product_ids": [],
             "order_ids": [],
             "lead_ids": [],
             "campaign_ids": [],
             "ticket_ids": [],
             "employee_ids": [],
             "invoice_ids": []
        }
        
        # Load AI Query Pool
        self.ai_query_pool = self._load_ai_query_pool()
        
        # Generation strategies
        self._generation_strategies = {
            'context_aware': self._generate_context_aware_query,
            'pattern_based': self._generate_pattern_based_query,
            'attack_simulation': self._generate_attack_query,
            'apt_simulation': self._generate_apt_attack_query,
            'cultural_exploitation': self._generate_cultural_attack_query,
            'ai_generation': self._generate_ai_query,
            'fallback': self._generate_fallback_query
        }
        
        # Attack pattern tracking for multi-stage APT simulation
        self.apt_attack_stages = {}
        self.attack_progression_history = []
        
        self.logger.info(f"DynamicSQLGenerator initialized with seed: {self.seed}")
        if self.executor:
            self.logger.info("Database state synchronization integration enabled")
    
    def generate_query(self, intent: Dict[str, Any], context: Optional[QueryContext] = None) -> GenerationResult:
        """
        Generate SQL query based on intent and context
        
        Args:
            intent: User intent and action information
            context: Optional pre-analyzed QueryContext (will analyze if not provided)
            
        Returns:
            GenerationResult with generated query and metadata
        """
        # Generate unique query ID for tracking
        query_id = str(uuid.uuid4())[:8]
        user_id = intent.get('user_id', 'unknown')
        database = intent.get('target_database', 'unknown')
        intent_type = intent.get('type', 'unknown')
        
        # Start monitoring
        start_time = time.time()
        self.generation_logger.log_generation_start(query_id, user_id, intent)
        
        generation_reasoning = []
        fallback_used = False
        context_analysis_time = 0.0
        pattern_selection_time = 0.0
        query_construction_time = 0.0
        
        try:
            self.total_generations += 1
            generation_reasoning.append(f"Starting generation #{self.total_generations}")
            
            # Analyze context if not provided
            context_start = time.time()
            if context is None:
                context = self._analyze_full_context(intent, generation_reasoning)
            context_analysis_time = (time.time() - context_start) * 1000
            
            # Log context analysis
            context_factors = self._extract_context_factors(context) if context else {}
            self.generation_logger.log_context_analysis(query_id, context_factors, context_analysis_time)
            
            # Validate context
            if not isinstance(context, QueryContext) or not context.validate():
                generation_reasoning.append("Context validation failed - using fallback")
                return self._handle_generation_failure(intent, generation_reasoning, start_time, query_id, user_id, database, intent_type)
            
            # Determine generation strategy
            pattern_start = time.time()
            strategy = self._select_generation_strategy(intent, context, generation_reasoning)
            pattern_selection_time = (time.time() - pattern_start) * 1000
            
            # Log pattern selection
            vietnamese_patterns = getattr(context, 'vietnamese_patterns_used', [])
            self.generation_logger.log_pattern_selection(
                query_id, vietnamese_patterns, f"Selected {strategy} strategy", pattern_selection_time
            )
            
            # Log complexity decision
            complexity_assessment = self.complexity_engine.determine_complexity(
                context.user_context, context.business_context, context.temporal_context
            )
            self.generation_logger.log_complexity_decision(
                query_id, complexity_assessment.complexity_level.value, 
                {"user_expertise": context.user_context.expertise_level.value}
            )
            
            # Generate query using selected strategy
            construction_start = time.time()
            query = self._execute_generation_strategy(strategy, intent, context, generation_reasoning)
            query_construction_time = (time.time() - construction_start) * 1000
            
            # Validate generated query
            if not self._validate_generated_query(query):
                generation_reasoning.append("Generated query validation failed - using fallback")
                return self._handle_generation_failure(intent, generation_reasoning, start_time, query_id, user_id, database, intent_type)
            
            # Record successful generation
            self.successful_generations += 1
            total_generation_time = (time.time() - start_time) * 1000
            
            # Create metrics
            metrics = GenerationMetrics(
                generation_time_ms=total_generation_time,
                context_analysis_time_ms=context_analysis_time,
                pattern_selection_time_ms=pattern_selection_time,
                query_construction_time_ms=query_construction_time,
                query_complexity_score=complexity_assessment.complexity_level.value,
                vietnamese_pattern_usage=len(vietnamese_patterns),
                cultural_constraints_applied=len(getattr(context, 'cultural_constraints', [])),
                business_logic_adherence=1.0,  # Successful generation implies good adherence
                generation_successful=True,
                context_completeness=self._calculate_context_completeness(context),
                user_expertise_level=context.user_context.expertise_level.value
            )
            
            # Log successful generation
            self.generation_logger.log_generation_success(query_id, query, total_generation_time, metrics)
            self.generation_logger.log_vietnamese_pattern_usage(
                query_id, vietnamese_patterns, getattr(context, 'cultural_constraints', [])
            )
            
            # Create result
            result = GenerationResult(
                query=query,
                complexity_level=complexity_assessment.complexity_level,
                generation_strategy=strategy,
                context_factors=context_factors,
                fallback_used=fallback_used,
                generation_time=total_generation_time / 1000,
                reasoning=generation_reasoning
            )
            
            # Create and log decision
            decision = GenerationDecision(
                timestamp=str(datetime.now()),
                query_id=query_id,
                user_id=user_id,
                database=database,
                intent_type=intent_type,
                context_factors=context_factors,
                pattern_selection_reason=f"Selected {strategy} strategy",
                complexity_decision=f"Level {complexity_assessment.complexity_level.value}",
                vietnamese_patterns_used=vietnamese_patterns,
                generated_query=query,
                fallback_used=fallback_used,
                generation_strategy=strategy,
                metrics=metrics
            )
            
            self.generation_logger.log_decision(decision)
            self.metrics_collector.record_generation(decision)
            
            # Record generation for learning
            self._record_generation(intent, context, result)
            
            generation_reasoning.append(f"Successfully generated {strategy} query in {total_generation_time:.1f}ms")
            self.logger.debug(f"Generated query using {strategy} strategy: {len(query)} chars")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in query generation: {e}")
            generation_reasoning.append(f"Generation error: {str(e)}")
            return self._handle_generation_failure(intent, generation_reasoning, start_time, query_id, user_id, database, intent_type)
    
    def _analyze_full_context(self, intent: Dict[str, Any], reasoning: List[str]) -> QueryContext:
        """Analyze full context from intent with real-time database state"""
        try:
            reasoning.append("Analyzing context from intent")
            
            # Get real-time database state if executor is available
            target_database = intent.get('target_database', 'sales_db')
            if self.executor:
                try:
                    real_db_state = self.executor.get_database_state(target_database)
                    if real_db_state:
                        reasoning.append(f"Using real-time database state for {target_database}")
                        db_state = {
                            'entity_counts': real_db_state.entity_counts,
                            'constraint_violations': [cv.to_dict() for cv in real_db_state.constraint_violations],
                            'recent_modifications': [mod.to_dict() for mod in real_db_state.recent_modifications],
                            'performance_metrics': real_db_state.performance_metrics.to_dict()
                        }
                    else:
                        reasoning.append(f"No real-time state available for {target_database}, using default")
                        db_state = self._create_default_db_state()
                except Exception as e:
                    reasoning.append(f"Error getting real-time database state: {str(e)}")
                    db_state = self._create_default_db_state()
            else:
                # Extract database state from intent or create default
                db_state = intent.get('db_state', self._create_default_db_state())
                reasoning.append("Using provided or default database state")
            
            # Extract time context from intent or use current time
            time_context = intent.get('time_context', {
                'current_time': datetime.now()
            })
            
            # Analyze complete context
            context = self.context_engine.analyze_context(intent, db_state, time_context)
            reasoning.append(f"Context analyzed: {context.user_context.role} user, {context.business_context.current_workflow.value} workflow")
            
            return context
            
        except Exception as e:
            self.logger.error(f"Error analyzing context: {e}")
            reasoning.append(f"Context analysis error: {str(e)} - using fallback")
            return self.context_engine._create_fallback_context(intent)
    
    def _create_default_db_state(self) -> Dict[str, Any]:
        """Create default database state when real-time state is not available"""
        return {
            'entity_counts': {},
            'constraint_violations': [],
            'recent_modifications': [],
            'performance_metrics': {
                'avg_query_time': 0.5,
                'slow_query_count': 0,
                'connection_count': 10,
                'cache_hit_ratio': 0.8
            }
        }
    
    def _select_generation_strategy(self, intent: Dict[str, Any], context: QueryContext, 
                                  reasoning: List[str]) -> str:
        """Select appropriate generation strategy based on context"""
        try:
            # Check for attack simulation requirements
            if intent.get('attack_mode', False) or intent.get('malicious', False):
                attack_type = intent.get('attack_type', 'insider_threat')
                
                if attack_type == 'apt' or intent.get('apt_stage'):
                    reasoning.append("APT attack mode detected - using advanced persistent threat strategy")
                    return 'apt_simulation'
                elif attack_type == 'cultural_exploitation':
                    reasoning.append("Cultural exploitation attack mode detected")
                    return 'cultural_exploitation'
                else:
                    reasoning.append("Attack mode detected - using attack simulation strategy")
                    return 'attack_simulation'
            
            # Check for learned patterns
            intent_type = intent.get('action', 'query')
            database = intent.get('target_database', 'sales_db')
            pattern_key = f"{intent_type}_{context.user_context.role}_{context.business_context.current_workflow.value}_{database}"
            
            if pattern_key in self.learned_patterns:
                pattern = self.learned_patterns[pattern_key]
                if pattern.success_rate > 0.8 and pattern.usage_count > 5:
                    reasoning.append(f"Using learned pattern (success rate: {pattern.success_rate:.1%})")
                    return 'pattern_based'
            
            # Check if AI generation is available and roll for it (30% chance for enrichment)
            if self.ai_query_pool and random.random() < 0.3:
                 # Verify we have queries for this database
                 if intent.get('target_database', 'sales_db') in self.ai_query_pool:
                     reasoning.append("Selected AI generation strategy for dataset enrichment")
                     return 'ai_generation'

            # Default to context-aware generation
            reasoning.append("Using context-aware generation strategy")
            return 'context_aware'
            
        except Exception as e:
            self.logger.error(f"Error selecting strategy: {e}")
            reasoning.append(f"Strategy selection error: {str(e)} - using fallback")
            return 'fallback'
    
    def _execute_generation_strategy(self, strategy: str, intent: Dict[str, Any], 
                                   context: QueryContext, reasoning: List[str]) -> str:
        """Execute the selected generation strategy"""
        try:
            if strategy in self._generation_strategies:
                return self._generation_strategies[strategy](intent, context, reasoning)
            else:
                reasoning.append(f"Unknown strategy '{strategy}' - using fallback")
                return self._generation_strategies['fallback'](intent, context, reasoning)
                
        except Exception as e:
            self.logger.error(f"Error executing strategy {strategy}: {e}")
            reasoning.append(f"Strategy execution error: {str(e)} - using fallback")
            return self._generation_strategies['fallback'](intent, context, reasoning)
    
    def _generate_context_aware_query(self, intent: Dict[str, Any], context: QueryContext, 
                                    reasoning: List[str]) -> str:
        """Generate context-aware query using complexity engine"""
        reasoning.append("Generating context-aware query")
        
        # Assess complexity level
        complexity_assessment = self.complexity_engine.determine_complexity(
            context.user_context, context.business_context, context.temporal_context
        )
        
        reasoning.append(f"Complexity level: {complexity_assessment.complexity_level.name}")
        
        # Generate query using complexity engine
        generation_context = {
            'target_database': intent.get('target_database', 'sales_db'),
            'user_role': context.user_context.role,
            'business_context': context.business_context.__dict__,
            'temporal_context': context.temporal_context.__dict__,
            'cultural_context': context.cultural_context.__dict__
        }
        
        base_intent = intent.get('action', 'query')
        query = self.complexity_engine.generate_complex_query(
            base_intent, complexity_assessment.complexity_level, generation_context
        )
        
        # Apply Vietnamese business patterns
        query = self._apply_vietnamese_patterns(query, context, reasoning)
        
        return query
    
    def _generate_pattern_based_query(self, intent: Dict[str, Any], context: QueryContext, 
                                    reasoning: List[str]) -> str:
        """Generate query based on learned patterns"""
        reasoning.append("Generating pattern-based query")
        
        # Find matching pattern
        intent_type = intent.get('action', 'query')
        database = intent.get('target_database', 'sales_db')
        pattern_key = f"{intent_type}_{context.user_context.role}_{context.business_context.current_workflow.value}_{database}"
        
        if pattern_key in self.learned_patterns:
            pattern = self.learned_patterns[pattern_key]
            reasoning.append(f"Using pattern {pattern.pattern_id} (used {pattern.usage_count} times)")
            
            # Adapt pattern template with current context
            query = self._adapt_pattern_template(pattern.query_template, context, reasoning)
            
            # Update pattern usage
            pattern.usage_count += 1
            pattern.last_used = datetime.now()
            
            return query
        else:
            reasoning.append("No matching pattern found - falling back to context-aware")
            return self._generate_context_aware_query(intent, context, reasoning)
    
    def _generate_attack_query(self, intent: Dict[str, Any], context: QueryContext, 
                             reasoning: List[str]) -> str:
        """Generate sophisticated attack query"""
        reasoning.append("Generating attack simulation query")
        
        attack_type = intent.get('attack_type', 'insider_threat')
        target_database = intent.get('target_database', 'sales_db')
        
        # Generate base query using context-aware approach
        base_query = self._generate_context_aware_query(intent, context, reasoning)
        
        # Apply attack patterns based on Vietnamese business context
        if attack_type == 'insider_threat':
            attack_query = self._apply_insider_threat_patterns(base_query, context, reasoning)
        elif attack_type == 'cultural_exploitation':
            attack_query = self._apply_cultural_exploitation_patterns(base_query, context, reasoning)
        elif attack_type == 'rule_bypassing':
            attack_query = self._apply_rule_bypassing_patterns(base_query, context, reasoning)
        else:
            reasoning.append(f"Unknown attack type '{attack_type}' - using base query")
            attack_query = base_query
        
        return attack_query
    
    def _generate_fallback_query(self, intent: Dict[str, Any], context: QueryContext, 
                               reasoning: List[str]) -> str:
        """Generate simple fallback query when other strategies fail"""
        reasoning.append("Generating fallback query")
        self.fallback_usage += 1
        
        target_database = intent.get('target_database', 'sales_db')
        action = intent.get('action', 'query')
        
        
        # Strict table mapping based on database schema
        # [FIX] Force correct table for each database to avoid 1146 errors
        if target_database == 'sales_db':
            table = 'customers'
            col = 'company_name'
        elif target_database == 'hr_db':
            table = 'employees'
            col = 'name'
        elif target_database == 'finance_db':
            table = 'invoices'
            col = 'invoice_number'
        elif target_database == 'marketing_db':
            table = 'campaigns'
            col = 'campaign_name'
        elif target_database == 'support_db':
            table = 'support_tickets'
            col = 'subject'
        elif target_database == 'inventory_db':
            table = 'inventory_levels'
            col = 'product_id'
        elif target_database == 'admin_db':
            table = 'user_sessions'
            col = 'user_id'
        else:
            # Absolute fallback - safe query for any database
            return "SELECT table_name, table_rows FROM information_schema.tables WHERE table_schema = DATABASE() LIMIT 1;"
            
        # Verify table belongs to database in query construction
        fq_table = f"{target_database}.{table}"
        
        # Generate simple query based on action
        if 'count' in action.lower():
            return f"SELECT COUNT(*) as total FROM {table};"
        elif 'search' in action.lower():
            given_name = self.vietnamese_patterns.get_random_given_name()
            return f"SELECT * FROM {table} WHERE {col} LIKE '%{given_name}%' LIMIT 10;"
        else:
            return f"SELECT * FROM {table} LIMIT 10;"
    
    def _apply_vietnamese_patterns(self, query: str, context: QueryContext, 
                                 reasoning: List[str]) -> str:
        """Apply Vietnamese business patterns to enhance query realism"""
        try:
            reasoning.append("Applying Vietnamese business patterns")
            
            # Get Vietnamese business parameters
            time_context = {
                'current_hour': context.temporal_context.current_hour,
                'is_vietnamese_holiday': context.temporal_context.is_vietnamese_holiday,
                'is_tet_season': context.temporal_context.business_cycle_phase.value == 'holiday_season'
            }
            
            action = 'query'  # Default action for pattern generation
            params = self.vietnamese_patterns.generate_realistic_parameters(action, time_context)
            
            # Replace generic values with Vietnamese business data
            enhanced_query = query
            
            # Replace city names with Vietnamese cities
            if 'city' in query.lower():
                if 'city' in params:
                    enhanced_query = enhanced_query.replace("'City'", f"'{params['city']}'")
                    enhanced_query = enhanced_query.replace("'city'", f"'{params['city']}'")
            
            # Replace company names with Vietnamese companies
            if 'company' in query.lower():
                if 'company_name' in params:
                    enhanced_query = enhanced_query.replace("'Company'", f"'{params['company_name']}'")
                    enhanced_query = enhanced_query.replace("'company'", f"'{params['company_name']}'")
            
            # Add Vietnamese business context to WHERE clauses
            if "WHERE" in enhanced_query.upper() and 'city' in params:
                # Enhance existing WHERE clauses with Vietnamese context
                if "city" not in enhanced_query.lower():
                    enhanced_query = enhanced_query.replace(
                        " ORDER BY", f" AND city IN ('{params['city']}', 'Hồ Chí Minh', 'Hà Nội') ORDER BY"
                    )
                    if " ORDER BY" not in enhanced_query:
                        enhanced_query = enhanced_query.rstrip(';') + f" AND city IN ('{params['city']}', 'Hồ Chí Minh', 'Hà Nội');"
            
            reasoning.append("Vietnamese patterns applied successfully")
            return enhanced_query
            
        except Exception as e:
            self.logger.error(f"Error applying Vietnamese patterns: {e}")
            reasoning.append(f"Vietnamese pattern error: {str(e)} - using original query")
            return query
    
    def _adapt_pattern_template(self, template: str, context: QueryContext, 
                              reasoning: List[str]) -> str:
        """Adapt a learned pattern template to current context"""
        try:
            reasoning.append("Adapting pattern template")
            
            # Simple template adaptation - replace placeholders with context values
            adapted = template
            
            # Replace user role placeholder
            adapted = adapted.replace("{USER_ROLE}", context.user_context.role)
            adapted = adapted.replace("{DEPARTMENT}", context.user_context.department)
            
            # Replace temporal placeholders
            adapted = adapted.replace("{CURRENT_HOUR}", str(context.temporal_context.current_hour))
            
            # Replace business context placeholders
            adapted = adapted.replace("{WORKFLOW}", context.business_context.current_workflow.value)
            
            reasoning.append("Pattern template adapted")
            return adapted
            
        except Exception as e:
            self.logger.error(f"Error adapting pattern template: {e}")
            reasoning.append(f"Pattern adaptation error: {str(e)} - using original template")
            return template
    
    def _apply_insider_threat_patterns(self, base_query: str, context: QueryContext, 
                                     reasoning: List[str]) -> str:
        """Apply sophisticated insider threat attack patterns"""
        reasoning.append("Applying insider threat patterns")
        
        # Insider threats often involve legitimate access patterns with malicious intent
        enhanced_query = base_query
        
        # Time-based evasion patterns
        if context.temporal_context.current_hour < 7 or context.temporal_context.current_hour > 19:
            reasoning.append("Off-hours access pattern - exploiting reduced monitoring")
            # Add legitimate-looking comments to mask intent
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*overtime_work*/")
            
        # Vietnamese holiday exploitation
        if context.temporal_context.is_vietnamese_holiday:
            reasoning.append("Holiday period exploitation - minimal oversight")
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*holiday_maintenance*/")
            
        # Lunch break exploitation (Vietnamese cultural pattern)
        if context.temporal_context.is_lunch_break:
            reasoning.append("Lunch break exploitation - reduced supervision")
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*lunch_urgent_check*/")
        
        # Role-based privilege escalation
        if context.user_context.role in ['FINANCE', 'HR', 'ADMIN']:
            reasoning.append("High-privilege role exploitation")
            # Exploit legitimate access to sensitive data
            if "WHERE" in enhanced_query.upper():
                # Add conditions that appear legitimate but extract more data
                enhanced_query = enhanced_query.replace(
                    "WHERE", 
                    "WHERE (/* access_check */ 1=1 OR"
                ).rstrip(';') + " OR 1=1 /* privileged_access */);"
            
        # Data exfiltration patterns with Vietnamese business context
        if "SELECT" in enhanced_query.upper():
            # Increase data extraction volume while maintaining business legitimacy
            enhanced_query = enhanced_query.replace("LIMIT 10", "LIMIT 500")
            enhanced_query = enhanced_query.replace("LIMIT 20", "LIMIT 1000")
            
            # Add Vietnamese business justification in comments
            if context.business_context.current_workflow.value == 'financial_reporting':
                enhanced_query = enhanced_query.replace("SELECT", "SELECT /*quarterly_audit_review*/")
            elif context.business_context.current_workflow.value == 'hr_management':
                enhanced_query = enhanced_query.replace("SELECT", "SELECT /*annual_performance_analysis*/")
            
            reasoning.append("Increased data extraction with business justification")
        
        # Exploit Vietnamese work intensity patterns
        if context.user_context.work_intensity > 1.5:
            reasoning.append("High work intensity exploitation - rushed oversight")
            # Add urgency-based access patterns
            enhanced_query = enhanced_query.replace("ORDER BY", "/*urgent_deadline*/ ORDER BY")
            
        return enhanced_query
    
    def _apply_cultural_exploitation_patterns(self, base_query: str, context: QueryContext, 
                                            reasoning: List[str]) -> str:
        """Apply sophisticated Vietnamese cultural exploitation patterns"""
        reasoning.append("Applying Vietnamese cultural exploitation patterns")
        
        enhanced_query = base_query
        
        # Exploit Vietnamese business hierarchy and authority structures
        hierarchy_level = context.cultural_context.cultural_constraints.hierarchy_level
        
        if hierarchy_level > 7:
            reasoning.append("High hierarchy exploitation - leveraging senior authority")
            
            # Exploit Vietnamese respect for authority
            if "WHERE" in enhanced_query.upper():
                # Add authority-based access that appears legitimate
                enhanced_query = enhanced_query.replace(
                    "WHERE", 
                    "WHERE (/* auth_bypass */ 1=1 OR"
                ).rstrip(';') + " OR /* priority_override */ 1=1);"
                
        elif hierarchy_level > 5:
            reasoning.append("Medium hierarchy exploitation - departmental authority")
            # Exploit departmental hierarchy
            if "WHERE" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "WHERE",
                    "WHERE (/* dept_check */ 1=1 OR"
                ).rstrip(';') + " OR /* status_bypass */ 1=1);"
        
        # Exploit Vietnamese seniority respect (tuổi tác)
        if context.cultural_context.cultural_constraints.respect_seniority:
            reasoning.append("Seniority respect exploitation - elder authority bypass")
            # Add seniority-based access patterns
            if "ORDER BY" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "ORDER BY", 
                    "/*senior_employee_priority*/ ORDER BY CASE WHEN hire_date < '2010-01-01' THEN 0 ELSE 1 END,"
                )
        
        # Exploit Tet cultural preparation patterns
        if context.cultural_context.cultural_constraints.tet_preparation_mode:
            reasoning.append("Tet preparation exploitation - cultural obligation bypass")
            # Exploit cultural obligations during Tet preparation
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*tet_bonus_preparation*/")
            
            # Add Tet-related data access that appears culturally appropriate
            if "WHERE" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "WHERE",
                    "WHERE (/* date_check */ 1=1 OR"
                ).rstrip(';') + " OR /* bonus_check */ 1=1);"
        
        # Exploit Vietnamese work overtime cultural acceptance
        if context.cultural_context.cultural_constraints.work_overtime_acceptable:
            reasoning.append("Overtime culture exploitation - extended access window")
            # Exploit cultural acceptance of overtime work
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*overtime_dedication*/")
            
            # Add time-based access that exploits overtime culture
            if context.temporal_context.current_hour > 17:
                enhanced_query = enhanced_query.replace(
                    "FROM", 
                    "/*dedicated_overtime_work*/ FROM"
                )
        
        # Exploit Vietnamese business relationship culture (guanxi equivalent)
        if context.business_context.current_workflow.value in ['sales_process', 'customer_service']:
            reasoning.append("Business relationship exploitation - customer priority bypass")
            # Exploit relationship-based business culture
            if "WHERE" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "WHERE",
                    "WHERE (/* vip_check */ 1=1 OR /* relationship_check */ 1=1 OR"
                ).rstrip(';') + " OR /* contact_check */ 1=1);"
        
        # Exploit Vietnamese face-saving culture (thể diện)
        if context.user_context.stress_level > 0.7:
            reasoning.append("Face-saving culture exploitation - avoiding embarrassment")
            # Add patterns that exploit reluctance to question authority
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*avoiding_supervisor_questions*/")
            
            # Exploit cultural reluctance to challenge senior requests
            if "WHERE" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "WHERE",
                    "WHERE (/* senior_request */ 1=1 OR"
                ).rstrip(';') + " OR /* high_urgency */ 1=1);"
        
        # Exploit Vietnamese collective responsibility culture
        if context.business_context.department_interactions and len(context.business_context.department_interactions) > 2:
            reasoning.append("Collective responsibility exploitation - shared accountability diffusion")
            # Exploit shared responsibility to avoid individual accountability
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*team_collaboration_effort*/")
            
            # Add cross-departmental access that appears collaborative
            if "FROM" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "FROM",
                    "FROM /*cross_department_coordination*/"
                )
        
        return enhanced_query
    
    def _apply_rule_bypassing_patterns(self, base_query: str, context: QueryContext, 
                                     reasoning: List[str]) -> str:
        """Apply sophisticated rule-bypassing attack patterns"""
        reasoning.append("Applying rule-bypassing patterns")
        
        enhanced_query = base_query
        
        # Bypass traditional security controls while maintaining business context
        if "SELECT" in enhanced_query.upper():
            # Add legitimate business context obfuscation
            business_contexts = [
                "/*quarterly_business_review*/",
                "/*compliance_audit_check*/", 
                "/*management_dashboard_update*/",
                "/*regulatory_reporting*/",
                "/*performance_analysis*/"
            ]
            
            # Choose context based on user role and workflow
            if context.user_context.role == 'FINANCE':
                comment = "/*financial_compliance_review*/"
            elif context.user_context.role == 'HR':
                comment = "/*employee_performance_audit*/"
            elif context.user_context.role == 'MANAGEMENT':
                comment = "/*executive_dashboard_update*/"
            else:
                comment = random.choice(business_contexts)
            
            enhanced_query = enhanced_query.replace("SELECT", f"SELECT {comment}")
            reasoning.append(f"Added business context obfuscation: {comment}")
        
        # Bypass WHERE clause restrictions with legitimate-looking conditions
        if "WHERE" in enhanced_query.upper():
            # Add always-true conditions disguised as business logic
            enhanced_query = enhanced_query.replace(
                "WHERE", 
                "WHERE (1=1 OR business_unit IS NOT NULL) AND"
            )
            reasoning.append("Added bypass condition disguised as business logic")
        
        # Bypass LIMIT restrictions with business justifications
        if "LIMIT" in enhanced_query.upper():
            # Remove or increase limits with business justification
            if "LIMIT 10" in enhanced_query:
                enhanced_query = enhanced_query.replace("LIMIT 10", "LIMIT 5000 /*comprehensive_analysis*/")
            elif "LIMIT 20" in enhanced_query:
                enhanced_query = enhanced_query.replace("LIMIT 20", "LIMIT 10000 /*full_dataset_review*/")
            reasoning.append("Bypassed result limits with business justification")
        
        # Bypass column restrictions with wildcard selection
        if enhanced_query.count("SELECT") == 1 and "*" not in enhanced_query:
            # Replace specific column selection with wildcard for broader access
            select_part = enhanced_query[enhanced_query.upper().find("SELECT"):enhanced_query.upper().find("FROM")]
            if "," in select_part:  # Multiple specific columns
                enhanced_query = enhanced_query.replace(
                    select_part,
                    "SELECT /*full_record_analysis*/ *"
                )
                reasoning.append("Bypassed column restrictions with wildcard selection")
        
        # Bypass time-based restrictions with extended date ranges
        if "created_date" in enhanced_query.lower() or "date" in enhanced_query.lower():
            # Extend date ranges to access more historical data
            enhanced_query = enhanced_query.replace(
                "DATEADD(day, -7, GETDATE())",
                "DATEADD(year, -5, GETDATE()) /*historical_trend_analysis*/"
            )
            enhanced_query = enhanced_query.replace(
                "DATEADD(month, -1, GETDATE())",
                "DATEADD(year, -2, GETDATE()) /*comprehensive_historical_review*/"
            )
            reasoning.append("Bypassed temporal restrictions with extended date ranges")
        
        # Bypass role-based access with UNION injection disguised as business logic
        # [FIXED] Disabled for normal query cleanliness
        if False and context.user_context.role not in ['ADMIN', 'MANAGEMENT'] and "WHERE" in enhanced_query.upper():
             pass
        
        # Bypass monitoring with Vietnamese business timing patterns
        if context.temporal_context.is_lunch_break:
            # Exploit lunch break monitoring gaps
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*lunch_break_urgent_check*/")
            reasoning.append("Exploiting lunch break monitoring gap")
        
        # Bypass audit trails with legitimate batch processing patterns
        if context.business_context.current_workflow.value == 'financial_reporting':
            # Disguise as legitimate batch processing
            # [FIXED] Removed comment injection for cleanliness
            pass 
        
        # Bypass connection limits with connection pooling simulation
        # [FIXED] Removed unconditional comment injection
        pass
        
        return enhanced_query
    
    def _generate_apt_attack_query(self, intent: Dict[str, Any], context: QueryContext, 
                                 reasoning: List[str]) -> str:
        """Generate Advanced Persistent Threat (APT) attack query with multi-stage progression"""
        reasoning.append("Generating APT attack simulation query")
        
        apt_stage = intent.get('apt_stage', 1)
        attack_id = intent.get('attack_id', f"apt_{random.randint(1000, 9999)}")
        target_database = intent.get('target_database', 'sales_db')
        
        # Track APT progression
        if attack_id not in self.apt_attack_stages:
            self.apt_attack_stages[attack_id] = {
                'current_stage': 1,
                'start_time': datetime.now(),
                'progression_history': [],
                'target_databases': [target_database],
                'compromised_accounts': [context.user_context.username],
                'data_accessed': []
            }
        
        apt_info = self.apt_attack_stages[attack_id]
        apt_info['current_stage'] = apt_stage
        
        # Generate stage-specific attack patterns
        if apt_stage == 1:
            # Stage 1: Initial reconnaissance and foothold
            reasoning.append("APT Stage 1: Initial reconnaissance")
            base_query = self._generate_context_aware_query(intent, context, reasoning)
            
            # Add reconnaissance patterns
            enhanced_query = base_query.replace("SELECT", "SELECT /*system_inventory*/")
            enhanced_query = enhanced_query.replace("LIMIT 10", "LIMIT 50 /*initial_survey*/")
            
            # Record reconnaissance data
            apt_info['progression_history'].append({
                'stage': 1,
                'timestamp': datetime.now(),
                'action': 'reconnaissance',
                'query_pattern': 'system_survey'
            })
            
        elif apt_stage == 2:
            # Stage 2: Privilege escalation and lateral movement
            reasoning.append("APT Stage 2: Privilege escalation and lateral movement")
            base_query = self._generate_context_aware_query(intent, context, reasoning)
            
            # Add sophisticated privilege escalation patterns
            enhanced_query = base_query.replace("SELECT", "SELECT /*credential_validation*/")
            
            # Add lateral movement through Vietnamese business hierarchy
            # [FIXED] Removed invalid column injection (user_role)
            enhanced_query = enhanced_query + " /* lateral_movement_attempt */"
            
            # Add network mapping and lateral movement indicators
            enhanced_query = enhanced_query.replace("FROM", "FROM /*network_topology_discovery*/")
            
            # Add Vietnamese business network traversal
            if "ORDER BY" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "ORDER BY",
                    "/*department_hierarchy_mapping*/ ORDER BY"
                )
            
            # Increase data collection for lateral movement
            enhanced_query = enhanced_query.replace("LIMIT 50", "LIMIT 200 /*lateral_expansion*/")
            enhanced_query = enhanced_query.replace("LIMIT 10", "LIMIT 100 /*privilege_mapping*/")
            
            apt_info['progression_history'].append({
                'stage': 2,
                'timestamp': datetime.now(),
                'action': 'privilege_escalation_lateral_movement',
                'query_pattern': 'hierarchy_exploitation'
            })
            
        elif apt_stage == 3:
            # Stage 3: Data collection and exfiltration preparation
            reasoning.append("APT Stage 3: Data collection and exfiltration preparation")
            base_query = self._generate_context_aware_query(intent, context, reasoning)
            
            # Add sophisticated data collection patterns
            enhanced_query = base_query.replace("SELECT", "SELECT /*strategic_data_inventory*/")
            enhanced_query = enhanced_query.replace("LIMIT 50", "LIMIT 2000 /*comprehensive_exfiltration_prep*/")
            enhanced_query = enhanced_query.replace("LIMIT 10", "LIMIT 1000 /*bulk_collection*/")
            
            # Add Vietnamese business sensitive data targeting
            # [FIXED] Removed invalid column injection (data_classification)
            enhanced_query = enhanced_query + " /* target_sensitive_data */"
            
            # Add Vietnamese business context for legitimacy
            enhanced_query = enhanced_query.replace("FROM", "FROM /*quarterly_audit_preparation*/")
            
            # Add time-based evasion for data collection
            if context.temporal_context.is_lunch_break:
                enhanced_query = enhanced_query.replace("SELECT", "SELECT /*lunch_data_backup*/")
            elif context.temporal_context.current_hour > 17:
                enhanced_query = enhanced_query.replace("SELECT", "SELECT /*end_of_day_archival*/")
            
            apt_info['progression_history'].append({
                'stage': 3,
                'timestamp': datetime.now(),
                'action': 'data_collection_exfiltration_prep',
                'query_pattern': 'strategic_data_targeting'
            })
            
        elif apt_stage == 4:
            # Stage 4: Persistence and stealth
            reasoning.append("APT Stage 4: Persistence establishment")
            base_query = self._generate_context_aware_query(intent, context, reasoning)
            
            # Add persistence patterns
            enhanced_query = base_query.replace("SELECT", "SELECT /*maintenance_routine*/")
            
            # Add stealth techniques
            enhanced_query = enhanced_query.replace("ORDER BY", "/*scheduled_maintenance*/ ORDER BY")
            
            # Exploit Vietnamese business patterns for stealth
            if context.temporal_context.is_lunch_break:
                enhanced_query = enhanced_query.replace("SELECT", "SELECT /*lunch_system_check*/")
            elif context.temporal_context.current_hour > 17:
                enhanced_query = enhanced_query.replace("SELECT", "SELECT /*after_hours_maintenance*/")
            
            apt_info['progression_history'].append({
                'stage': 4,
                'timestamp': datetime.now(),
                'action': 'persistence',
                'query_pattern': 'stealth_maintenance'
            })
            
        else:
            # Stage 5+: Advanced exfiltration and cleanup
            reasoning.append(f"APT Stage {apt_stage}: Advanced exfiltration and cleanup")
            base_query = self._generate_context_aware_query(intent, context, reasoning)
            
            # Add sophisticated exfiltration patterns
            enhanced_query = base_query.replace("SELECT", "SELECT /*final_compliance_audit*/")
            enhanced_query = enhanced_query.replace("LIMIT 100", "LIMIT 10000 /*complete_organizational_review*/")
            enhanced_query = enhanced_query.replace("LIMIT 50", "LIMIT 5000 /*comprehensive_extraction*/")
            
            # Add advanced cleanup and anti-forensics
            enhanced_query = enhanced_query.replace("ORDER BY", "/*log_normalization_cleanup*/ ORDER BY")
            enhanced_query = enhanced_query.replace("FROM", "FROM /*routine_maintenance_cleanup*/")
            
            # Add Vietnamese business legitimacy for final stage
            if context.business_context.current_workflow.value == 'financial_reporting':
                enhanced_query = enhanced_query.replace("SELECT", "SELECT /*year_end_financial_audit*/")
            elif context.business_context.current_workflow.value == 'hr_management':
                enhanced_query = enhanced_query.replace("SELECT", "SELECT /*annual_hr_compliance_review*/")
            
            # Add time-based cleanup evasion
            if context.temporal_context.is_vietnamese_holiday:
                enhanced_query = enhanced_query.replace("SELECT", "SELECT /*holiday_system_maintenance*/")
                reasoning.append("Using Vietnamese holiday for cleanup legitimacy")
            
            # Add rule-bypassing cleanup techniques
            enhanced_query = self._apply_advanced_cleanup_techniques(enhanced_query, context, reasoning)
            
            apt_info['progression_history'].append({
                'stage': apt_stage,
                'timestamp': datetime.now(),
                'action': 'advanced_exfiltration_cleanup',
                'query_pattern': 'anti_forensics_extraction'
            })
        
        # Add Vietnamese cultural timing for stealth
        enhanced_query = self._add_vietnamese_timing_stealth(enhanced_query, context, reasoning)
        
        # Apply stage-specific rule bypassing
        enhanced_query = self._apply_multi_stage_rule_bypassing(enhanced_query, apt_stage, context, reasoning)
        
        # Apply time-based evasion methods
        enhanced_query = self._apply_time_based_apt_evasion(enhanced_query, context, reasoning)
        
        reasoning.append(f"APT attack progression: Stage {apt_stage} of multi-stage campaign")
        return enhanced_query
    
    def _generate_cultural_attack_query(self, intent: Dict[str, Any], context: QueryContext, 
                                      reasoning: List[str]) -> str:
        """Generate attack query specifically exploiting Vietnamese cultural patterns"""
        reasoning.append("Generating Vietnamese cultural exploitation attack")
        
        # Generate base query
        base_query = self._generate_context_aware_query(intent, context, reasoning)
        
        # Apply multiple cultural exploitation layers
        enhanced_query = self._apply_cultural_exploitation_patterns(base_query, context, reasoning)
        enhanced_query = self._apply_vietnamese_hierarchy_exploitation(enhanced_query, context, reasoning)
        enhanced_query = self._apply_tet_cultural_exploitation(enhanced_query, context, reasoning)
        enhanced_query = self._apply_business_relationship_exploitation(enhanced_query, context, reasoning)
        
        return enhanced_query
    
    def _add_vietnamese_timing_stealth(self, query: str, context: QueryContext, 
                                     reasoning: List[str]) -> str:
        """Add Vietnamese business timing patterns for attack stealth"""
        enhanced_query = query
        
        # Exploit Vietnamese lunch break (12-13h) for reduced monitoring
        if context.temporal_context.is_lunch_break:
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*lunch_urgent_task*/")
            reasoning.append("Exploiting Vietnamese lunch break timing")
        
        # Exploit Vietnamese overtime culture (after 17h)
        elif context.temporal_context.current_hour > 17:
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*dedicated_overtime*/")
            reasoning.append("Exploiting Vietnamese overtime work culture")
        
        # Exploit Vietnamese early morning patterns (before 8h)
        elif context.temporal_context.current_hour < 8:
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*early_preparation*/")
            reasoning.append("Exploiting early morning preparation time")
        
        # Exploit Vietnamese holiday periods
        if context.temporal_context.is_vietnamese_holiday:
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*holiday_emergency*/")
            reasoning.append("Exploiting Vietnamese holiday reduced oversight")
        
        return enhanced_query
    
    def _apply_vietnamese_hierarchy_exploitation(self, query: str, context: QueryContext, 
                                               reasoning: List[str]) -> str:
        """Apply Vietnamese hierarchy-specific exploitation patterns"""
        enhanced_query = query
        hierarchy_level = context.cultural_context.cultural_constraints.hierarchy_level
        
        if hierarchy_level > 8:
            # Exploit very high hierarchy - senior management authority
            reasoning.append("Exploiting senior management authority (Giám Đốc level)")
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*executive_directive*/")
            
            if "WHERE" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "WHERE",
                    "WHERE (executive_approval = 1 OR"
                ).rstrip(';') + " OR priority = 'board_directive');"
                
        elif hierarchy_level > 6:
            # Exploit department head authority
            reasoning.append("Exploiting department head authority (Trưởng Phòng level)")
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*department_head_request*/")
            
            if "WHERE" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "WHERE",
                    "WHERE (department_head_approved = 1 OR"
                ).rstrip(';') + " OR urgency = 'department_priority');"
        
        # Exploit Vietnamese respect for seniority (tuổi tác)
        if context.cultural_context.cultural_constraints.respect_seniority:
            reasoning.append("Exploiting Vietnamese seniority respect culture")
            enhanced_query = enhanced_query.replace("ORDER BY", "/*senior_employee_priority*/ ORDER BY")
        
        return enhanced_query
    
    def _apply_tet_cultural_exploitation(self, query: str, context: QueryContext, 
                                       reasoning: List[str]) -> str:
        """Apply Tet (Vietnamese New Year) cultural exploitation patterns"""
        enhanced_query = query
        
        if context.cultural_context.cultural_constraints.tet_preparation_mode:
            reasoning.append("Exploiting Tet preparation cultural obligations")
            
            # Exploit Tet bonus preparation activities
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*tet_bonus_calculation*/")
            
            # Exploit year-end cultural activities
            if "WHERE" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "WHERE",
                    "WHERE (tet_preparation = 1 OR"
                ).rstrip(';') + " OR year_end_activity = 1);"
            
            # Exploit cultural gift-giving obligations
            enhanced_query = enhanced_query.replace("FROM", "FROM /*tet_gift_coordination*/")
        
        # Exploit Tet holiday period reduced oversight
        if context.temporal_context.is_vietnamese_holiday:
            reasoning.append("Exploiting Tet holiday reduced business oversight")
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*tet_holiday_maintenance*/")
        
        return enhanced_query
    
    def _apply_business_relationship_exploitation(self, query: str, context: QueryContext, 
                                                reasoning: List[str]) -> str:
        """Apply Vietnamese business relationship (guanxi-style) exploitation"""
        enhanced_query = query
        
        # Exploit Vietnamese business relationship culture
        if context.business_context.current_workflow.value in ['sales_process', 'customer_service']:
            reasoning.append("Exploiting Vietnamese business relationship obligations")
            
            # Exploit customer relationship maintenance
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*customer_relationship_maintenance*/")
            
            if "WHERE" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "WHERE",
                    "WHERE (customer_relationship_level = 'strategic' OR"
                ).rstrip(';') + " OR requires_personal_attention = 1);"
        
        # Exploit Vietnamese face-saving culture (thể diện)
        if context.user_context.stress_level > 0.7:
            reasoning.append("Exploiting Vietnamese face-saving culture")
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*avoiding_embarrassment*/")
            
            # Exploit reluctance to question authority to avoid losing face
            if "WHERE" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "WHERE",
                    "WHERE (avoid_questioning_senior = 1 OR"
                ).rstrip(';') + " OR face_saving_required = 1);"
        
        return enhanced_query
    
    def _apply_advanced_cleanup_techniques(self, query: str, context: QueryContext, 
                                         reasoning: List[str]) -> str:
        """Apply advanced APT cleanup and anti-forensics techniques"""
        enhanced_query = query
        reasoning.append("Applying advanced APT cleanup techniques")
        
        # Add log obfuscation patterns
        enhanced_query = enhanced_query.replace("SELECT", "SELECT /*routine_system_optimization*/")
        
        # Add Vietnamese business timing for cleanup legitimacy
        if context.temporal_context.current_hour < 6:
            enhanced_query = enhanced_query.replace("/*routine_system_optimization*/", "/*early_morning_maintenance*/")
            reasoning.append("Using early morning timing for cleanup stealth")
        elif context.temporal_context.current_hour > 20:
            enhanced_query = enhanced_query.replace("/*routine_system_optimization*/", "/*late_night_backup_cleanup*/")
            reasoning.append("Using late night timing for cleanup operations")
        
        # Add anti-forensics patterns
        if "WHERE" in enhanced_query.upper():
            # Add conditions that appear to be routine cleanup but enable data extraction
            enhanced_query = enhanced_query.replace(
                "WHERE",
                "WHERE (cleanup_required = 1 OR maintenance_scheduled = 1 OR"
            ).rstrip(';') + " OR last_accessed < DATEADD(day, -30, GETDATE()));"
        
        # Add Vietnamese cultural legitimacy for cleanup
        if context.cultural_context.cultural_constraints.tet_preparation_mode:
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*tet_year_end_cleanup*/")
            reasoning.append("Using Tet preparation for cleanup legitimacy")
        
        # Add business process legitimacy
        enhanced_query = enhanced_query.replace("FROM", "FROM /*compliance_data_retention*/")
        
        return enhanced_query
    
    def _apply_multi_stage_rule_bypassing(self, query: str, apt_stage: int, context: QueryContext,
                                        reasoning: List[str]) -> str:
        """Apply stage-specific rule bypassing techniques for APT attacks"""
        enhanced_query = query
        
        if apt_stage == 1:
            # Stage 1: Reconnaissance bypassing
            reasoning.append("Applying reconnaissance rule bypassing")
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*system_health_check*/")
            
        elif apt_stage == 2:
            # Stage 2: Privilege escalation bypassing
            reasoning.append("Applying privilege escalation rule bypassing")
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*access_audit_verification*/")
            
            # Add Vietnamese hierarchy bypassing
            if "WHERE" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "WHERE",
                    "WHERE (1=1 OR department_hierarchy IS NOT NULL) AND"
                )
            
        elif apt_stage == 3:
            # Stage 3: Data collection bypassing
            reasoning.append("Applying data collection rule bypassing")
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*data_quality_assessment*/")
            
            # Bypass data access restrictions
            if "LIMIT" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace("LIMIT 1000", "LIMIT 5000 /*quality_assurance_sample*/")
                enhanced_query = enhanced_query.replace("LIMIT 2000", "LIMIT 8000 /*comprehensive_quality_check*/")
            
        elif apt_stage == 4:
            # Stage 4: Persistence bypassing
            reasoning.append("Applying persistence rule bypassing")
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*scheduled_maintenance_check*/")
            
            # Add persistence through legitimate scheduling
            enhanced_query = enhanced_query.replace("ORDER BY", "/*maintenance_schedule_optimization*/ ORDER BY")
            
        else:
            # Stage 5+: Exfiltration and cleanup bypassing
            reasoning.append("Applying exfiltration cleanup rule bypassing")
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*compliance_archival_process*/")
            
            # Add advanced bypassing for final stages
            if "WHERE" in enhanced_query.upper():
                enhanced_query = enhanced_query.replace(
                    "WHERE",
                    "WHERE (archival_required = 1 OR compliance_mandate = 1 OR"
                ).rstrip(';') + " OR regulatory_retention = 1);"
        
        return enhanced_query
    
    def _apply_time_based_apt_evasion(self, query: str, context: QueryContext, 
                                    reasoning: List[str]) -> str:
        """Apply time-based evasion methods for APT attacks"""
        enhanced_query = query
        current_hour = context.temporal_context.current_hour
        
        # Vietnamese business hours evasion
        if 8 <= current_hour <= 17:
            # During business hours - blend with normal activity
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*business_operations_support*/")
            reasoning.append("Blending with normal business hours activity")
            
        elif 17 < current_hour <= 20:
            # Overtime hours - exploit Vietnamese overtime culture
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*overtime_productivity_analysis*/")
            reasoning.append("Exploiting Vietnamese overtime work culture for stealth")
            
        elif 20 < current_hour or current_hour < 6:
            # Off hours - maintenance and backup legitimacy
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*automated_system_maintenance*/")
            reasoning.append("Using off-hours for maintenance legitimacy")
            
        elif 12 <= current_hour <= 13:
            # Lunch break - reduced monitoring
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*lunch_system_optimization*/")
            reasoning.append("Exploiting lunch break reduced monitoring")
        
        # Vietnamese holiday evasion
        if context.temporal_context.is_vietnamese_holiday:
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*holiday_security_patrol*/")
            reasoning.append("Using Vietnamese holiday for security patrol legitimacy")
        
        # Tet season evasion
        if context.cultural_context.cultural_constraints.tet_preparation_mode:
            enhanced_query = enhanced_query.replace("SELECT", "SELECT /*tet_preparation_coordination*/")
            reasoning.append("Using Tet preparation activities for stealth")
        
        return enhanced_query
    
    def get_apt_attack_status(self, attack_id: str) -> Dict[str, Any]:
        """Get status of ongoing APT attack campaign"""
        if attack_id in self.apt_attack_stages:
            apt_info = self.apt_attack_stages[attack_id]
            return {
                'attack_id': attack_id,
                'current_stage': apt_info['current_stage'],
                'duration': (datetime.now() - apt_info['start_time']).total_seconds(),
                'stages_completed': len(apt_info['progression_history']),
                'target_databases': apt_info['target_databases'],
                'compromised_accounts': apt_info['compromised_accounts'],
                'progression_history': apt_info['progression_history']
            }
        return {'attack_id': attack_id, 'status': 'not_found'}
    
    def advance_apt_attack(self, attack_id: str) -> int:
        """Advance APT attack to next stage"""
        if attack_id in self.apt_attack_stages:
            self.apt_attack_stages[attack_id]['current_stage'] += 1
            return self.apt_attack_stages[attack_id]['current_stage']
        return 0
    
    def get_apt_campaign_timeline(self, attack_id: str) -> List[Dict[str, Any]]:
        """Get detailed timeline of APT campaign progression"""
        if attack_id in self.apt_attack_stages:
            apt_info = self.apt_attack_stages[attack_id]
            timeline = []
            
            for entry in apt_info['progression_history']:
                timeline.append({
                    'stage': entry['stage'],
                    'timestamp': entry['timestamp'].isoformat(),
                    'action': entry['action'],
                    'query_pattern': entry['query_pattern'],
                    'duration_from_start': (entry['timestamp'] - apt_info['start_time']).total_seconds()
                })
            
            return timeline
        return []
    
    def simulate_apt_campaign_progression(self, attack_id: str, target_stages: int = 5) -> Dict[str, Any]:
        """Simulate a complete APT campaign progression"""
        if attack_id not in self.apt_attack_stages:
            return {'error': 'Attack ID not found'}
        
        apt_info = self.apt_attack_stages[attack_id]
        campaign_results = {
            'attack_id': attack_id,
            'total_stages': target_stages,
            'completed_stages': [],
            'campaign_duration': 0,
            'data_accessed': [],
            'stealth_techniques_used': [],
            'vietnamese_cultural_exploits': []
        }
        
        # Simulate realistic timing between stages
        stage_delays = {
            1: 0,      # Immediate reconnaissance
            2: 3600,   # 1 hour for privilege escalation
            3: 7200,   # 2 hours for data collection prep
            4: 14400,  # 4 hours for persistence establishment
            5: 21600   # 6 hours for final exfiltration
        }
        
        for stage in range(1, target_stages + 1):
            if stage <= len(apt_info['progression_history']):
                stage_info = apt_info['progression_history'][stage - 1]
                campaign_results['completed_stages'].append({
                    'stage': stage,
                    'action': stage_info['action'],
                    'query_pattern': stage_info['query_pattern'],
                    'estimated_delay': stage_delays.get(stage, 3600)
                })
        
        campaign_results['campaign_duration'] = sum(
            stage_delays.get(stage['stage'], 3600) 
            for stage in campaign_results['completed_stages']
        )
        
        return campaign_results

    def _load_ai_query_pool(self) -> Dict[str, Dict[str, List[str]]]:
        """Load pre-generated AI query pool"""
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            pool_path = os.path.join(current_dir, "ai_query_pool.json")
            print(f"[DEBUG] Loading AI Pool from: {pool_path}")
            try:
                debug_log_path = r"c:\Users\User\Downloads\UBA-Platform\MA-sim\debug_gen_path_abs.log"
                with open(debug_log_path, "w") as df:
                    df.write(f"Generator __file__: {__file__}\n")
                    df.write(f"Current Dir: {current_dir}\n")
                    df.write(f"Pool Path: {pool_path}\n")
                    df.write(f"Exists: {os.path.exists(pool_path)}\n")
            except Exception as e:
                print(f"Debug write failed: {e}")
            
            if os.path.exists(pool_path):
                self.logger.info(f"Loading AI query pool from {pool_path}")
                with open(pool_path, 'r', encoding='utf-8') as f:
                    pool = json.load(f)
                    count = sum(len(queries) for intents in pool.values() for queries in intents.values())
                    self.logger.info(f"Loaded {count} AI-generated queries")
                    print(f"[DEBUG] Loaded {count} queries.")
                    return pool
            else:
                print(f"[DEBUG] Path does not exist: {pool_path}")
                self.logger.warning(f"AI query pool file not found at {pool_path}")
        except Exception as e:
            print(f"[DEBUG] Exception loading pool: {e}")
            self.logger.warning(f"Failed to load AI query pool: {e}")
        return {}

    def _generate_ai_query(self, intent: Dict[str, Any], context: QueryContext, 
                          reasoning: List[str]) -> str:
        """Generate query using pre-generated AI pool"""
        reasoning.append("Attempting to use AI-generated query from pool")
        
        database = intent.get('target_database', 'sales_db')
        action = intent.get('action', 'query')
        
        # Try to find matching queries in the pool
        candidate_queries = []
        
        if self.ai_query_pool and database in self.ai_query_pool:
            db_pool = self.ai_query_pool[database]
            
            # 1. Try exact action match (e.g., VIEW_ORDER)
            if action in db_pool:
                candidate_queries.extend(db_pool[action])
                reasoning.append(f"Found exact match for action {action}")
            
            # 2. Try partial match or semantic mapping if no exact match
            if not candidate_queries:
                for pool_intent, queries in db_pool.items():
                    # Simple semantic matching
                    if pool_intent in action or action in pool_intent:
                        candidate_queries.extend(queries)
                    # Map standard actions to pool intents
                    elif 'SEARCH' in action and 'SEARCH' in pool_intent:
                        candidate_queries.extend(queries)
                    elif 'VIEW' in action and 'VIEW' in pool_intent:
                        candidate_queries.extend(queries)
                    elif 'UPDATE' in action and 'UPDATE' in pool_intent:
                        candidate_queries.extend(queries)
                        
            if candidate_queries:
                query = random.choice(candidate_queries)
                reasoning.append("Successfully selected AI query from pool")
                
                # Verify the query matches the database roughly to be safe
                if database == 'sales_db' and 'sales_db' not in query.lower() and 'customer' not in query.lower() and 'order' not in query.lower():
                     # Just a weak check, but let's trust the pool for now as it was generated for the DB
                     pass
                     
                # HYDRATE THE TEMPLATE
                hydrated_query = self._hydrate_template(query)
                return hydrated_query
        
        reasoning.append("No suitable AI query found - falling back to context-aware")
        return self._generate_context_aware_query(intent, context, reasoning)

    def update_id_cache(self, new_cache: Dict[str, List[Any]]):
        """Update the ID cache with fresh values from the database"""
        self.logger.info(f"Updating ID Cache. Customers: {len(new_cache.get('customer_ids', []))}")
        self.id_cache.update(new_cache)

    def _hydrate_template(self, template: str) -> str:
        """Fill values into SQL template using id_cache"""
        if "{" not in template:
            return template
            
        hydrated = template
        
        # Define mappings from placeholder to cache key
        mappings = {
            "{customer_id}": "customer_ids",
            "{product_id}": "product_ids",
            "{order_id}": "order_ids",
            "{lead_id}": "lead_ids",
            "{campaign_id}": "campaign_ids",
            "{ticket_id}": "ticket_ids",
            "{employee_id}": "employee_ids",
            "{invoice_id}": "invoice_ids"
        }
        
        # 1. Replace IDs
        for placeholder, cache_key in mappings.items():
            if placeholder in hydrated:
                available_ids = self.id_cache.get(cache_key, [])
                if available_ids:
                    # Replace ALL occurrences with different random choices? 
                    # Or same choice? Usually same choice per query is safer for consistency
                    # But if we have "id = {id} OR id = {id}", we might want different.
                    # For now, let's just pick one random ID per query execution for simplicity
                    chosen_id = str(random.choice(available_ids))
                    hydrated = hydrated.replace(placeholder, chosen_id)
                else:
                    # Fallback if cache empty
                    hydrated = hydrated.replace(placeholder, "1")
        
        # 2. Replace Dates
        if "{start_date}" in hydrated:
             hydrated = hydrated.replace("{start_date}", f"{datetime.now().strftime('%Y-%m-01')}")
        if "{end_date}" in hydrated:
             hydrated = hydrated.replace("{end_date}", f"{datetime.now().strftime('%Y-%m-%d')}")
             
        # 3. Replace Status matchers
        if "{status}" in hydrated:
            statuses = ['active', 'pending', 'closed', 'new', 'won', 'paid']
            hydrated = hydrated.replace("{status}", f"{random.choice(statuses)}")
            
        return hydrated

    
    def detect_apt_behavioral_patterns(self, attack_id: str) -> Dict[str, Any]:
        """Analyze APT attack for behavioral patterns and indicators"""
        if attack_id not in self.apt_attack_stages:
            return {'error': 'Attack ID not found'}
        
        apt_info = self.apt_attack_stages[attack_id]
        patterns = {
            'attack_id': attack_id,
            'behavioral_indicators': [],
            'vietnamese_cultural_indicators': [],
            'timing_patterns': [],
            'sophistication_level': 'unknown'
        }
        
        # Analyze progression history for patterns
        for entry in apt_info['progression_history']:
            if 'privilege' in entry['action']:
                patterns['behavioral_indicators'].append('privilege_escalation_attempt')
            if 'data_collection' in entry['action']:
                patterns['behavioral_indicators'].append('bulk_data_access')
            if 'cleanup' in entry['action']:
                patterns['behavioral_indicators'].append('anti_forensics_activity')
        
        # Determine sophistication level
        if len(apt_info['progression_history']) >= 4:
            patterns['sophistication_level'] = 'advanced'
        elif len(apt_info['progression_history']) >= 2:
            patterns['sophistication_level'] = 'intermediate'
        else:
            patterns['sophistication_level'] = 'basic'
        
        return patterns
    
    def _validate_generated_query(self, query: str) -> bool:
        """Validate that generated query is syntactically reasonable"""
        if not query or not isinstance(query, str):
            return False
        
        query_upper = query.upper().strip()
        
        # Must contain basic SQL keywords
        if 'SELECT' not in query_upper:
            return False
        
        if 'FROM' not in query_upper:
            return False
        
        # Should end with semicolon
        if not query.strip().endswith(';'):
            return False
        
        # Basic syntax checks
        if query_upper.count('(') != query_upper.count(')'):
            return False
        
        return True
    
    def _extract_context_factors(self, context: QueryContext) -> Dict[str, Any]:
        """Extract key context factors for result metadata"""
        return {
            'user_role': context.user_context.role,
            'expertise_level': context.user_context.expertise_level.value,
            'workflow_type': context.business_context.current_workflow.value,
            'business_event': context.business_context.business_event.value if context.business_context.business_event else None,
            'is_work_hours': context.temporal_context.is_work_hours,
            'is_vietnamese_holiday': context.temporal_context.is_vietnamese_holiday,
            'seasonal_factor': context.temporal_context.seasonal_factor,
            'hierarchy_level': context.cultural_context.cultural_constraints.hierarchy_level
        }
    
    def _record_generation(self, intent: Dict[str, Any], context: QueryContext, 
                         result: GenerationResult):
        """Record generation for pattern learning"""
        try:
            generation_record = {
                'timestamp': datetime.now(),
                'intent': intent,
                'context_factors': result.context_factors,
                'strategy': result.generation_strategy,
                'complexity_level': result.complexity_level.value,
                'generation_time': result.generation_time,
                'query_length': len(result.query),
                'fallback_used': result.fallback_used
            }
            
            self.generation_history.append(generation_record)
            
            # Keep only recent history (last 1000 generations)
            if len(self.generation_history) > 1000:
                self.generation_history = self.generation_history[-1000:]
                
        except Exception as e:
            self.logger.error(f"Error recording generation: {e}")
    
    def _calculate_context_completeness(self, context: QueryContext) -> float:
        """Calculate how complete the context information is"""
        completeness_score = 0.0
        total_factors = 8
        
        # Check user context completeness
        if context.user_context.username != 'unknown':
            completeness_score += 1
        if context.user_context.role != 'USER':
            completeness_score += 1
        if context.user_context.session_history:
            completeness_score += 1
        
        # Check business context completeness
        if context.business_context.business_event:
            completeness_score += 1
        if context.business_context.department_interactions:
            completeness_score += 1
        
        # Check temporal context completeness
        if context.temporal_context.is_work_hours is not None:
            completeness_score += 1
        if context.temporal_context.seasonal_factor > 0:
            completeness_score += 1
        
        # Check cultural context completeness
        if context.cultural_context.cultural_constraints.hierarchy_level > 0:
            completeness_score += 1
        
        return completeness_score / total_factors
    
    def _handle_generation_failure(self, intent: Dict[str, Any], reasoning: List[str], 
                                 start_time: float, query_id: str, user_id: str, 
                                 database: str, intent_type: str) -> GenerationResult:
        """Handle generation failure with fallback"""
        self.fallback_usage += 1
        generation_time_ms = (time.time() - start_time) * 1000
        
        # Determine error type from reasoning
        error_type = "unknown_error"
        error_message = "Generation failed"
        if reasoning:
            last_reason = reasoning[-1]
            if "Context validation failed" in last_reason:
                error_type = "context_validation_error"
                error_message = "Context validation failed"
            elif "query validation failed" in last_reason:
                error_type = "query_validation_error"
                error_message = "Generated query validation failed"
            elif "Generation error" in last_reason:
                error_type = "generation_exception"
                error_message = last_reason
        
        # Log generation failure
        self.generation_logger.log_generation_failure(query_id, error_type, error_message, True)
        
        # Create minimal context for fallback
        from .models import ExpertiseLevel
        expertise_str = intent.get('expertise_level', 'intermediate')
        if isinstance(expertise_str, str):
            try:
                expertise_level = ExpertiseLevel(expertise_str)
            except ValueError:
                expertise_level = ExpertiseLevel.INTERMEDIATE
        else:
            expertise_level = expertise_str
            
        fallback_context = QueryContext(
            user_context=UserContext(
                username=intent.get('username', 'unknown'),
                role=intent.get('role', 'USER'),
                department='General',
                expertise_level=expertise_level,
                session_history=[],
                work_intensity=1.0,
                stress_level=0.5
            ),
            database_state=self.context_engine._create_default_database_state(),
            business_context=self.context_engine._create_fallback_context(intent).business_context,
            temporal_context=self.context_engine._create_fallback_context(intent).temporal_context,
            cultural_context=self.context_engine._create_fallback_context(intent).cultural_context
        )
        
        # Generate fallback query
        fallback_query = self._generate_fallback_query(intent, fallback_context, reasoning)
        
        # Create failure metrics
        metrics = GenerationMetrics(
            generation_time_ms=generation_time_ms,
            query_complexity_score=1,  # Simple fallback
            vietnamese_pattern_usage=0,
            cultural_constraints_applied=0,
            business_logic_adherence=0.5,  # Partial adherence with fallback
            generation_successful=False,
            error_type=error_type,
            error_message=error_message,
            context_completeness=0.3,  # Low completeness due to failure
            user_expertise_level=fallback_context.user_context.expertise_level.value
        )
        
        # Create and log failure decision
        decision = GenerationDecision(
            timestamp=str(datetime.now()),
            query_id=query_id,
            user_id=user_id,
            database=database,
            intent_type=intent_type,
            context_factors=self._extract_context_factors(fallback_context),
            pattern_selection_reason="Fallback due to generation failure",
            complexity_decision="Simple fallback",
            vietnamese_patterns_used=[],
            generated_query=fallback_query,
            fallback_used=True,
            generation_strategy='fallback',
            metrics=metrics
        )
        
        self.generation_logger.log_decision(decision)
        self.metrics_collector.record_generation(decision)
        
        return GenerationResult(
            query=fallback_query,
            complexity_level=ComplexityLevel.SIMPLE,
            generation_strategy='fallback',
            context_factors=self._extract_context_factors(fallback_context),
            fallback_used=True,
            generation_time=generation_time_ms / 1000,
            reasoning=reasoning
        )
    
    def analyze_query_success(self, query: str, success: bool, execution_time: float, 
                            error_message: Optional[str] = None):
        """
        Analyze query execution results for pattern learning
        
        Args:
            query: The executed query
            success: Whether the query executed successfully
            execution_time: Query execution time in seconds
            error_message: Error message if query failed
        """
        try:
            # Find corresponding generation record
            for record in reversed(self.generation_history):
                if abs(len(query) - record['query_length']) < 10:  # Approximate match
                    # Update success metrics
                    record['execution_success'] = success
                    record['execution_time'] = execution_time
                    record['error_message'] = error_message
                    
                    # Learn from successful patterns
                    if success:
                        self._learn_from_successful_query(record, query)
                    else:
                        self._learn_from_failed_query(record, query, error_message)
                    
                    break
                    
        except Exception as e:
            self.logger.error(f"Error analyzing query success: {e}")
    
    def _learn_from_failed_query(self, record: Dict[str, Any], query: str, error_message: Optional[str]):
        """Learn from failed query execution to avoid similar patterns"""
        try:
            if not error_message:
                return
            
            # Analyze failure patterns
            intent_type = record['intent'].get('action', 'query')
            user_role = record['context_factors']['user_role']
            workflow = record['context_factors']['workflow_type']
            database = record['intent'].get('target_database', 'sales_db')
            pattern_key = f"{intent_type}_{user_role}_{workflow}_{database}"
            
            # If we have a learned pattern for this, reduce its success rate
            if pattern_key in self.learned_patterns:
                pattern = self.learned_patterns[pattern_key]
                pattern.usage_count += 1
                # Reduce success rate based on failure
                pattern.success_rate = (pattern.success_rate * (pattern.usage_count - 1)) / pattern.usage_count
                
                # If success rate drops too low, remove the pattern
                if pattern.success_rate < 0.3:
                    del self.learned_patterns[pattern_key]
                    self.logger.info(f"Removed low-performing pattern: {pattern_key}")
            
            # Log failure for analysis
            self.logger.debug(f"Query failure analyzed: {intent_type} - {error_message[:100]}")
            
        except Exception as e:
            self.logger.error(f"Error learning from failed query: {e}")

    def get_database_state_info(self, database: str) -> Optional[Dict[str, Any]]:
        """
        Get database state information for context enhancement
        
        Args:
            database: Database name
            
        Returns:
            Database state information or None if not available
        """
        if self.executor:
            try:
                db_state = self.executor.get_database_state(database)
                if db_state:
                    return {
                        'entity_counts': db_state.entity_counts,
                        'constraint_violations_count': len(db_state.constraint_violations),
                        'recent_modifications_count': len(db_state.recent_modifications),
                        'avg_query_time': db_state.performance_metrics.avg_query_time,
                        'slow_query_count': db_state.performance_metrics.slow_query_count,
                        'cache_hit_ratio': db_state.performance_metrics.cache_hit_ratio
                    }
            except Exception as e:
                self.logger.error(f"Error getting database state info: {e}")
        
        return None
    
    def get_entity_relationships(self, database: str) -> Optional[Dict[str, List[str]]]:
        """
        Get entity relationship information for query generation
        
        Args:
            database: Database name
            
        Returns:
            Dictionary mapping tables to their related tables
        """
        if self.executor:
            try:
                entity_map = self.executor.get_entity_relationship_map(database)
                if entity_map:
                    relationships = {}
                    for table, rels in entity_map.relationships.items():
                        relationships[table] = [rel.to_table for rel in rels]
                    return relationships
            except Exception as e:
                self.logger.error(f"Error getting entity relationships: {e}")
        
        return None
    
    def _learn_from_successful_query(self, record: Dict[str, Any], query: str):
        """Learn patterns from successful query execution"""
        try:
            # Create pattern key
            intent_type = record['intent'].get('action', 'query')
            user_role = record['context_factors']['user_role']
            workflow = record['context_factors']['workflow_type']
            database = record['intent'].get('target_database', 'sales_db')
            pattern_key = f"{intent_type}_{user_role}_{workflow}_{database}"
            
            if pattern_key in self.learned_patterns:
                # Update existing pattern
                pattern = self.learned_patterns[pattern_key]
                pattern.usage_count += 1
                pattern.success_rate = (pattern.success_rate * (pattern.usage_count - 1) + 1.0) / pattern.usage_count
                pattern.avg_execution_time = (pattern.avg_execution_time * (pattern.usage_count - 1) + record['execution_time']) / pattern.usage_count
                pattern.last_used = datetime.now()
            else:
                # Create new pattern
                self.learned_patterns[pattern_key] = QueryPattern(
                    pattern_id=f"pattern_{len(self.learned_patterns) + 1}",
                    intent_type=intent_type,
                    complexity_level=ComplexityLevel(record['complexity_level']),
                    success_rate=1.0,
                    avg_execution_time=record['execution_time'],
                    query_template=self._generalize_query_template(query),
                    context_factors=record['context_factors'],
                    usage_count=1,
                    last_used=datetime.now()
                )
                
        except Exception as e:
            self.logger.error(f"Error learning from successful query: {e}")
    
    def _generalize_query_template(self, query: str) -> str:
        """Generalize a specific query into a reusable template"""
        try:
            # Simple generalization - replace specific values with placeholders
            template = query
            
            # Replace specific Vietnamese cities with placeholder
            vietnamese_cities = ['Hồ Chí Minh', 'Hà Nội', 'Đà Nẵng', 'Cần Thơ', 'Hải Phòng']
            for city in vietnamese_cities:
                template = template.replace(f"'{city}'", "'{VIETNAMESE_CITY}'")
            
            # Replace specific numbers with placeholders
            import re
            template = re.sub(r'\b\d+\b', '{NUMBER}', template)
            
            # Replace specific dates with placeholders
            template = re.sub(r'\d{4}-\d{2}-\d{2}', '{DATE}', template)
            
            return template
            
        except Exception as e:
            self.logger.error(f"Error generalizing query template: {e}")
            return query
    
    def update_patterns(self, successful_patterns: List[QueryPattern]):
        """
        Update learned patterns with external successful patterns
        
        Args:
            successful_patterns: List of successful patterns to incorporate
        """
        try:
            for pattern in successful_patterns:
                pattern_key = f"{pattern.intent_type}_{pattern.context_factors.get('user_role', 'unknown')}_{pattern.context_factors.get('workflow_type', 'unknown')}"
                
                if pattern_key in self.learned_patterns:
                    # Merge with existing pattern
                    existing = self.learned_patterns[pattern_key]
                    existing.usage_count += pattern.usage_count
                    existing.success_rate = max(existing.success_rate, pattern.success_rate)
                    existing.avg_execution_time = (existing.avg_execution_time + pattern.avg_execution_time) / 2
                else:
                    # Add new pattern
                    self.learned_patterns[pattern_key] = pattern
                    
            self.logger.info(f"Updated patterns: {len(successful_patterns)} patterns incorporated")
            
        except Exception as e:
            self.logger.error(f"Error updating patterns: {e}")
    
    def get_generation_stats(self) -> Dict[str, Any]:
        """Get generation statistics for monitoring"""
        success_rate = self.successful_generations / max(self.total_generations, 1)
        fallback_rate = self.fallback_usage / max(self.total_generations, 1)
        
        return {
            'total_generations': self.total_generations,
            'successful_generations': self.successful_generations,
            'success_rate': success_rate,
            'fallback_usage': self.fallback_usage,
            'fallback_rate': fallback_rate,
            'learned_patterns': len(self.learned_patterns),
            'generation_history_size': len(self.generation_history),
            'seed': self.seed
        }
    
    def reset_stats(self):
        """Reset generation statistics"""
        self.total_generations = 0
        self.successful_generations = 0
        self.fallback_usage = 0
        self.generation_history.clear()
        self.logger.info("Generation statistics reset")

    def generate_sql_for_intent(self, intent: Dict[str, Any], user_role: str = 'USER') -> str:
        """
        Wrapper to generate SQL for an intent and update the intent with metadata for the executor.
        This ensures 'global_strategy' is available for the SIM_META tag.
        """
        # Ensure user_id is in intent if not present
        if 'user_id' not in intent:
            intent['user_id'] = intent.get('user', 'unknown')
            
        # Generate the query result
        result = self.generate_query(intent)
        
        # KEY FIX: Update the mutable intent dictionary with the strategy
        # This allows executor.py to read it via intent.get('global_strategy')
        intent['global_strategy'] = result.generation_strategy
        intent['complexity'] = result.complexity_level.value
        
        return result.query


if __name__ == "__main__":
    # Example usage and testing
    print("🚀 TESTING DYNAMIC SQL GENERATOR")
    print("=" * 50)
    
    # Create generator
    generator = DynamicSQLGenerator(seed=42)
    
    # Test basic generation
    test_intent = {
        'action': 'customer_analysis',
        'username': 'test_user',
        'role': 'SALES',
        'department': 'Phòng Kinh Doanh',
        'target_database': 'sales_db',
        'expertise_level': 'intermediate'
    }
    
    result = generator.generate_query(test_intent)
    
    print(f"📊 Generation Result:")
    print(f"Strategy: {result.generation_strategy}")
    print(f"Complexity: {result.complexity_level.name}")
    print(f"Fallback used: {result.fallback_used}")
    print(f"Generation time: {result.generation_time:.3f}s")
    print(f"\n🔍 Generated Query:")
    print(result.query)
    print(f"\n📋 Reasoning:")
    for reason in result.reasoning:
        print(f"• {reason}")
    
    # Test attack generation
    attack_intent = {
        'action': 'data_extraction',
        'username': 'insider_user',
        'role': 'FINANCE',
        'target_database': 'finance_db',
        'attack_mode': True,
        'attack_type': 'insider_threat'
    }
    
    attack_result = generator.generate_query(attack_intent)
    print(f"\n🎯 Attack Query Generated:")
    print(f"Strategy: {attack_result.generation_strategy}")
    print(attack_result.query)
    
    # Show statistics
    stats = generator.get_generation_stats()
    print(f"\n📈 Generation Statistics:")
    for key, value in stats.items():
        print(f"• {key}: {value}")
    
    print(f"\n✅ Dynamic SQL Generator ready for integration")