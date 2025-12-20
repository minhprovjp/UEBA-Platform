"""
Query Context Engine for Dynamic SQL Generation System

Analyzes runtime context including database state, user behavior patterns,
business workflows, and Vietnamese cultural factors to inform SQL generation.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

from .models import (
    QueryContext, UserContext, BusinessContext, TemporalContext, 
    CulturalContext, DatabaseState, Relationship, ConstraintViolation,
    Modification, PerformanceMetrics, WorkflowType, BusinessEvent,
    BusinessCyclePhase, SensitivityLevel, ExpertiseLevel, CulturalConstraints
)
from .vietnamese_patterns import VietnameseBusinessPatterns
from .config import get_vietnamese_config, get_database_config


@dataclass
class WorkflowContext:
    """Business workflow context analysis result"""
    workflow_type: WorkflowType
    complexity_level: int
    department_interactions: List[str]
    data_access_patterns: Dict[str, float]
    cultural_considerations: List[str]


@dataclass
class RelationshipMap:
    """Database relationship mapping result"""
    primary_tables: List[str]
    related_tables: Dict[str, List[str]]
    join_paths: Dict[str, List[str]]
    constraint_dependencies: Dict[str, List[str]]


class QueryContextEngine:
    """
    Query Context Engine for analyzing runtime context and database state
    to inform context-aware SQL generation with Vietnamese business patterns
    """
    
    def __init__(self):
        self.vietnamese_patterns = VietnameseBusinessPatterns()
        self.vietnamese_config = get_vietnamese_config()
        self.database_config = get_database_config()
        self.logger = logging.getLogger(__name__)
        
        # Cache for performance optimization
        self._relationship_cache = {}
        self._workflow_cache = {}
        
        # Database schema information (would be loaded from actual database)
        self._initialize_database_schemas()
    
    def _initialize_database_schemas(self):
        """Initialize database schema information for analysis"""
        self.database_schemas = {
            'sales_db': {
                'customers': ['customer_id', 'name', 'email', 'phone', 'city', 'company'],
                'orders': ['order_id', 'customer_id', 'order_date', 'total_amount', 'status'],
                'products': ['product_id', 'name', 'category', 'price', 'stock_quantity'],
                'order_items': ['order_item_id', 'order_id', 'product_id', 'quantity', 'unit_price']
            },
            'hr_db': {
                'employees': ['employee_id', 'name', 'department', 'position', 'hire_date', 'salary'],
                'attendance': ['attendance_id', 'employee_id', 'date', 'check_in', 'check_out'],
                'salaries': ['salary_id', 'employee_id', 'month', 'base_salary', 'bonus', 'deductions'],
                'departments': ['department_id', 'name', 'manager_id', 'budget']
            },
            'finance_db': {
                'invoices': ['invoice_id', 'customer_id', 'amount', 'issue_date', 'due_date', 'status'],
                'expenses': ['expense_id', 'employee_id', 'category', 'amount', 'date', 'description'],
                'accounts': ['account_id', 'account_name', 'account_type', 'balance'],
                'transactions': ['transaction_id', 'account_id', 'amount', 'date', 'description']
            },
            'marketing_db': {
                'campaigns': ['campaign_id', 'name', 'start_date', 'end_date', 'budget', 'status'],
                'leads': ['lead_id', 'name', 'email', 'phone', 'source', 'status'],
                'campaign_results': ['result_id', 'campaign_id', 'metric', 'value', 'date']
            },
            'support_db': {
                'tickets': ['ticket_id', 'customer_id', 'subject', 'description', 'status', 'priority'],
                'ticket_responses': ['response_id', 'ticket_id', 'employee_id', 'response', 'timestamp'],
                'knowledge_base': ['article_id', 'title', 'content', 'category', 'views']
            },
            'inventory_db': {
                'products': ['product_id', 'name', 'category', 'supplier_id', 'stock_level'],
                'suppliers': ['supplier_id', 'name', 'contact_person', 'email', 'phone'],
                'stock_movements': ['movement_id', 'product_id', 'type', 'quantity', 'date']
            },
            'admin_db': {
                'users': ['user_id', 'username', 'email', 'role', 'last_login'],
                'audit_logs': ['log_id', 'user_id', 'action', 'table_name', 'timestamp'],
                'system_config': ['config_id', 'key', 'value', 'description']
            }
        }
        
        # Define relationships between tables
        self.table_relationships = {
            'sales_db': [
                Relationship('orders', 'customers', 'many_to_one', 'customer_id'),
                Relationship('order_items', 'orders', 'many_to_one', 'order_id'),
                Relationship('order_items', 'products', 'many_to_one', 'product_id')
            ],
            'hr_db': [
                Relationship('employees', 'departments', 'many_to_one', 'department_id'),
                Relationship('attendance', 'employees', 'many_to_one', 'employee_id'),
                Relationship('salaries', 'employees', 'many_to_one', 'employee_id')
            ],
            'finance_db': [
                Relationship('invoices', 'customers', 'many_to_one', 'customer_id'),
                Relationship('expenses', 'employees', 'many_to_one', 'employee_id'),
                Relationship('transactions', 'accounts', 'many_to_one', 'account_id')
            ]
        }
    
    def analyze_context(self, intent: Dict[str, Any], db_state: Dict[str, Any], 
                       time_context: Dict[str, Any]) -> QueryContext:
        """
        Analyze complete context for SQL generation
        
        Args:
            intent: User intent and action information
            db_state: Current database state information
            time_context: Temporal context including current time and business events
            
        Returns:
            Complete QueryContext for SQL generation
        """
        try:
            # Extract user information from intent
            user_context = self._build_user_context(intent)
            
            # Analyze database state
            database_state = self._analyze_database_state(db_state)
            
            # Determine business context
            business_context = self._analyze_business_context(intent, time_context, user_context)
            
            # Analyze temporal patterns
            temporal_context = self._analyze_temporal_context(time_context)
            
            # Apply Vietnamese cultural context
            cultural_context = self._analyze_cultural_context(intent, temporal_context, user_context)
            
            # Create complete query context
            query_context = QueryContext(
                user_context=user_context,
                database_state=database_state,
                business_context=business_context,
                temporal_context=temporal_context,
                cultural_context=cultural_context
            )
            
            # Validate context
            if not query_context.validate():
                self.logger.warning("Generated QueryContext failed validation")
                return self._create_fallback_context(intent)
            
            return query_context
            
        except Exception as e:
            self.logger.error(f"Error analyzing context: {e}")
            return self._create_fallback_context(intent)
    
    def analyze_database_state(self, db_state: Dict[str, Any]) -> DatabaseState:
        """
        Analyze current database state and entity relationships
        
        Args:
            db_state: Raw database state information
            
        Returns:
            Analyzed DatabaseState with relationships and constraints
        """
        try:
            # Extract entity counts
            entity_counts = db_state.get('entity_counts', {})
            
            # Build relationship map
            relationship_map = self._build_relationship_map(db_state)
            
            # Detect constraint violations
            constraint_violations = self._detect_constraint_violations(db_state)
            
            # Analyze recent modifications
            recent_modifications = self._analyze_recent_modifications(db_state)
            
            # Collect performance metrics
            performance_metrics = self._collect_performance_metrics(db_state)
            
            return DatabaseState(
                entity_counts=entity_counts,
                relationship_map=relationship_map,
                constraint_violations=constraint_violations,
                recent_modifications=recent_modifications,
                performance_metrics=performance_metrics
            )
            
        except Exception as e:
            self.logger.error(f"Error analyzing database state: {e}")
            return self._create_default_database_state()
    
    def get_business_workflow_context(self, user_role: str, action: str) -> WorkflowContext:
        """
        Get business workflow context based on user role and action
        
        Args:
            user_role: User's role in the organization
            action: Specific action being performed
            
        Returns:
            WorkflowContext with business logic patterns
        """
        cache_key = f"{user_role}_{action}"
        if cache_key in self._workflow_cache:
            return self._workflow_cache[cache_key]
        
        try:
            # Map role to department
            department = self._map_role_to_department(user_role)
            
            # Determine workflow type
            workflow_type = self._determine_workflow_type(action, department)
            
            # Get Vietnamese business patterns
            time_context = {'current_hour': datetime.now().hour, 'is_vietnamese_holiday': False}
            patterns = self.vietnamese_patterns.get_workflow_patterns(department, time_context)
            
            if patterns:
                pattern = patterns[0]  # Use first matching pattern
                workflow_context = WorkflowContext(
                    workflow_type=workflow_type,
                    complexity_level=self._calculate_complexity_level(user_role, action),
                    department_interactions=pattern.department_interactions,
                    data_access_patterns=pattern.data_access_patterns,
                    cultural_considerations=pattern.cultural_considerations
                )
            else:
                # Fallback workflow context
                workflow_context = WorkflowContext(
                    workflow_type=WorkflowType.ADMINISTRATIVE,
                    complexity_level=3,
                    department_interactions=[department],
                    data_access_patterns={'admin_db': 1.0},
                    cultural_considerations=['standard_business_practice']
                )
            
            # Cache result
            self._workflow_cache[cache_key] = workflow_context
            return workflow_context
            
        except Exception as e:
            self.logger.error(f"Error getting workflow context: {e}")
            return WorkflowContext(
                workflow_type=WorkflowType.ADMINISTRATIVE,
                complexity_level=1,
                department_interactions=[],
                data_access_patterns={},
                cultural_considerations=[]
            )
    
    def assess_data_relationships(self, target_database: str, entities: List[str]) -> RelationshipMap:
        """
        Assess data relationships for target database and entities
        
        Args:
            target_database: Target database name
            entities: List of entity/table names
            
        Returns:
            RelationshipMap with join paths and dependencies
        """
        cache_key = f"{target_database}_{','.join(sorted(entities))}"
        if cache_key in self._relationship_cache:
            return self._relationship_cache[cache_key]
        
        try:
            # Get database schema
            schema = self.database_schemas.get(target_database, {})
            relationships = self.table_relationships.get(target_database, [])
            
            # Identify primary tables (those in entities list)
            primary_tables = [entity for entity in entities if entity in schema]
            
            # Build related tables map
            related_tables = {}
            for table in primary_tables:
                related_tables[table] = self._find_related_tables(table, relationships)
            
            # Calculate join paths
            join_paths = {}
            for table in primary_tables:
                join_paths[table] = self._calculate_join_paths(table, relationships, schema)
            
            # Determine constraint dependencies
            constraint_dependencies = {}
            for table in primary_tables:
                constraint_dependencies[table] = self._find_constraint_dependencies(table, relationships)
            
            relationship_map = RelationshipMap(
                primary_tables=primary_tables,
                related_tables=related_tables,
                join_paths=join_paths,
                constraint_dependencies=constraint_dependencies
            )
            
            # Cache result
            self._relationship_cache[cache_key] = relationship_map
            return relationship_map
            
        except Exception as e:
            self.logger.error(f"Error assessing data relationships: {e}")
            return RelationshipMap(
                primary_tables=entities,
                related_tables={},
                join_paths={},
                constraint_dependencies={}
            )
    
    def _build_user_context(self, intent: Dict[str, Any]) -> UserContext:
        """Build user context from intent information"""
        return UserContext(
            username=intent.get('username', 'unknown_user'),
            role=intent.get('role', 'USER'),
            department=intent.get('department', 'General'),
            expertise_level=ExpertiseLevel(intent.get('expertise_level', 'intermediate')),
            session_history=[],  # Would be populated from session data
            work_intensity=intent.get('work_intensity', 1.0),
            stress_level=intent.get('stress_level', 0.5)
        )
    
    def _analyze_database_state(self, db_state: Dict[str, Any]) -> DatabaseState:
        """Analyze database state with enhanced relationship mapping"""
        return self.analyze_database_state(db_state)
    
    def _analyze_business_context(self, intent: Dict[str, Any], time_context: Dict[str, Any], 
                                user_context: UserContext) -> BusinessContext:
        """Analyze business context from intent and time"""
        action = intent.get('action', 'query')
        workflow_context = self.get_business_workflow_context(user_context.role, action)
        
        # Determine business event
        business_event = self._determine_business_event(time_context)
        
        # Get compliance requirements
        compliance_requirements = self.vietnamese_patterns.get_regulatory_requirements(
            intent.get('target_database', 'admin_db'),
            intent.get('data_type', 'general')
        )
        
        return BusinessContext(
            current_workflow=workflow_context.workflow_type,
            business_event=business_event,
            department_interactions=workflow_context.department_interactions,
            compliance_requirements=compliance_requirements,
            data_sensitivity_level=SensitivityLevel(intent.get('sensitivity_level', 'internal'))
        )
    
    def _analyze_temporal_context(self, time_context: Dict[str, Any]) -> TemporalContext:
        """Analyze temporal context with Vietnamese business patterns"""
        current_time = time_context.get('current_time', datetime.now())
        
        # Enhanced Vietnamese work hour analysis
        work_hour_analysis = self.analyze_vietnamese_work_hours(current_time)
        
        # Enhanced holiday and business event analysis
        holiday_analysis = self.analyze_vietnamese_holidays_and_events(current_time)
        
        # Extract key temporal information
        current_hour = work_hour_analysis['current_hour']
        is_work_hours = work_hour_analysis['is_work_day'] and work_hour_analysis['activity_level'] > 0.5
        is_lunch_break = work_hour_analysis['is_lunch_break']
        is_vietnamese_holiday = holiday_analysis['is_holiday']
        business_cycle_phase = holiday_analysis['business_cycle_phase']
        
        # Calculate enhanced seasonal factor
        seasonal_factor = self._calculate_seasonal_factor(current_time.month, business_cycle_phase)
        seasonal_factor *= holiday_analysis['activity_impact']  # Apply holiday impact
        seasonal_factor *= work_hour_analysis['work_intensity_factor']  # Apply work hour impact
        
        # Ensure seasonal factor stays within valid range (0.0 to 2.0)
        seasonal_factor = min(max(seasonal_factor, 0.0), 2.0)
        
        return TemporalContext(
            current_hour=current_hour,
            is_work_hours=is_work_hours,
            is_lunch_break=is_lunch_break,
            is_vietnamese_holiday=is_vietnamese_holiday,
            business_cycle_phase=business_cycle_phase,
            seasonal_factor=seasonal_factor
        )
    
    def _analyze_cultural_context(self, intent: Dict[str, Any], temporal_context: TemporalContext,
                                user_context: UserContext) -> CulturalContext:
        """Analyze Vietnamese cultural context with enhanced constraints"""
        action = intent.get('action', 'query')
        current_time = intent.get('current_time', datetime.now())
        
        # Enhanced temporal analysis for cultural context
        work_hour_analysis = self.analyze_vietnamese_work_hours(current_time)
        holiday_analysis = self.analyze_vietnamese_holidays_and_events(current_time)
        
        # Apply enhanced cultural business constraints
        cultural_analysis = self.apply_cultural_business_constraints(
            action, user_context, {**work_hour_analysis, **holiday_analysis}
        )
        
        # Create enhanced cultural constraints
        enhanced_cultural_constraints = CulturalConstraints(
            hierarchy_level=cultural_analysis['hierarchy_level'],
            respect_seniority=cultural_analysis['respect_seniority'],
            work_overtime_acceptable=cultural_analysis['work_overtime_acceptable'],
            tet_preparation_mode=cultural_analysis['tet_preparation_mode']
        )
        
        # Get Vietnamese holidays with cultural context
        vietnamese_holidays = list(self.vietnamese_patterns.vietnamese_holidays.keys())
        
        # Enhanced business etiquette rules
        business_etiquette = {
            'hierarchy_respect': cultural_analysis['hierarchy_level'] >= 5,
            'formal_communication': cultural_analysis['formal_communication_required'],
            'overtime_acceptable': cultural_analysis['work_overtime_acceptable'],
            'senior_approval_required': cultural_analysis['senior_approval_required'],
            'cross_department_access': cultural_analysis['cross_department_access_allowed'],
            'cultural_sensitivity': cultural_analysis['cultural_sensitivity_level'],
            'business_etiquette_strictness': cultural_analysis['business_etiquette_strictness'],
            'tet_season_considerations': holiday_analysis.get('is_tet_season', False),
            'holiday_respect': holiday_analysis.get('is_holiday', False)
        }
        
        # Enhanced language preferences with cultural context
        language_preferences = {
            'primary': 'vietnamese',
            'business_terms': 'vietnamese',
            'technical_terms': 'english',
            'formal_level': 'high' if cultural_analysis['hierarchy_level'] >= 7 else 'medium',
            'cultural_terms': 'vietnamese_traditional' if holiday_analysis.get('is_tet_season', False) else 'vietnamese_modern'
        }
        
        return CulturalContext(
            cultural_constraints=enhanced_cultural_constraints,
            vietnamese_holidays=vietnamese_holidays,
            business_etiquette=business_etiquette,
            language_preferences=language_preferences
        )
    
    def _build_relationship_map(self, db_state: Dict[str, Any]) -> Dict[str, List[Relationship]]:
        """Build relationship map from database state"""
        relationship_map = {}
        
        for db_name, relationships in self.table_relationships.items():
            relationship_map[db_name] = relationships
        
        return relationship_map
    
    def _detect_constraint_violations(self, db_state: Dict[str, Any]) -> List[ConstraintViolation]:
        """Detect constraint violations from database state"""
        violations = []
        
        # Simulate constraint violation detection
        constraint_data = db_state.get('constraint_violations', [])
        for violation_data in constraint_data:
            violations.append(ConstraintViolation(
                constraint_type=violation_data.get('type', 'unknown'),
                table_name=violation_data.get('table', 'unknown'),
                column_name=violation_data.get('column', 'unknown'),
                violation_count=violation_data.get('count', 0)
            ))
        
        return violations
    
    def _analyze_recent_modifications(self, db_state: Dict[str, Any]) -> List[Modification]:
        """Analyze recent database modifications"""
        modifications = []
        
        # Simulate recent modification analysis
        mod_data = db_state.get('recent_modifications', [])
        for mod in mod_data:
            modifications.append(Modification(
                table_name=mod.get('table', 'unknown'),
                operation=mod.get('operation', 'SELECT'),
                timestamp=datetime.fromisoformat(mod.get('timestamp', datetime.now().isoformat())),
                affected_rows=mod.get('affected_rows', 0)
            ))
        
        return modifications
    
    def _collect_performance_metrics(self, db_state: Dict[str, Any]) -> PerformanceMetrics:
        """Collect database performance metrics"""
        metrics_data = db_state.get('performance_metrics', {})
        
        return PerformanceMetrics(
            avg_query_time=metrics_data.get('avg_query_time', 0.5),
            slow_query_count=metrics_data.get('slow_query_count', 0),
            connection_count=metrics_data.get('connection_count', 10),
            cache_hit_ratio=metrics_data.get('cache_hit_ratio', 0.8)
        )
    
    def _create_default_database_state(self) -> DatabaseState:
        """Create default database state for fallback"""
        return DatabaseState(
            entity_counts={},
            relationship_map={},
            constraint_violations=[],
            recent_modifications=[],
            performance_metrics=PerformanceMetrics(
                avg_query_time=1.0,
                slow_query_count=0,
                connection_count=5,
                cache_hit_ratio=0.7
            )
        )
    
    def _create_fallback_context(self, intent: Dict[str, Any]) -> QueryContext:
        """Create fallback context when analysis fails"""
        return QueryContext(
            user_context=self._build_user_context(intent),
            database_state=self._create_default_database_state(),
            business_context=BusinessContext(
                current_workflow=WorkflowType.ADMINISTRATIVE,
                business_event=None,
                department_interactions=[],
                compliance_requirements=[],
                data_sensitivity_level=SensitivityLevel.INTERNAL
            ),
            temporal_context=TemporalContext(
                current_hour=9,
                is_work_hours=True,
                is_lunch_break=False,
                is_vietnamese_holiday=False,
                business_cycle_phase=BusinessCyclePhase.TRANSITION,
                seasonal_factor=1.0
            ),
            cultural_context=CulturalContext(
                cultural_constraints=self.vietnamese_patterns.get_cultural_constraints('query', {}),
                vietnamese_holidays=[],
                business_etiquette={},
                language_preferences={}
            )
        )
    
    def _map_role_to_department(self, role: str) -> str:
        """Map user role to Vietnamese department"""
        role_dept_map = {
            'SALES': 'Phòng Kinh Doanh',
            'MARKETING': 'Phòng Marketing',
            'HR': 'Phòng Nhân Sự',
            'FINANCE': 'Phòng Tài Chính',
            'ADMIN': 'Phòng Hành Chính',
            'DEV': 'Phòng Công Nghệ Thông Tin',
            'CUSTOMER_SERVICE': 'Phòng Chăm Sóc Khách Hàng',
            'MANAGEMENT': 'Ban Giám Đốc'
        }
        return role_dept_map.get(role, 'Phòng Hành Chính')
    
    def _determine_workflow_type(self, action: str, department: str) -> WorkflowType:
        """Determine workflow type from action and department"""
        if 'sales' in action.lower() or department == 'Phòng Kinh Doanh':
            return WorkflowType.SALES_PROCESS
        elif 'customer' in action.lower() or department == 'Phòng Chăm Sóc Khách Hàng':
            return WorkflowType.CUSTOMER_SERVICE
        elif 'finance' in action.lower() or department == 'Phòng Tài Chính':
            return WorkflowType.FINANCIAL_REPORTING
        elif 'hr' in action.lower() or department == 'Phòng Nhân Sự':
            return WorkflowType.HR_MANAGEMENT
        elif 'marketing' in action.lower() or department == 'Phòng Marketing':
            return WorkflowType.MARKETING_CAMPAIGN
        elif 'inventory' in action.lower():
            return WorkflowType.INVENTORY_MANAGEMENT
        else:
            return WorkflowType.ADMINISTRATIVE
    
    def _calculate_complexity_level(self, role: str, action: str) -> int:
        """Calculate complexity level based on role and action"""
        base_complexity = {
            'MANAGEMENT': 4,
            'FINANCE': 4,
            'DEV': 3,
            'SALES': 2,
            'HR': 2,
            'ADMIN': 1
        }.get(role, 2)
        
        if 'report' in action.lower():
            base_complexity += 1
        elif 'analysis' in action.lower():
            base_complexity += 2
        
        return min(max(base_complexity, 1), 5)
    
    def _find_related_tables(self, table: str, relationships: List[Relationship]) -> List[str]:
        """Find tables related to given table"""
        related = []
        for rel in relationships:
            if rel.from_table == table:
                related.append(rel.to_table)
            elif rel.to_table == table:
                related.append(rel.from_table)
        return list(set(related))
    
    def _calculate_join_paths(self, table: str, relationships: List[Relationship], 
                            schema: Dict[str, List[str]]) -> List[str]:
        """Calculate possible join paths from table"""
        join_paths = []
        for rel in relationships:
            if rel.from_table == table:
                join_paths.append(f"{table}.{rel.foreign_key} = {rel.to_table}.{rel.foreign_key}")
            elif rel.to_table == table:
                join_paths.append(f"{rel.from_table}.{rel.foreign_key} = {table}.{rel.foreign_key}")
        return join_paths
    
    def _find_constraint_dependencies(self, table: str, relationships: List[Relationship]) -> List[str]:
        """Find constraint dependencies for table"""
        dependencies = []
        for rel in relationships:
            if rel.from_table == table:
                dependencies.append(f"FK: {rel.foreign_key} -> {rel.to_table}")
        return dependencies
    
    def _determine_business_event(self, time_context: Dict[str, Any]) -> Optional[BusinessEvent]:
        """Determine current business event from time context"""
        current_time = time_context.get('current_time', datetime.now())
        
        # Check for month-end
        if current_time.day >= 28:
            return BusinessEvent.MONTH_END_CLOSING
        
        # Check for quarter-end
        if current_time.month in [3, 6, 9, 12] and current_time.day >= 25:
            return BusinessEvent.QUARTER_END_REPORTING
        
        # Check for Tet season (January-February)
        if current_time.month in [1, 2]:
            return BusinessEvent.TET_PREPARATION
        
        return None  # Normal operations, no special business event
    
    def _calculate_seasonal_factor(self, month: int, business_cycle: BusinessCyclePhase) -> float:
        """Calculate seasonal activity factor"""
        if business_cycle == BusinessCyclePhase.PEAK_SEASON:
            return 1.5
        elif business_cycle == BusinessCyclePhase.HOLIDAY_SEASON:
            return 0.7
        elif business_cycle == BusinessCyclePhase.LOW_SEASON:
            return 0.8
        else:
            return 1.0
    
    def analyze_vietnamese_work_hours(self, current_time: datetime) -> Dict[str, Any]:
        """
        Analyze Vietnamese work hour patterns and business cycle context
        
        Args:
            current_time: Current timestamp
            
        Returns:
            Dictionary with work hour analysis results
        """
        current_hour = current_time.hour
        current_weekday = current_time.weekday()  # 0=Monday, 6=Sunday
        
        # Standard Vietnamese work schedule
        is_weekend = current_weekday >= 5  # Saturday=5, Sunday=6
        is_work_day = not is_weekend
        
        # Work hour detection
        is_early_morning = 6 <= current_hour < 8
        is_morning_rush = 8 <= current_hour < 10
        is_mid_morning = 10 <= current_hour < 12
        is_lunch_break = 12 <= current_hour < 13
        is_afternoon = 13 <= current_hour < 17
        is_end_of_day = 17 <= current_hour < 19
        is_evening = 19 <= current_hour < 22
        is_night = current_hour >= 22 or current_hour < 6
        
        # Activity level calculation
        if is_weekend:
            activity_level = 0.1  # Very low weekend activity
        elif is_lunch_break:
            activity_level = 0.3  # Reduced lunch activity
        elif is_morning_rush or is_afternoon:
            activity_level = 1.5  # Peak activity periods
        elif is_mid_morning or is_end_of_day:
            activity_level = 1.2  # High activity
        elif is_early_morning:
            activity_level = 0.8  # Early arrivals
        elif is_evening:
            activity_level = 0.6  # Overtime work
        else:  # Night hours
            activity_level = 0.2  # Minimal night activity
        
        # Business context
        business_context = "normal"
        if is_weekend:
            business_context = "weekend"
        elif is_lunch_break:
            business_context = "lunch_break"
        elif is_morning_rush:
            business_context = "morning_peak"
        elif is_afternoon:
            business_context = "afternoon_peak"
        elif is_evening:
            business_context = "overtime"
        elif is_night:
            business_context = "maintenance"
        
        return {
            'current_hour': current_hour,
            'is_work_day': is_work_day,
            'is_weekend': is_weekend,
            'activity_level': activity_level,
            'business_context': business_context,
            'is_peak_hours': is_morning_rush or is_afternoon,
            'is_lunch_break': is_lunch_break,
            'is_overtime_hours': is_evening,
            'work_intensity_factor': min(activity_level, 2.0)
        }
    
    def analyze_vietnamese_holidays_and_events(self, current_time: datetime) -> Dict[str, Any]:
        """
        Analyze Vietnamese holidays and business events impact
        
        Args:
            current_time: Current timestamp
            
        Returns:
            Dictionary with holiday and event analysis
        """
        date_str = current_time.strftime('%Y-%m-%d')
        month = current_time.month
        day = current_time.day
        
        # Check for Vietnamese holidays
        is_holiday = self.vietnamese_patterns.is_vietnamese_holiday(date_str)
        holiday_name = self.vietnamese_patterns.get_holiday_name(date_str) if is_holiday else None
        
        # Tet season analysis (extended period around Lunar New Year)
        is_tet_season = month in [1, 2]  # January-February
        tet_preparation_phase = False
        tet_celebration_phase = False
        
        if is_tet_season:
            # Tet preparation typically starts 2 weeks before
            if month == 1 and day < 15:
                tet_preparation_phase = True
            elif month == 1 and 15 <= day <= 31:
                tet_celebration_phase = True
            elif month == 2 and day < 15:
                tet_celebration_phase = True
        
        # Business cycle events
        is_month_end = day >= 28
        is_quarter_end = month in [3, 6, 9, 12] and day >= 25
        is_year_end = month == 12 and day >= 20
        
        # Activity impact calculation
        activity_impact = 1.0  # Normal activity
        
        if is_holiday:
            activity_impact = 0.1  # Minimal activity on holidays
        elif tet_celebration_phase:
            activity_impact = 0.3  # Reduced activity during Tet
        elif tet_preparation_phase:
            activity_impact = 0.8  # Slightly reduced for preparation
        elif is_year_end:
            activity_impact = 1.3  # Increased year-end activity
        elif is_quarter_end:
            activity_impact = 1.2  # Increased quarter-end activity
        elif is_month_end:
            activity_impact = 1.1  # Slightly increased month-end activity
        
        # Cultural considerations
        cultural_considerations = []
        if is_holiday:
            cultural_considerations.append("respect_holiday_traditions")
        if is_tet_season:
            cultural_considerations.append("tet_cultural_sensitivity")
        if is_month_end or is_quarter_end:
            cultural_considerations.append("business_deadline_pressure")
        
        return {
            'is_holiday': is_holiday,
            'holiday_name': holiday_name,
            'is_tet_season': is_tet_season,
            'tet_preparation_phase': tet_preparation_phase,
            'tet_celebration_phase': tet_celebration_phase,
            'is_month_end': is_month_end,
            'is_quarter_end': is_quarter_end,
            'is_year_end': is_year_end,
            'activity_impact': activity_impact,
            'cultural_considerations': cultural_considerations,
            'business_cycle_phase': self.vietnamese_patterns.get_business_cycle_phase(month)
        }
    
    def apply_cultural_business_constraints(self, action: str, user_context: UserContext, 
                                         temporal_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply Vietnamese cultural business constraints to context analysis
        
        Args:
            action: Action being performed
            user_context: User context information
            temporal_analysis: Temporal analysis results
            
        Returns:
            Dictionary with cultural constraint analysis
        """
        # Get base cultural constraints
        time_context = {
            'current_hour': temporal_analysis.get('current_hour', 9),
            'is_vietnamese_holiday': temporal_analysis.get('is_holiday', False),
            'is_tet_season': temporal_analysis.get('is_tet_season', False)
        }
        
        cultural_constraints = self.vietnamese_patterns.get_cultural_constraints(action, time_context)
        
        # Role-based hierarchy adjustments
        hierarchy_adjustment = 0
        if user_context.role == 'MANAGEMENT':
            hierarchy_adjustment = 2  # Higher hierarchy respect for management
        elif user_context.role in ['FINANCE', 'HR']:
            hierarchy_adjustment = 1  # Moderate increase for sensitive roles
        elif user_context.role in ['DEV', 'ADMIN']:
            hierarchy_adjustment = -1  # Slightly lower for technical roles
        
        adjusted_hierarchy = min(max(cultural_constraints.hierarchy_level + hierarchy_adjustment, 1), 10)
        
        # Time-based constraint adjustments
        overtime_acceptable = cultural_constraints.work_overtime_acceptable
        if temporal_analysis.get('is_weekend', False):
            overtime_acceptable = False  # No overtime on weekends typically
        elif temporal_analysis.get('is_holiday', False):
            overtime_acceptable = False  # No work on holidays
        elif temporal_analysis.get('current_hour', 9) > 19:
            overtime_acceptable = False  # Very late hours not acceptable
        
        # Stress and work intensity impact
        stress_impact = user_context.stress_level
        work_intensity_impact = user_context.work_intensity
        
        # Adjust constraints based on stress and intensity
        if stress_impact > 0.7:  # High stress
            adjusted_hierarchy += 1  # More formal when stressed
            overtime_acceptable = overtime_acceptable and work_intensity_impact < 1.5
        
        # Action-specific adjustments
        action_sensitivity = {
            'financial_report': 3,
            'audit': 3,
            'hr_data': 2,
            'customer_data': 2,
            'sales_report': 1,
            'routine_query': 0
        }
        
        sensitivity_boost = action_sensitivity.get(action.lower(), 0)
        adjusted_hierarchy = min(adjusted_hierarchy + sensitivity_boost, 10)
        
        # Department interaction constraints
        department = self._map_role_to_department(user_context.role)
        cross_department_access = action.lower() in ['report', 'analysis', 'audit']
        
        # Seniority respect based on Vietnamese culture
        respect_seniority = cultural_constraints.respect_seniority
        if user_context.expertise_level in [ExpertiseLevel.NOVICE, ExpertiseLevel.INTERMEDIATE]:
            respect_seniority = True  # Junior staff always respect seniority
        
        return {
            'hierarchy_level': adjusted_hierarchy,
            'respect_seniority': respect_seniority,
            'work_overtime_acceptable': overtime_acceptable,
            'tet_preparation_mode': cultural_constraints.tet_preparation_mode,
            'cross_department_access_allowed': cross_department_access,
            'formal_communication_required': adjusted_hierarchy >= 6,
            'senior_approval_required': adjusted_hierarchy >= 8,
            'cultural_sensitivity_level': 'high' if temporal_analysis.get('is_tet_season', False) else 'normal',
            'business_etiquette_strictness': min(adjusted_hierarchy / 2, 5),
            'department': department,
            'stress_impact_factor': stress_impact,
            'work_intensity_factor': work_intensity_impact
        }