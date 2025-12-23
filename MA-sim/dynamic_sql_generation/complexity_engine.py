"""
Query Complexity Engine for Dynamic SQL Generation System

Determines appropriate query sophistication based on user expertise,
business context, and Vietnamese cultural patterns. Adapts query
complexity to match user capabilities and business requirements.
"""

import logging
import random
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

from .models import (
    UserContext, BusinessContext, TemporalContext, ExpertiseLevel,
    WorkflowType, BusinessEvent, SensitivityLevel
)
from .config import get_generation_config, get_vietnamese_config
from .vietnamese_patterns import VietnameseBusinessPatterns


class ComplexityLevel(Enum):
    """Query complexity levels"""
    SIMPLE = 1      # Basic SELECT, single table
    BASIC = 2       # Simple JOINs, basic WHERE
    INTERMEDIATE = 3 # Multiple JOINs, GROUP BY
    ADVANCED = 4    # Subqueries, window functions
    EXPERT = 5      # Complex analytics, CTEs


@dataclass
class ComplexityAssessment:
    """Result of complexity level assessment"""
    complexity_level: ComplexityLevel
    user_expertise_factor: float
    business_context_factor: float
    temporal_factor: float
    cultural_factor: float
    final_score: float
    reasoning: List[str]


@dataclass
class QueryGenerationStrategy:
    """Strategy for generating queries at specific complexity"""
    max_tables: int
    max_joins: int
    allow_subqueries: bool
    allow_aggregations: bool
    allow_window_functions: bool
    allow_ctes: bool
    max_where_conditions: int
    preferred_patterns: List[str]


class QueryComplexityEngine:
    """
    Query Complexity Engine for determining appropriate query sophistication
    based on user expertise, business context, and Vietnamese business patterns
    """
    
    def __init__(self):
        self.generation_config = get_generation_config()
        self.vietnamese_config = get_vietnamese_config()
        self.vietnamese_patterns = VietnameseBusinessPatterns()
        self.logger = logging.getLogger(__name__)
        
        # Define complexity strategies
        self._complexity_strategies = {
            ComplexityLevel.SIMPLE: QueryGenerationStrategy(
                max_tables=1,
                max_joins=0,
                allow_subqueries=False,
                allow_aggregations=False,
                allow_window_functions=False,
                allow_ctes=False,
                max_where_conditions=2,
                preferred_patterns=['simple_select', 'basic_filter']
            ),
            ComplexityLevel.BASIC: QueryGenerationStrategy(
                max_tables=2,
                max_joins=1,
                allow_subqueries=False,
                allow_aggregations=True,
                allow_window_functions=False,
                allow_ctes=False,
                max_where_conditions=3,
                preferred_patterns=['inner_join', 'basic_aggregation', 'simple_group_by']
            ),
            ComplexityLevel.INTERMEDIATE: QueryGenerationStrategy(
                max_tables=3,
                max_joins=2,
                allow_subqueries=True,
                allow_aggregations=True,
                allow_window_functions=False,
                allow_ctes=False,
                max_where_conditions=4,
                preferred_patterns=['multi_join', 'subquery', 'group_by_having', 'case_when']
            ),
            ComplexityLevel.ADVANCED: QueryGenerationStrategy(
                max_tables=4,
                max_joins=3,
                allow_subqueries=True,
                allow_aggregations=True,
                allow_window_functions=True,
                allow_ctes=False,
                max_where_conditions=5,
                preferred_patterns=['window_functions', 'correlated_subquery', 'exists_not_exists', 'union']
            ),
            ComplexityLevel.EXPERT: QueryGenerationStrategy(
                max_tables=5,
                max_joins=4,
                allow_subqueries=True,
                allow_aggregations=True,
                allow_window_functions=True,
                allow_ctes=True,
                max_where_conditions=6,
                preferred_patterns=['cte', 'recursive_cte', 'advanced_analytics', 'pivot_unpivot']
            )
        }
        
        # User expertise scoring
        self._expertise_scores = {
            ExpertiseLevel.NOVICE: 1.0,
            ExpertiseLevel.INTERMEDIATE: 2.5,
            ExpertiseLevel.ADVANCED: 4.0,
            ExpertiseLevel.EXPERT: 5.0
        }
        
        # Business context complexity requirements
        self._workflow_complexity = {
            WorkflowType.ADMINISTRATIVE: 1.5,
            WorkflowType.SALES_PROCESS: 2.0,
            WorkflowType.CUSTOMER_SERVICE: 2.0,
            WorkflowType.HR_MANAGEMENT: 2.5,
            WorkflowType.MARKETING_CAMPAIGN: 3.0,
            WorkflowType.INVENTORY_MANAGEMENT: 3.0,
            WorkflowType.FINANCIAL_REPORTING: 4.0,
            WorkflowType.MAINTENANCE: 1.0
        }
        
        # Business event complexity modifiers
        self._event_complexity_modifiers = {
            BusinessEvent.NORMAL_OPERATIONS: 1.0,
            BusinessEvent.MONTH_END_CLOSING: 1.3,
            BusinessEvent.QUARTER_END_REPORTING: 1.5,
            BusinessEvent.AUDIT_PERIOD: 1.4,
            BusinessEvent.BUDGET_PLANNING: 1.3,
            BusinessEvent.PERFORMANCE_REVIEW: 1.2,
            BusinessEvent.TET_PREPARATION: 0.8,
            BusinessEvent.HOLIDAY_PERIOD: 0.6
        }
    
    def _randomize_structure(self, query: str) -> str:
        """
        Randomize SQL structure to prevent overfitting (whitespace, casing, etc.)
        Does not change semantics.
        """
        # 1. Randomize keywords (Simple approach: SELECT -> select)
        if random.random() < 0.3:
            query = query.replace("SELECT", "select").replace("FROM", "from").replace("WHERE", "where")
        
        # 2. Randomize whitespace
        if random.random() < 0.4:
            query = query.replace(" ", "  ") # Double spaces
        
        # # 3. Random comments
        # if random.random() < 0.2:
        #     comments = ["/* check */ ", "/* audit */ ", "-- status check\n"]
        #     query = random.choice(comments) + query
            
        return query.strip()

    def determine_complexity(self, user_context: UserContext, 
                           business_context: BusinessContext,
                           temporal_context: TemporalContext) -> ComplexityAssessment:
        """
        Determine appropriate query complexity based on context
        
        Args:
            user_context: Context of the user performing the action
            business_context: Business context of the action
            temporal_context: Temporal context (time, holidays, etc.)
            
        Returns:
            ComplexityAssessment with level and reasoning
        """
        try:
            reasoning = []
            
            # 1. Assess user expertise factor
            user_expertise_factor = self._assess_user_expertise(user_context, reasoning)
            
            # 2. Assess business context requirements
            business_context_factor = self._assess_business_context(business_context, reasoning)
            
            # 3. Assess temporal factors
            temporal_factor = self._assess_temporal_factors(temporal_context, reasoning) if temporal_context else 1.0
            
            # 4. Apply Vietnamese cultural factors
            cultural_factor = self._assess_cultural_factors(user_context, business_context, reasoning)
            
            # 5. Calculate final complexity score
            base_score = user_expertise_factor * 0.4 + business_context_factor * 0.3
            adjusted_score = base_score * temporal_factor * cultural_factor
            
            # 6. Map score to complexity level
            complexity_level = self._map_score_to_complexity(adjusted_score)
            
            reasoning.append(f"Final complexity score: {adjusted_score:.2f} -> {complexity_level.name}")
            
            return ComplexityAssessment(
                complexity_level=complexity_level,
                user_expertise_factor=user_expertise_factor,
                business_context_factor=business_context_factor,
                temporal_factor=temporal_factor,
                cultural_factor=cultural_factor,
                final_score=adjusted_score,
                reasoning=reasoning
            )
            
        except Exception as e:
            self.logger.error(f"Error determining complexity level: {e}")
            return self._create_fallback_assessment(reasoning)
    
    def _assess_user_expertise(self, user_context: UserContext, reasoning: List[str]) -> float:
        """Assess user expertise level and adjust based on behavior patterns"""
        base_score = self._expertise_scores[user_context.expertise_level]
        reasoning.append(f"Base expertise ({user_context.expertise_level.value}): {base_score}")
        
        # Adjust based on session history
        if user_context.session_history:
            success_rate = sum(1 for h in user_context.session_history if h.success) / len(user_context.session_history)
            avg_complexity = self._calculate_avg_historical_complexity(user_context.session_history)
            
            # Success rate adjustment
            if success_rate > 0.8:
                base_score *= 1.1  # Boost for high success rate
                reasoning.append(f"High success rate ({success_rate:.1%}): +10%")
            elif success_rate < 0.6:
                base_score *= 0.9  # Reduce for low success rate
                reasoning.append(f"Low success rate ({success_rate:.1%}): -10%")
            
            # Historical complexity adjustment
            if avg_complexity > base_score:
                base_score = min(base_score * 1.05, avg_complexity)
                reasoning.append(f"Historical complexity suggests higher capability: +5%")
        
        # Role-based adjustments
        role_adjustments = {
            'MANAGEMENT': 1.2,  # Management often needs complex reports
            'FINANCE': 1.15,    # Finance requires analytical queries
            'DEV': 1.1,         # Developers comfortable with complexity
            'ADMIN': 0.95,      # Admin tasks often simpler
            'SALES': 1.0,       # Standard complexity
            'HR': 1.05,         # Slightly above average
            'MARKETING': 1.1    # Marketing analytics
        }
        
        role_factor = role_adjustments.get(user_context.role, 1.0)
        if role_factor != 1.0:
            base_score *= role_factor
            reasoning.append(f"Role adjustment ({user_context.role}): {role_factor:.0%}")
        
        # Stress and work intensity impact
        if user_context.stress_level > 0.7:
            base_score *= 0.9  # High stress reduces complexity handling
            reasoning.append(f"High stress level ({user_context.stress_level:.1f}): -10%")
        
        if user_context.work_intensity > 1.5:
            base_score *= 0.95  # High intensity may reduce focus on complex queries
            reasoning.append(f"High work intensity ({user_context.work_intensity:.1f}): -5%")
        
        return min(max(base_score, 1.0), 5.0)  # Clamp to valid range
    
    def _assess_business_context(self, business_context: BusinessContext, reasoning: List[str]) -> float:
        """Assess business context complexity requirements"""
        # Base workflow complexity
        base_score = self._workflow_complexity[business_context.current_workflow]
        reasoning.append(f"Workflow complexity ({business_context.current_workflow.value}): {base_score}")
        
        # Business event modifier
        if business_context.business_event:
            event_modifier = self._event_complexity_modifiers.get(
                business_context.business_event, 1.0
            )
            base_score *= event_modifier
            reasoning.append(f"Business event ({business_context.business_event.value}): {event_modifier:.0%}")
        
        # Data sensitivity impact
        sensitivity_modifiers = {
            SensitivityLevel.PUBLIC: 0.9,
            SensitivityLevel.INTERNAL: 1.0,
            SensitivityLevel.CONFIDENTIAL: 1.1,
            SensitivityLevel.RESTRICTED: 1.2
        }
        
        sensitivity_modifier = sensitivity_modifiers[business_context.data_sensitivity_level]
        if sensitivity_modifier != 1.0:
            base_score *= sensitivity_modifier
            reasoning.append(f"Data sensitivity ({business_context.data_sensitivity_level.value}): {sensitivity_modifier:.0%}")
        
        # Department interactions complexity
        if len(business_context.department_interactions) > 2:
            base_score *= 1.1  # Cross-department queries are more complex
            reasoning.append(f"Multi-department interaction ({len(business_context.department_interactions)} depts): +10%")
        
        # Compliance requirements impact
        if len(business_context.compliance_requirements) > 1:
            base_score *= 1.05  # Multiple compliance rules add complexity
            reasoning.append(f"Multiple compliance requirements ({len(business_context.compliance_requirements)}): +5%")
        
        return min(max(base_score, 1.0), 5.0)  # Clamp to valid range
    
    def _assess_temporal_factors(self, temporal_context: TemporalContext, reasoning: List[str]) -> float:
        """Assess temporal factors affecting complexity"""
        factor = 1.0
        
        # Work hours impact
        if not temporal_context.is_work_hours:
            factor *= 0.9  # Off-hours queries tend to be simpler
            reasoning.append("Off-hours: -10% complexity")
        
        # Lunch break impact
        if temporal_context.is_lunch_break:
            factor *= 0.8  # Lunch time queries should be quick
            reasoning.append("Lunch break: -20% complexity")
        
        # Holiday impact
        if temporal_context.is_vietnamese_holiday:
            factor *= 0.7  # Holiday queries should be minimal
            reasoning.append("Vietnamese holiday: -30% complexity")
        
        # Business cycle impact
        cycle_factors = {
            'PEAK_SEASON': 1.1,     # Peak season may need more complex analysis
            'LOW_SEASON': 0.95,     # Low season simpler queries
            'HOLIDAY_SEASON': 0.8,  # Holiday season reduced complexity
            'TRANSITION': 1.0       # Normal complexity
        }
        
        cycle_factor = cycle_factors.get(temporal_context.business_cycle_phase.value, 1.0)
        if cycle_factor != 1.0:
            factor *= cycle_factor
            reasoning.append(f"Business cycle ({temporal_context.business_cycle_phase.value}): {cycle_factor:.0%}")
        
        # Seasonal factor impact
        if temporal_context.seasonal_factor < 0.8:
            factor *= 0.9  # Low seasonal activity reduces complexity needs
            reasoning.append(f"Low seasonal activity ({temporal_context.seasonal_factor:.1f}): -10%")
        elif temporal_context.seasonal_factor > 1.3:
            factor *= 1.05  # High seasonal activity may increase complexity needs
            reasoning.append(f"High seasonal activity ({temporal_context.seasonal_factor:.1f}): +5%")
        
        return factor
    
    def _assess_cultural_factors(self, user_context: UserContext, 
                               business_context: BusinessContext, reasoning: List[str]) -> float:
        """Assess Vietnamese cultural factors affecting complexity"""
        factor = 1.0
        
        # Hierarchy considerations - junior staff get simpler queries
        if user_context.expertise_level == ExpertiseLevel.NOVICE:
            factor *= 0.85  # Junior staff should start with simpler queries
            reasoning.append("Junior staff (cultural hierarchy): -15%")
        
        # Department cultural patterns
        dept_factors = {
            'Phòng Tài Chính': 1.1,      # Finance dept expects complex analysis
            'Phòng Nhân Sự': 1.0,        # HR standard complexity
            'Phòng Kinh Doanh': 1.05,    # Sales slightly above average
            'Phòng Marketing': 1.1,       # Marketing analytics
            'Ban Giám Đốc': 1.2          # Management highest complexity
        }
        
        dept_factor = dept_factors.get(user_context.department, 1.0)
        if dept_factor != 1.0:
            factor *= dept_factor
            reasoning.append(f"Department culture ({user_context.department}): {dept_factor:.0%}")
        
        # Work intensity cultural acceptance
        if user_context.work_intensity > 1.5 and self.vietnamese_config.overtime_acceptance_rate < 0.7:
            factor *= 0.95  # High intensity with low overtime acceptance
            reasoning.append("High intensity + low overtime acceptance: -5%")
        
        return factor
    
    def _calculate_avg_historical_complexity(self, session_history: List) -> float:
        """Calculate average complexity from session history"""
        if not session_history:
            return 2.0  # Default intermediate
        
        complexity_map = {
            'simple': 1.0,
            'basic': 2.0,
            'medium': 3.0,
            'intermediate': 3.0,
            'complex': 4.0,
            'advanced': 4.0,
            'expert': 5.0
        }
        
        total_complexity = 0
        count = 0
        
        for history in session_history:
            complexity = complexity_map.get(history.complexity_level.lower(), 2.0)
            total_complexity += complexity
            count += 1
        
        return total_complexity / count if count > 0 else 2.0
    
    def _map_score_to_complexity(self, score: float) -> ComplexityLevel:
        """Map numerical score to complexity level"""
        if score <= 1.5:
            return ComplexityLevel.SIMPLE
        elif score <= 2.5:
            return ComplexityLevel.BASIC
        elif score <= 3.5:
            return ComplexityLevel.INTERMEDIATE
        elif score <= 4.5:
            return ComplexityLevel.ADVANCED
        else:
            return ComplexityLevel.EXPERT
    
    def _create_fallback_assessment(self, reasoning: List[str]) -> ComplexityAssessment:
        """Create fallback assessment when analysis fails"""
        reasoning.append("Error in complexity assessment - using fallback")
        return ComplexityAssessment(
            complexity_level=ComplexityLevel.BASIC,
            user_expertise_factor=2.0,
            business_context_factor=2.0,
            temporal_factor=1.0,
            cultural_factor=1.0,
            final_score=2.0,
            reasoning=reasoning
        )
    
    def get_generation_strategy(self, complexity_level: ComplexityLevel) -> QueryGenerationStrategy:
        """Get query generation strategy for complexity level"""
        return self._complexity_strategies[complexity_level]
    
    def adjust_complexity_based_on_success_rate(self, user_context: UserContext, 
                                              current_complexity: ComplexityLevel,
                                              recent_success_rate: float) -> ComplexityLevel:
        """
        Dynamically adjust complexity based on user's recent success rate
        
        Args:
            user_context: Current user context
            current_complexity: Current complexity level
            recent_success_rate: Success rate from recent queries (0.0 to 1.0)
            
        Returns:
            Adjusted complexity level
        """
        try:
            # If success rate is very high, consider increasing complexity
            if recent_success_rate > 0.9 and current_complexity != ComplexityLevel.EXPERT:
                next_level = ComplexityLevel(current_complexity.value + 1)
                self.logger.info(f"High success rate ({recent_success_rate:.1%}) - increasing complexity to {next_level.name}")
                return next_level
            
            # If success rate is low, consider decreasing complexity
            elif recent_success_rate < 0.6 and current_complexity != ComplexityLevel.SIMPLE:
                prev_level = ComplexityLevel(current_complexity.value - 1)
                self.logger.info(f"Low success rate ({recent_success_rate:.1%}) - decreasing complexity to {prev_level.name}")
                return prev_level
            
            # Otherwise maintain current complexity
            return current_complexity
            
        except Exception as e:
            self.logger.error(f"Error adjusting complexity: {e}")
            return current_complexity
    
    def get_complexity_explanation(self, assessment: ComplexityAssessment) -> str:
        """Get human-readable explanation of complexity assessment"""
        explanation = f"Query complexity set to {assessment.complexity_level.name} "
        explanation += f"(score: {assessment.final_score:.2f})\n\n"
        explanation += "Factors considered:\n"
        
        for reason in assessment.reasoning:
            explanation += f"• {reason}\n"
        
        strategy = self.get_generation_strategy(assessment.complexity_level)
        explanation += f"\nGeneration strategy:\n"
        explanation += f"• Max tables: {strategy.max_tables}\n"
        explanation += f"• Max joins: {strategy.max_joins}\n"
        explanation += f"• Subqueries: {'Yes' if strategy.allow_subqueries else 'No'}\n"
        explanation += f"• Aggregations: {'Yes' if strategy.allow_aggregations else 'No'}\n"
        explanation += f"• Window functions: {'Yes' if strategy.allow_window_functions else 'No'}\n"
        
        return explanation
    
    def generate_complex_query(self, base_intent: str, complexity_level: ComplexityLevel, 
                             context: Dict[str, Any]) -> str:
        """
        Generate sophisticated SQL query based on complexity level and context
        
        Args:
            base_intent: Base query intent (e.g., "customer_analysis", "sales_report")
            complexity_level: Target complexity level
            context: Additional context for query generation
            
        Returns:
            Generated SQL query string
        """
        try:
            strategy = self.get_generation_strategy(complexity_level)
            
            # Extract context information
            target_database = context.get('target_database', 'sales_db')
            user_role = context.get('user_role', 'USER')
            business_context = context.get('business_context', {})
            
            # Generate query based on complexity level
            if complexity_level == ComplexityLevel.SIMPLE:
                return self._generate_simple_query(base_intent, target_database, context)
            elif complexity_level == ComplexityLevel.BASIC:
                return self._generate_basic_query(base_intent, target_database, context, strategy)
            elif complexity_level == ComplexityLevel.INTERMEDIATE:
                return self._generate_intermediate_query(base_intent, target_database, context, strategy)
            elif complexity_level == ComplexityLevel.ADVANCED:
                return self._generate_advanced_query(base_intent, target_database, context, strategy)
            else:  # EXPERT
                return self._generate_expert_query(base_intent, target_database, context, strategy)
                
        except Exception as e:
            self.logger.error(f"Error generating complex query: {e}")
            return self._generate_fallback_query(base_intent, target_database)
    
    def _generate_simple_query(self, intent: str, database: str, context: Dict[str, Any]) -> str:
        """Generate simple single-table query"""
        # Map database to table AND search column
        # Schema based on executor.py mappings
        schema_map = {
            'sales_db': {'table': 'customers', 'col': 'email'},
            'hr_db': {'table': 'employees', 'col': 'name'},
            'finance_db': {'table': 'invoices', 'col': 'invoice_number'}, 
            'marketing_db': {'table': 'campaigns', 'col': 'campaign_name'},
            'support_db': {'table': 'support_tickets', 'col': 'subject'},
            'inventory_db': {'table': 'inventory_levels', 'col': 'location_id'},
            'admin_db': {'table': 'users', 'col': 'username'}
        }
        
        schema = schema_map.get(database, {'table': 'customers', 'col': 'contact_person'})
        table = schema['table']
        col = schema['col']
        
        if 'search' in intent.lower():
            # [NEW] Dynamic realistic search patterns
            given_name = self.vietnamese_patterns.get_random_given_name()
            patterns = [
                f"SELECT * FROM {table} WHERE {col} LIKE '%{given_name}%' LIMIT {random.choice([5, 10, 15, 20])};",
                f"SELECT * FROM {table} WHERE {col} LIKE '%{self.vietnamese_patterns.get_random_given_name()}%' LIMIT {random.choice([5, 10])};",
                f"SELECT * FROM {table} WHERE {col} LIKE '%{self.vietnamese_patterns.get_random_given_name()}%' ORDER BY {col};"
            ]
            return random.choice(patterns)
        elif 'count' in intent.lower():
            return f"SELECT COUNT(*) as total FROM {table};"
        elif 'recent' in intent.lower():
            date_column = 'created_date' if database == 'sales_db' else 'date'
            if database == 'finance_db': date_column = 'invoice_date'
            if database == 'marketing_db': date_column = 'start_date'
            patterns = [
                f"SELECT * FROM {table} ORDER BY {date_column} DESC LIMIT 5;",
                f"SELECT * FROM {table} LIMIT 10;" 
            ]
            return random.choice(patterns)
        elif 'login' in intent.lower():
            # [NEW] Realistic login queries with literals instead of placeholders
            if database == 'admin_db':
                patterns = [
                    f"SELECT session_id, user_id, ip_address FROM user_sessions WHERE user_id = 'admin' AND is_active = 1;",
                    f"SELECT * FROM user_sessions WHERE user_id = 'user@example.com' LIMIT 1;",
                    f"UPDATE user_sessions SET last_activity = NOW() WHERE user_id = 'user_1';",
                    f"SELECT message FROM system_logs WHERE log_level = 'error' LIMIT 5;"
                ]
            else:
                # For non-admin DBs, simulate checking the main entity
                patterns = [
                    f"SELECT * FROM {table} WHERE {col} = 'admin' LIMIT 1;",
                    f"SELECT * FROM {table} WHERE {col} LIKE 'user%' LIMIT 1;",
                    f"SELECT * FROM {table} ORDER BY {col} LIMIT 1;"
                ]
            return self._randomize_structure(random.choice(patterns))
        else:
            # [NEW] Varied fallback patterns
            patterns = [
                f"SELECT * FROM {table} LIMIT 10;",
                f"SELECT * FROM {table} LIMIT 20;",
                f"SELECT * FROM {table} LIMIT 5;",
                f"SELECT COUNT(*) FROM {table};"
            ]
            return self._randomize_structure(random.choice(patterns))
    
    def _generate_basic_query(self, intent: str, database: str, context: Dict[str, Any], 
                            strategy: QueryGenerationStrategy) -> str:
        """Generate basic query with simple joins and aggregations"""
        
        query = ""
        if database == 'sales_db':
            if 'customer' in intent.lower() and 'order' in intent.lower():
                query = """
                SELECT c.company_name, c.city, COUNT(o.order_id) as order_count, SUM(o.total_amount) as total_spent
                FROM customers c
                LEFT JOIN orders o ON c.customer_id = o.customer_id
                WHERE c.city IN ('Hồ Chí Minh', 'Hà Nội', 'Đà Nẵng')
                GROUP BY c.customer_id, c.company_name, c.city
                ORDER BY total_spent DESC
                LIMIT 20;
                """.strip()
            else:
                city = random.choice(self.vietnamese_patterns.vietnamese_cities)
                query = f"""
                SELECT company_name, email, city, contact_person
                FROM customers 
                WHERE city = '{city}' 
                ORDER BY company_name
                LIMIT {random.choice([10, 15, 20])};
                """.strip()
        
        elif database == 'hr_db':
            query = """
            SELECT d.dept_name as department, COUNT(e.employee_id) as employee_count, AVG(e.salary) as avg_salary
            FROM departments d
            LEFT JOIN employees e ON d.dept_id = e.dept_id
            GROUP BY d.dept_id, d.dept_name
            ORDER BY employee_count DESC;
            """.strip()
        
        elif database == 'finance_db':
            query = """
            SELECT 
                DATE_FORMAT(invoice_date, '%Y-%m') as month,
                COUNT(*) as invoice_count,
                SUM(total_amount) as total_amount
            FROM invoices 
            WHERE invoice_date >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
            GROUP BY DATE_FORMAT(invoice_date, '%Y-%m')
            ORDER BY month DESC;
            """.strip()
        
        else:
            return self._generate_simple_query(intent, database, context)
            
        return self._randomize_structure(query)
    
    def _generate_intermediate_query(self, intent: str, database: str, context: Dict[str, Any],
                                   strategy: QueryGenerationStrategy) -> str:
        """Generate intermediate query with multiple joins and subqueries"""
        
        query = ""
        if database == 'sales_db':
            query = """
            SELECT 
                c.company_name,
                c.city,
                c.company_name as company,
                order_stats.order_count,
                order_stats.total_amount,
                order_stats.avg_order_value,
                product_stats.favorite_category
            FROM customers c
            LEFT JOIN (
                SELECT 
                    customer_id,
                    COUNT(*) as order_count,
                    SUM(total_amount) as total_amount,
                    AVG(total_amount) as avg_order_value
                FROM orders 
                WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
                GROUP BY customer_id
            ) order_stats ON c.customer_id = order_stats.customer_id
            LEFT JOIN (
                SELECT 
                    o.customer_id,
                    cat.category_name as favorite_category,
                    ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY SUM(oi.quantity) DESC) as rn
                FROM orders o
                JOIN order_items oi ON o.order_id = oi.order_id
                JOIN products p ON oi.product_id = p.product_id
                JOIN product_categories cat ON p.category_id = cat.category_id
                GROUP BY o.customer_id, cat.category_name
            ) product_stats ON c.customer_id = product_stats.customer_id AND product_stats.rn = 1
            WHERE c.city IN ('Hồ Chí Minh', 'Hà Nội', 'Đà Nẵng')
            ORDER BY order_stats.total_amount DESC NULLS LAST
            LIMIT 25;
            """.strip()
        
        elif database == 'hr_db':
            query = """
            SELECT 
                e.name,
                d.dept_name as department,
                e.position,
                e.hire_date,
                salary_info.current_salary,
                salary_info.salary_growth,
                attendance_info.avg_hours_per_day
            FROM employees e
            JOIN departments d ON e.dept_id = d.dept_id
            LEFT JOIN (
                SELECT employee_id, amount as current_salary, 0.05 as salary_growth
                FROM salaries 
                WHERE payment_date = (SELECT MAX(payment_date) FROM salaries s2 WHERE s2.employee_id = salaries.employee_id)
            ) salary_info ON e.employee_id = salary_info.employee_id
            LEFT JOIN (
                SELECT employee_id, 8.0 as avg_hours_per_day
                FROM attendance 
                WHERE date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                GROUP BY employee_id
            ) attendance_info ON e.employee_id = attendance_info.employee_id
            WHERE e.hire_date >= DATE_SUB(CURDATE(), INTERVAL 2 YEAR)
            ORDER BY salary_info.current_salary DESC;
            """.strip()
        
        else:
            return self._generate_basic_query(intent, database, context, strategy)
            
        return self._randomize_structure(query)
    
    def _generate_advanced_query(self, intent: str, database: str, context: Dict[str, Any],
                               strategy: QueryGenerationStrategy) -> str:
        """Generate advanced query with window functions and complex analytics"""
        
        if database == 'sales_db':
            return """
            WITH customer_metrics AS (
                SELECT 
                    c.customer_id,
                    c.contact_person as name,
                    c.city,
                    c.company_name as company,
                    COUNT(o.order_id) as total_orders,
                    SUM(o.total_amount) as total_revenue,
                    AVG(o.total_amount) as avg_order_value,
                    MAX(o.order_date) as last_order_date,
                    DATEDIFF(CURDATE(), MAX(o.order_date)) as days_since_last_order
                FROM customers c
                LEFT JOIN orders o ON c.customer_id = o.customer_id
                GROUP BY c.customer_id, c.contact_person, c.city, c.company_name
            ),
            customer_segments AS (
                SELECT 
                    *,
                    CASE 
                        WHEN total_revenue >= 50000 AND days_since_last_order <= 30 THEN 'VIP Active'
                        WHEN total_revenue >= 50000 AND days_since_last_order > 30 THEN 'VIP At Risk'
                        WHEN total_revenue >= 10000 AND days_since_last_order <= 60 THEN 'Regular Active'
                        WHEN total_revenue >= 10000 AND days_since_last_order > 60 THEN 'Regular At Risk'
                        WHEN total_revenue > 0 AND days_since_last_order <= 90 THEN 'New Customer'
                        ELSE 'Inactive'
                    END as customer_segment,
                    ROW_NUMBER() OVER (PARTITION BY city ORDER BY total_revenue DESC) as city_rank,
                    PERCENT_RANK() OVER (ORDER BY total_revenue) as revenue_percentile
                FROM customer_metrics
            )
            SELECT 
                name,
                city,
                company,
                customer_segment,
                total_orders,
                total_revenue,
                avg_order_value,
                city_rank,
                ROUND(revenue_percentile * 100, 1) as revenue_percentile,
                CASE 
                    WHEN revenue_percentile >= 0.9 THEN 'Top 10%'
                    WHEN revenue_percentile >= 0.7 THEN 'Top 30%'
                    WHEN revenue_percentile >= 0.5 THEN 'Above Average'
                    ELSE 'Below Average'
                END as performance_tier
            FROM customer_segments
            WHERE customer_segment != 'Inactive'
            ORDER BY total_revenue DESC, city_rank
            LIMIT 50;
            """.strip()
        
        elif database == 'finance_db':
            return """
            WITH monthly_metrics AS (
                SELECT 
                    DATE_FORMAT(issue_date, '%Y-%m') as month,
                    COUNT(*) as invoice_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount,
                    SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) as paid_amount,
                    SUM(CASE WHEN status = 'overdue' THEN amount ELSE 0 END) as overdue_amount
                FROM invoices 
                WHERE issue_date >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)
                GROUP BY DATE_FORMAT(issue_date, '%Y-%m')
            ),
            trend_analysis AS (
                SELECT 
                    *,
                    LAG(total_amount) OVER (ORDER BY month) as prev_month_amount,
                    (total_amount - LAG(total_amount) OVER (ORDER BY month)) / 
                    NULLIF(LAG(total_amount) OVER (ORDER BY month), 0) * 100 as growth_rate,
                    AVG(total_amount) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rolling_avg_3m,
                    SUM(total_amount) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as rolling_sum_12m
                FROM monthly_metrics
            )
            SELECT 
                month,
                invoice_count,
                total_amount,
                paid_amount,
                overdue_amount,
                ROUND((paid_amount / NULLIF(total_amount, 0)) * 100, 1) as collection_rate,
                ROUND(growth_rate, 1) as month_over_month_growth,
                ROUND(rolling_avg_3m, 0) as three_month_avg,
                ROUND(rolling_sum_12m, 0) as twelve_month_total,
                CASE 
                    WHEN growth_rate > 10 THEN 'Strong Growth'
                    WHEN growth_rate > 0 THEN 'Moderate Growth'
                    WHEN growth_rate > -10 THEN 'Slight Decline'
                    ELSE 'Significant Decline'
                END as trend_category
            FROM trend_analysis
            WHERE month >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 12 MONTH), '%Y-%m')
            ORDER BY month DESC;
            """.strip()
        
        else:
            return self._generate_intermediate_query(intent, database, context, strategy)
    
    def _generate_expert_query(self, intent: str, database: str, context: Dict[str, Any],
                             strategy: QueryGenerationStrategy) -> str:
        """Generate expert-level query with CTEs, advanced analytics, and complex business logic"""
        
        if database == 'sales_db':
            return """
            WITH RECURSIVE customer_hierarchy AS (
                -- Base case: direct customers
                SELECT 
                    customer_id,
                    name,
                    company,
                    city,
                    customer_id as root_customer,
                    0 as level,
                    CAST(name AS CHAR(1000)) as path
                FROM customers 
                WHERE company IS NOT NULL
                
                UNION ALL
                
                -- Recursive case: customers from same company
                SELECT 
                    c.customer_id,
                    c.name,
                    c.company,
                    c.city,
                    ch.root_customer,
                    ch.level + 1,
                    CONCAT(ch.path, ' -> ', c.name)
                FROM customers c
                JOIN customer_hierarchy ch ON c.company = (
                    SELECT company FROM customers WHERE customer_id = ch.root_customer
                )
                WHERE ch.level < 3 AND c.customer_id != ch.customer_id
            )
            SELECT name, company, city FROM customer_hierarchy LIMIT 100;
            """.strip()
        
        else:
            return self._generate_advanced_query(intent, database, context, strategy)
    
    def _generate_fallback_query(self, intent: str, database: str) -> str:
        """Generate simple fallback query when complex generation fails"""
        schema_map = {
            'sales_db': 'customers',
            'hr_db': 'employees',
            'finance_db': 'invoices',
            'marketing_db': 'campaigns',
            'support_db': 'support_tickets',
            'inventory_db': 'inventory_levels',
            'admin_db': 'users'
        }
        table = schema_map.get(database, 'customers')
        
        patterns = [
            f"SELECT * FROM {table} LIMIT 10;",
            f"SELECT * FROM {table} LIMIT 20;",
            f"SELECT * FROM {table} LIMIT 5;"
        ]
        return random.choice(patterns)
    
    def add_realistic_joins(self, base_query: str, available_tables: List[str], 
                          max_joins: int = 2) -> str:
        """
        Add realistic business joins to a base query
        
        Args:
            base_query: Base SQL query to enhance
            available_tables: List of available tables for joining
            max_joins: Maximum number of joins to add
            
        Returns:
            Enhanced query with realistic joins
        """
        try:
            # Define realistic join patterns for Vietnamese business context
            join_patterns = {
                'customers': {
                    'orders': 'customers.customer_id = orders.customer_id',
                    'invoices': 'customers.customer_id = invoices.customer_id'
                },
                'orders': {
                    'customers': 'orders.customer_id = customers.customer_id',
                    'order_items': 'orders.order_id = order_items.order_id'
                },
                'order_items': {
                    'orders': 'order_items.order_id = orders.order_id',
                    'products': 'order_items.product_id = products.product_id'
                },
                'employees': {
                    'departments': 'employees.department_id = departments.department_id',
                    'attendance': 'employees.employee_id = attendance.employee_id',
                    'salaries': 'employees.employee_id = salaries.employee_id'
                },
                'products': {
                    'order_items': 'products.product_id = order_items.product_id',
                    'stock_movements': 'products.product_id = stock_movements.product_id'
                }
            }
            
            # Extract main table from base query
            main_table = self._extract_main_table(base_query)
            if not main_table or main_table not in join_patterns:
                return base_query
            
            # Find available joins
            possible_joins = []
            for table, join_condition in join_patterns[main_table].items():
                if table in available_tables:
                    possible_joins.append((table, join_condition))
            
            if not possible_joins:
                return base_query
            
            # Add joins up to max_joins limit
            enhanced_query = base_query
            joins_added = 0
            
            for table, join_condition in possible_joins[:max_joins]:
                if 'JOIN' not in enhanced_query.upper():
                    # First join - add after FROM clause
                    enhanced_query = enhanced_query.replace(
                        f'FROM {main_table}',
                        f'FROM {main_table}\nJOIN {table} ON {join_condition}'
                    )
                else:
                    # Additional joins
                    where_pos = enhanced_query.upper().find('WHERE')
                    if where_pos != -1:
                        enhanced_query = (enhanced_query[:where_pos] + 
                                        f'JOIN {table} ON {join_condition}\n' +
                                        enhanced_query[where_pos:])
                    else:
                        enhanced_query += f'\nJOIN {table} ON {join_condition}'
                
                joins_added += 1
                if joins_added >= max_joins:
                    break
            
            return enhanced_query
            
        except Exception as e:
            self.logger.error(f"Error adding realistic joins: {e}")
            return base_query
    
    def _extract_main_table(self, query: str) -> Optional[str]:
        """Extract main table name from SQL query"""
        try:
            query_upper = query.upper()
            from_pos = query_upper.find('FROM')
            if from_pos == -1:
                return None
            
            # Extract table name after FROM
            from_clause = query[from_pos + 4:].strip()
            table_name = from_clause.split()[0].strip()
            
            # Remove any alias
            if ' ' in table_name:
                table_name = table_name.split()[0]
            
            return table_name.lower()
            
        except Exception:
            return None


if __name__ == "__main__":
    # Example usage and testing
    print("🧠 TESTING QUERY COMPLEXITY ENGINE")
    print("=" * 50)
    
    from .models import UserContext, BusinessContext, ExpertiseLevel, WorkflowType, SensitivityLevel
    
    # Create test contexts
    user_context = UserContext(
        username="test_user",
        role="FINANCE",
        department="Phòng Tài Chính",
        expertise_level=ExpertiseLevel.INTERMEDIATE,
        session_history=[],
        work_intensity=1.2,
        stress_level=0.4
    )
    
    business_context = BusinessContext(
        current_workflow=WorkflowType.FINANCIAL_REPORTING,
        business_event=None,
        department_interactions=["Phòng Tài Chính", "Phòng Kinh Doanh"],
        compliance_requirements=[],
        data_sensitivity_level=SensitivityLevel.CONFIDENTIAL
    )
    
    # Test complexity engine
    engine = QueryComplexityEngine()
    assessment = engine.determine_complexity_level(user_context, business_context)
    
    print(f"📊 Complexity Assessment:")
    print(f"Level: {assessment.complexity_level.name}")
    print(f"Score: {assessment.final_score:.2f}")
    print(f"\nFactors:")
    print(f"• User expertise: {assessment.user_expertise_factor:.2f}")
    print(f"• Business context: {assessment.business_context_factor:.2f}")
    print(f"• Temporal: {assessment.temporal_factor:.2f}")
    print(f"• Cultural: {assessment.cultural_factor:.2f}")
    
    print(f"\n📋 Reasoning:")
    for reason in assessment.reasoning:
        print(f"• {reason}")
    
    strategy = engine.get_generation_strategy(assessment.complexity_level)
    print(f"\n🎯 Generation Strategy:")
    print(f"• Max tables: {strategy.max_tables}")
    print(f"• Max joins: {strategy.max_joins}")
    print(f"• Subqueries: {strategy.allow_subqueries}")
    print(f"• Aggregations: {strategy.allow_aggregations}")
    
    print(f"\n✅ Query Complexity Engine ready for dynamic SQL generation")