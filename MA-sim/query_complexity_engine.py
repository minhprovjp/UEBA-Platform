"""
Query Complexity Engine for Vietnamese Business Context

This module provides sophisticated query complexity adaptation based on user expertise,
business context, and Vietnamese cultural patterns.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import random
from vietnamese_business_patterns import ExpertiseLevel, VietnameseBusinessPatterns

class ComplexityLevel(Enum):
    SIMPLE = 1
    BASIC = 2
    INTERMEDIATE = 3
    ADVANCED = 4
    EXPERT = 5

@dataclass
class ComplexityAssessment:
    """Assessment of query complexity requirements"""
    complexity_level: ComplexityLevel
    user_expertise_factor: float
    business_context_factor: float
    temporal_factor: float
    cultural_factor: float
    final_score: float
    reasoning: List[str]

@dataclass
class GenerationStrategy:
    """Strategy for generating queries at specific complexity level"""
    max_tables: int
    max_joins: int
    allow_subqueries: bool
    allow_aggregations: bool
    allow_window_functions: bool
    allow_ctes: bool
    max_where_conditions: int
    preferred_patterns: List[str]

class QueryComplexityEngine:
    """Engine for determining and adapting query complexity"""
    
    def __init__(self):
        self.patterns = VietnameseBusinessPatterns()
        self.complexity_strategies = self._initialize_strategies()
        self.user_success_rates = {}  # Track user success rates for adaptation
    
    def _initialize_strategies(self) -> Dict[ComplexityLevel, GenerationStrategy]:
        """Initialize generation strategies for each complexity level"""
        return {
            ComplexityLevel.SIMPLE: GenerationStrategy(
                max_tables=1,
                max_joins=0,
                allow_subqueries=False,
                allow_aggregations=False,
                allow_window_functions=False,
                allow_ctes=False,
                max_where_conditions=2,
                preferred_patterns=['simple_select', 'basic_filter']
            ),
            ComplexityLevel.BASIC: GenerationStrategy(
                max_tables=2,
                max_joins=1,
                allow_subqueries=False,
                allow_aggregations=True,
                allow_window_functions=False,
                allow_ctes=False,
                max_where_conditions=3,
                preferred_patterns=['inner_join', 'count_group_by', 'sum_aggregation']
            ),
            ComplexityLevel.INTERMEDIATE: GenerationStrategy(
                max_tables=3,
                max_joins=2,
                allow_subqueries=True,
                allow_aggregations=True,
                allow_window_functions=False,
                allow_ctes=False,
                max_where_conditions=4,
                preferred_patterns=['multiple_joins', 'subquery_filter', 'having_clause']
            ),
            ComplexityLevel.ADVANCED: GenerationStrategy(
                max_tables=4,
                max_joins=3,
                allow_subqueries=True,
                allow_aggregations=True,
                allow_window_functions=True,
                allow_ctes=False,
                max_where_conditions=5,
                preferred_patterns=['window_functions', 'complex_subqueries', 'case_statements']
            ),
            ComplexityLevel.EXPERT: GenerationStrategy(
                max_tables=5,
                max_joins=4,
                allow_subqueries=True,
                allow_aggregations=True,
                allow_window_functions=True,
                allow_ctes=True,
                max_where_conditions=6,
                preferred_patterns=['cte_queries', 'recursive_queries', 'advanced_analytics']
            )
        }
    
    def determine_complexity_level(self, user_context: Dict, 
                                 business_context: Dict) -> ComplexityAssessment:
        """Determine appropriate complexity level based on context"""
        reasoning = []
        
        # 1. User expertise factor (1.0 - 5.0)
        expertise_level = user_context.get('expertise_level', ExpertiseLevel.INTERMEDIATE)
        expertise_mapping = {
            ExpertiseLevel.NOVICE: 1.0,
            ExpertiseLevel.INTERMEDIATE: 2.5,
            ExpertiseLevel.ADVANCED: 4.0,
            ExpertiseLevel.EXPERT: 5.0
        }
        user_expertise_factor = expertise_mapping[expertise_level]
        
        # Adjust for stress level
        stress_level = user_context.get('stress_level', 0.5)
        if stress_level > 0.8:
            user_expertise_factor *= 0.8  # High stress reduces effective expertise
            reasoning.append(f"High stress level ({stress_level}) reduces effective expertise")
        elif stress_level < 0.3:
            user_expertise_factor *= 1.1  # Low stress allows for higher complexity
            reasoning.append(f"Low stress level ({stress_level}) allows higher complexity")
        
        reasoning.append(f"Base expertise: {expertise_level.value} -> factor: {user_expertise_factor}")
        
        # 2. Business context factor (1.0 - 5.0)
        workflow_type = business_context.get('current_workflow', 'ADMINISTRATIVE')
        business_event = business_context.get('business_event')
        sensitivity_level = business_context.get('data_sensitivity_level', 'INTERNAL')
        
        business_context_factor = 2.0  # Base level
        
        # Adjust for workflow type
        workflow_complexity = {
            'FINANCIAL_REPORTING': 4.0,
            'SALES_ANALYTICS': 3.5,
            'HR_ANALYTICS': 3.0,
            'MARKETING_ANALYSIS': 3.0,
            'ADMINISTRATIVE': 2.0,
            'CUSTOMER_SERVICE': 2.5
        }
        business_context_factor = workflow_complexity.get(workflow_type, 2.0)
        reasoning.append(f"Workflow {workflow_type} requires complexity factor: {business_context_factor}")
        
        # Adjust for business events
        if business_event:
            if business_event in ['QUARTER_END', 'YEAR_END', 'AUDIT']:
                business_context_factor += 1.0
                reasoning.append(f"Business event {business_event} increases complexity")
            elif business_event in ['HOLIDAY', 'TET_SEASON']:
                business_context_factor -= 0.5
                reasoning.append(f"Business event {business_event} reduces complexity")
        
        # Adjust for data sensitivity
        sensitivity_adjustment = {
            'PUBLIC': 0.0,
            'INTERNAL': 0.0,
            'CONFIDENTIAL': 0.5,
            'RESTRICTED': 1.0
        }
        business_context_factor += sensitivity_adjustment.get(sensitivity_level, 0.0)
        reasoning.append(f"Data sensitivity {sensitivity_level} adds complexity")
        
        # 3. Temporal factor (0.5 - 2.0)
        temporal_context = business_context.get('temporal_context', {})
        current_hour = temporal_context.get('current_hour', 12)
        is_holiday = temporal_context.get('is_holiday', False)
        is_peak_hours = temporal_context.get('is_peak_hours', False)
        activity_level = temporal_context.get('activity_level', 1.0)
        
        temporal_factor = 1.0  # Base
        
        if is_holiday:
            temporal_factor = 0.5  # Reduced complexity during holidays
            reasoning.append("Holiday period reduces complexity requirements")
        elif is_peak_hours:
            temporal_factor = 1.2  # Slightly higher complexity during peak hours
            reasoning.append("Peak hours allow for higher complexity")
        elif current_hour < 8 or current_hour > 17:
            temporal_factor = 0.8  # Reduced complexity outside work hours
            reasoning.append("Outside work hours reduces complexity")
        
        # Adjust for activity level
        temporal_factor *= activity_level
        reasoning.append(f"Activity level {activity_level} adjusts temporal factor to {temporal_factor}")
        
        # 4. Cultural factor (0.5 - 2.0)
        cultural_constraints = business_context.get('cultural_constraints', {})
        hierarchy_level = cultural_constraints.get('hierarchy_level', 5)
        respect_seniority = cultural_constraints.get('respect_seniority', True)
        cultural_sensitivity = cultural_constraints.get('cultural_sensitivity_level', 'normal')
        
        cultural_factor = 1.0  # Base
        
        # Hierarchy adjustment
        if hierarchy_level >= 8:  # Senior management
            cultural_factor = 1.5
            reasoning.append(f"Senior hierarchy level {hierarchy_level} increases complexity")
        elif hierarchy_level <= 3:  # Junior staff
            cultural_factor = 0.7
            reasoning.append(f"Junior hierarchy level {hierarchy_level} reduces complexity")
        
        # Cultural sensitivity adjustment
        if cultural_sensitivity == 'high':
            cultural_factor *= 0.9  # Slightly more conservative
            reasoning.append("High cultural sensitivity reduces complexity slightly")
        
        # 5. Calculate final score
        final_score = (user_expertise_factor * 0.4 + 
                      business_context_factor * 0.3 + 
                      temporal_factor * 0.2 + 
                      cultural_factor * 0.1)
        
        reasoning.append(f"Final score calculation: {user_expertise_factor}*0.4 + {business_context_factor}*0.3 + {temporal_factor}*0.2 + {cultural_factor}*0.1 = {final_score}")
        
        # 6. Map to complexity level
        if final_score <= 1.5:
            complexity_level = ComplexityLevel.SIMPLE
        elif final_score <= 2.5:
            complexity_level = ComplexityLevel.BASIC
        elif final_score <= 3.5:
            complexity_level = ComplexityLevel.INTERMEDIATE
        elif final_score <= 4.5:
            complexity_level = ComplexityLevel.ADVANCED
        else:
            complexity_level = ComplexityLevel.EXPERT
        
        reasoning.append(f"Final complexity level: {complexity_level.name}")
        
        return ComplexityAssessment(
            complexity_level=complexity_level,
            user_expertise_factor=user_expertise_factor,
            business_context_factor=business_context_factor,
            temporal_factor=temporal_factor,
            cultural_factor=cultural_factor,
            final_score=final_score,
            reasoning=reasoning
        )
    
    def get_generation_strategy(self, complexity_level: ComplexityLevel) -> GenerationStrategy:
        """Get generation strategy for complexity level"""
        return self.complexity_strategies[complexity_level]
    
    def generate_complex_query(self, intent: str, complexity_level: ComplexityLevel, 
                             context: Dict) -> str:
        """Generate query matching complexity level"""
        strategy = self.get_generation_strategy(complexity_level)
        database = context.get('target_database', 'sales_db')
        
        # Base query templates by complexity
        if complexity_level == ComplexityLevel.SIMPLE:
            return self._generate_simple_query(intent, database, context)
        elif complexity_level == ComplexityLevel.BASIC:
            return self._generate_basic_query(intent, database, context, strategy)
        elif complexity_level == ComplexityLevel.INTERMEDIATE:
            return self._generate_intermediate_query(intent, database, context, strategy)
        elif complexity_level == ComplexityLevel.ADVANCED:
            return self._generate_advanced_query(intent, database, context, strategy)
        else:  # EXPERT
            return self._generate_expert_query(intent, database, context, strategy)
    
    def _generate_simple_query(self, intent: str, database: str, context: Dict) -> str:
        """Generate simple single-table query"""
        table_mapping = {
            'sales_db': 'customers',
            'hr_db': 'employees',
            'finance_db': 'invoices',
            'marketing_db': 'leads',
            'support_db': 'support_tickets',
            'inventory_db': 'inventory_levels',
            'admin_db': 'user_sessions'
        }
        
        table = table_mapping.get(database, 'customers')
        
        if 'search' in intent.lower():
            return f"SELECT * FROM {table} WHERE status = '{{status}}' LIMIT {{limit}};"
        elif 'count' in intent.lower():
            return f"SELECT COUNT(*) FROM {table} WHERE status = 'active';"
        else:
            return f"SELECT * FROM {table} LIMIT 10;"
    
    def _generate_basic_query(self, intent: str, database: str, 
                            context: Dict, strategy: GenerationStrategy) -> str:
        """Generate basic query with simple joins and aggregations"""
        if database == 'sales_db':
            if 'customer' in intent.lower():
                return """SELECT c.customer_code, c.company_name, COUNT(o.order_id) as order_count
                         FROM customers c 
                         LEFT JOIN orders o ON c.customer_id = o.customer_id 
                         WHERE c.status = 'active' 
                         GROUP BY c.customer_id, c.customer_code, c.company_name 
                         LIMIT {limit};"""
            else:
                return """SELECT p.product_name, SUM(oi.quantity) as total_sold
                         FROM products p 
                         JOIN order_items oi ON p.product_id = oi.product_id 
                         GROUP BY p.product_id, p.product_name 
                         ORDER BY total_sold DESC 
                         LIMIT {limit};"""
        
        # Fallback to simple query
        return self._generate_simple_query(intent, database, context)
    
    def _generate_intermediate_query(self, intent: str, database: str, 
                                   context: Dict, strategy: GenerationStrategy) -> str:
        """Generate intermediate query with subqueries"""
        if database == 'sales_db':
            return """SELECT c.company_name, c.city,
                            (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) as order_count,
                            (SELECT SUM(o.total_amount) FROM orders o WHERE o.customer_id = c.customer_id) as total_revenue
                     FROM customers c 
                     WHERE c.customer_id IN (
                         SELECT DISTINCT customer_id FROM orders 
                         WHERE order_date >= '{start_date}' AND order_date <= '{end_date}'
                     )
                     ORDER BY total_revenue DESC 
                     LIMIT {limit};"""
        
        return self._generate_basic_query(intent, database, context, strategy)
    
    def _generate_advanced_query(self, intent: str, database: str, 
                               context: Dict, strategy: GenerationStrategy) -> str:
        """Generate advanced query with window functions"""
        if database == 'sales_db':
            return """SELECT c.company_name, c.city, o.order_date, o.total_amount,
                            ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY o.order_date DESC) as order_rank,
                            SUM(o.total_amount) OVER (PARTITION BY c.customer_id) as customer_total,
                            AVG(o.total_amount) OVER (PARTITION BY c.city) as city_avg
                     FROM customers c 
                     JOIN orders o ON c.customer_id = o.customer_id 
                     WHERE o.order_date >= '{start_date}' 
                     AND c.status = 'active'
                     QUALIFY order_rank <= 3
                     ORDER BY c.company_name, o.order_date DESC;"""
        
        return self._generate_intermediate_query(intent, database, context, strategy)
    
    def _generate_expert_query(self, intent: str, database: str, 
                             context: Dict, strategy: GenerationStrategy) -> str:
        """Generate expert query with CTEs and advanced analytics"""
        if database == 'sales_db':
            return """WITH customer_metrics AS (
                         SELECT c.customer_id, c.company_name, c.city,
                                COUNT(o.order_id) as order_count,
                                SUM(o.total_amount) as total_revenue,
                                AVG(o.total_amount) as avg_order_value,
                                MAX(o.order_date) as last_order_date
                         FROM customers c 
                         LEFT JOIN orders o ON c.customer_id = o.customer_id 
                         WHERE c.status = 'active'
                         GROUP BY c.customer_id, c.company_name, c.city
                     ),
                     city_rankings AS (
                         SELECT city,
                                SUM(total_revenue) as city_revenue,
                                RANK() OVER (ORDER BY SUM(total_revenue) DESC) as city_rank
                         FROM customer_metrics 
                         GROUP BY city
                     )
                     SELECT cm.company_name, cm.city, cm.total_revenue,
                            cm.order_count, cm.avg_order_value,
                            cr.city_rank,
                            CASE 
                                WHEN cm.total_revenue > 100000 THEN 'Premium'
                                WHEN cm.total_revenue > 50000 THEN 'Gold'
                                WHEN cm.total_revenue > 10000 THEN 'Silver'
                                ELSE 'Bronze'
                            END as customer_tier
                     FROM customer_metrics cm 
                     JOIN city_rankings cr ON cm.city = cr.city 
                     WHERE cm.order_count > 0
                     ORDER BY cm.total_revenue DESC 
                     LIMIT {limit};"""
        
        return self._generate_advanced_query(intent, database, context, strategy)
    
    def add_realistic_joins(self, base_query: str, available_tables: List[str], 
                          max_joins: int) -> str:
        """Add realistic joins to base query"""
        if max_joins == 0 or not available_tables:
            return base_query
        
        # Simple join addition logic
        enhanced_query = base_query
        
        # Add common business joins
        join_patterns = {
            'customers': 'LEFT JOIN orders ON customers.customer_id = orders.customer_id',
            'orders': 'LEFT JOIN order_items ON orders.order_id = order_items.order_id',
            'employees': 'LEFT JOIN departments ON employees.dept_id = departments.dept_id',
            'products': 'LEFT JOIN product_categories ON products.category_id = product_categories.category_id'
        }
        
        joins_added = 0
        for table in available_tables:
            if joins_added >= max_joins:
                break
            if table in join_patterns and table in base_query:
                join_clause = join_patterns[table]
                if join_clause not in enhanced_query:
                    # Insert join after FROM clause
                    from_pos = enhanced_query.upper().find('FROM')
                    if from_pos != -1:
                        # Find end of FROM table
                        from_end = enhanced_query.find(' ', from_pos + 5)
                        if from_end != -1:
                            enhanced_query = (enhanced_query[:from_end] + 
                                            f' {join_clause}' + 
                                            enhanced_query[from_end:])
                            joins_added += 1
        
        return enhanced_query
    
    def adjust_complexity_based_on_success_rate(self, user_context: Dict, 
                                               current_complexity: ComplexityLevel, 
                                               success_rate: float) -> ComplexityLevel:
        """Adjust complexity based on user success rate"""
        user_id = user_context.get('username', 'unknown')
        
        # Store success rate
        if user_id not in self.user_success_rates:
            self.user_success_rates[user_id] = []
        
        self.user_success_rates[user_id].append(success_rate)
        
        # Keep only recent success rates
        if len(self.user_success_rates[user_id]) > 10:
            self.user_success_rates[user_id] = self.user_success_rates[user_id][-10:]
        
        # Calculate average success rate
        avg_success_rate = sum(self.user_success_rates[user_id]) / len(self.user_success_rates[user_id])
        
        # Adjust complexity
        if avg_success_rate > 0.9 and current_complexity != ComplexityLevel.EXPERT:
            # High success rate - increase complexity
            new_level_value = min(5, current_complexity.value + 1)
            return ComplexityLevel(new_level_value)
        elif avg_success_rate < 0.6 and current_complexity != ComplexityLevel.SIMPLE:
            # Low success rate - decrease complexity
            new_level_value = max(1, current_complexity.value - 1)
            return ComplexityLevel(new_level_value)
        
        return current_complexity

# Global instance
complexity_engine = QueryComplexityEngine()