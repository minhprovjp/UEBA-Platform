"""
Property-Based Test for SQL Grammar Validation

This test implements Property 7: Grammar validation round trip
"""

import json
from datetime import datetime
from hypothesis import given, strategies as st, settings, HealthCheck
from hypothesis.strategies import composite
import pytest
import re

from dynamic_sql_generation.models import (
    QueryContext, UserContext, BusinessContext, TemporalContext, 
    CulturalContext, DatabaseState, CulturalConstraints, ExpertiseLevel, 
    WorkflowType, BusinessCyclePhase, SensitivityLevel, PerformanceMetrics
)


class TestGrammarValidation:
    """Property-based tests for SQL grammar validation"""
    
    @given(
        st.sampled_from(['SALES', 'MARKETING', 'HR', 'FINANCE', 'DEV', 'ADMIN', 'MANAGEMENT']),
        st.sampled_from(['customer_search', 'order_analysis', 'employee_report', 'financial_summary', 'inventory_check']),
        st.sampled_from(['sales_db', 'hr_db', 'finance_db', 'marketing_db', 'support_db', 'inventory_db', 'admin_db']),
        st.integers(min_value=42, max_value=999)  # seed for reproducibility
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_grammar_validation_round_trip(self, user_role, action, target_database, seed):
        """
        **Feature: dynamic-sql-generation, Property 7: Grammar validation round trip**
        **Validates: Requirements 1.5**
        
        For any generated SQL query, parsing then regenerating should produce an equivalent query structure
        """
        from dynamic_sql_generation.generator import DynamicSQLGenerator
        
        # Create generator with seed for reproducibility
        generator = DynamicSQLGenerator(seed=seed)
        
        # Create test intent
        intent = {
            'action': action,
            'username': f'test_user_{seed}',
            'role': user_role,
            'target_database': target_database,
            'department': f'Phòng {user_role}'
        }
        
        # Create minimal context for generation
        cultural_constraints = CulturalConstraints(
            hierarchy_level=5,
            respect_seniority=True,
            work_overtime_acceptable=True,
            tet_preparation_mode=False
        )
        
        cultural_context = CulturalContext(
            cultural_constraints=cultural_constraints,
            vietnamese_holidays=[],
            business_etiquette={'hierarchy_respect': 'medium'},
            language_preferences={'primary': 'vietnamese'}
        )
        
        temporal_context = TemporalContext(
            current_hour=10,
            is_work_hours=True,
            is_lunch_break=False,
            is_vietnamese_holiday=False,
            business_cycle_phase=BusinessCyclePhase.LOW_SEASON,
            seasonal_factor=1.0
        )
        
        business_context = BusinessContext(
            current_workflow=WorkflowType.SALES_PROCESS,
            business_event=None,
            department_interactions=[],
            compliance_requirements=[],
            data_sensitivity_level=SensitivityLevel.INTERNAL
        )
        
        user_context = UserContext(
            username=intent['username'],
            role=user_role,
            department=intent['department'],
            expertise_level=ExpertiseLevel.INTERMEDIATE,
            session_history=[],
            work_intensity=1.0,
            stress_level=0.5
        )
        
        database_state = DatabaseState(
            entity_counts={'customers': 100, 'orders': 500, 'employees': 50},
            relationship_map={},
            constraint_violations=[],
            recent_modifications=[],
            performance_metrics=PerformanceMetrics(
                avg_query_time=0.5,
                slow_query_count=0,
                connection_count=10,
                cache_hit_ratio=0.8
            )
        )
        
        context = QueryContext(
            user_context=user_context,
            database_state=database_state,
            business_context=business_context,
            temporal_context=temporal_context,
            cultural_context=cultural_context
        )
        
        # Generate SQL query
        result = generator.generate_query(intent, context)
        
        # Verify query was generated successfully
        assert result is not None
        assert isinstance(result.query, str)
        assert len(result.query.strip()) > 0
        
        original_query = result.query.strip()
        
        # Parse the SQL query to extract its structure
        parsed_structure = self._parse_sql_structure(original_query)
        
        # Debug output for failing cases
        print(f"\nOriginal query: {original_query}")
        print(f"Parsed structure: {parsed_structure}")
        
        # Verify parsing succeeded
        assert parsed_structure is not None
        assert 'select_clause' in parsed_structure
        assert 'from_clause' in parsed_structure
        
        # Regenerate SQL from parsed structure
        regenerated_query = self._regenerate_sql_from_structure(parsed_structure)
        
        # Verify regeneration succeeded
        assert isinstance(regenerated_query, str)
        assert len(regenerated_query.strip()) > 0
        
        # Parse the regenerated query to verify equivalence
        regenerated_structure = self._parse_sql_structure(regenerated_query)
        
        # Verify structural equivalence
        assert regenerated_structure is not None
        
        # Core structural elements should be equivalent
        assert parsed_structure['select_clause'] == regenerated_structure['select_clause']
        assert parsed_structure['from_clause'] == regenerated_structure['from_clause']
        
        # WHERE clauses should be equivalent (if present)
        if 'where_clause' in parsed_structure:
            assert 'where_clause' in regenerated_structure
            # Allow for minor formatting differences in WHERE clauses
            original_where = self._normalize_where_clause(parsed_structure['where_clause'])
            regenerated_where = self._normalize_where_clause(regenerated_structure['where_clause'])
            assert original_where == regenerated_where
        
        # JOIN clauses should be equivalent (if present)
        if 'join_clauses' in parsed_structure:
            assert 'join_clauses' in regenerated_structure
            assert len(parsed_structure['join_clauses']) == len(regenerated_structure['join_clauses'])
            for orig_join, regen_join in zip(parsed_structure['join_clauses'], regenerated_structure['join_clauses']):
                assert self._normalize_join_clause(orig_join) == self._normalize_join_clause(regen_join)
        
        # ORDER BY clauses should be equivalent (if present)
        if 'order_by_clause' in parsed_structure:
            assert 'order_by_clause' in regenerated_structure
            assert self._normalize_order_by(parsed_structure['order_by_clause']) == \
                   self._normalize_order_by(regenerated_structure['order_by_clause'])
        
        # LIMIT clauses should be equivalent (if present)
        if 'limit_clause' in parsed_structure:
            assert 'limit_clause' in regenerated_structure
            assert parsed_structure['limit_clause'] == regenerated_structure['limit_clause']
        
        # Verify both queries are syntactically valid
        assert generator._validate_generated_query(original_query)
        assert generator._validate_generated_query(regenerated_query)
        
        # Verify both queries have the same basic structure
        original_upper = original_query.upper()
        regenerated_upper = regenerated_query.upper()
        
        # Both should have SELECT and FROM
        assert 'SELECT' in original_upper
        assert 'FROM' in original_upper
        assert 'SELECT' in regenerated_upper
        assert 'FROM' in regenerated_upper
        
        # Both should end with semicolon
        assert original_query.strip().endswith(';')
        assert regenerated_query.strip().endswith(';')
        
        # Verify parentheses balance in both
        assert original_upper.count('(') == original_upper.count(')')
        assert regenerated_upper.count('(') == regenerated_upper.count(')')
    
    def _parse_sql_structure(self, query: str) -> dict:
        """Parse SQL query into structural components"""
        query = query.strip().rstrip(';')
        query_upper = query.upper()
        
        structure = {}
        
        # Extract SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE | re.DOTALL)
        if select_match:
            structure['select_clause'] = select_match.group(1).strip()
        
        # Extract FROM clause (including table name and aliases)
        from_match = re.search(r'FROM\s+([^\s]+(?:\s+AS\s+[^\s]+)?)', query, re.IGNORECASE)
        if from_match:
            structure['from_clause'] = from_match.group(1).strip()
        
        # Extract JOIN clauses
        join_matches = re.findall(r'((?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s+[^;]+?(?=\s+(?:WHERE|ORDER|GROUP|LIMIT|;|$)))', 
                                query, re.IGNORECASE | re.DOTALL)
        if join_matches:
            structure['join_clauses'] = [join.strip() for join in join_matches]
        
        # Extract WHERE clause
        where_match = re.search(r'WHERE\s+(.*?)(?:\s+(?:GROUP|ORDER|LIMIT|;|$))', query, re.IGNORECASE | re.DOTALL)
        if where_match:
            structure['where_clause'] = where_match.group(1).strip()
        
        # Extract GROUP BY clause
        group_match = re.search(r'GROUP\s+BY\s+(.*?)(?:\s+(?:HAVING|ORDER|LIMIT|;|$))', query, re.IGNORECASE | re.DOTALL)
        if group_match:
            structure['group_by_clause'] = group_match.group(1).strip()
        
        # Extract HAVING clause
        having_match = re.search(r'HAVING\s+(.*?)(?:\s+(?:ORDER|LIMIT|;|$))', query, re.IGNORECASE | re.DOTALL)
        if having_match:
            structure['having_clause'] = having_match.group(1).strip()
        
        # Extract ORDER BY clause
        order_match = re.search(r'ORDER\s+BY\s+(.*?)(?:\s+(?:LIMIT|;|$))', query, re.IGNORECASE | re.DOTALL)
        if order_match:
            structure['order_by_clause'] = order_match.group(1).strip()
        
        # Extract LIMIT clause
        limit_match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
        if limit_match:
            structure['limit_clause'] = limit_match.group(1)
        
        return structure
    
    def _regenerate_sql_from_structure(self, structure: dict) -> str:
        """Regenerate SQL query from parsed structure"""
        parts = []
        
        # SELECT clause
        if 'select_clause' in structure:
            parts.append(f"SELECT {structure['select_clause']}")
        
        # FROM clause
        if 'from_clause' in structure:
            parts.append(f"FROM {structure['from_clause']}")
        
        # JOIN clauses
        if 'join_clauses' in structure:
            for join_clause in structure['join_clauses']:
                parts.append(join_clause)
        
        # WHERE clause
        if 'where_clause' in structure:
            parts.append(f"WHERE {structure['where_clause']}")
        
        # GROUP BY clause
        if 'group_by_clause' in structure:
            parts.append(f"GROUP BY {structure['group_by_clause']}")
        
        # HAVING clause
        if 'having_clause' in structure:
            parts.append(f"HAVING {structure['having_clause']}")
        
        # ORDER BY clause
        if 'order_by_clause' in structure:
            parts.append(f"ORDER BY {structure['order_by_clause']}")
        
        # LIMIT clause
        if 'limit_clause' in structure:
            parts.append(f"LIMIT {structure['limit_clause']}")
        
        return ' '.join(parts) + ';'
    
    def _normalize_where_clause(self, where_clause: str) -> str:
        """Normalize WHERE clause for comparison"""
        # Remove extra whitespace and normalize operators
        normalized = re.sub(r'\s+', ' ', where_clause.strip())
        normalized = re.sub(r'\s*=\s*', ' = ', normalized)
        normalized = re.sub(r'\s*>\s*', ' > ', normalized)
        normalized = re.sub(r'\s*<\s*', ' < ', normalized)
        normalized = re.sub(r'\s*>=\s*', ' >= ', normalized)
        normalized = re.sub(r'\s*<=\s*', ' <= ', normalized)
        normalized = re.sub(r'\s*!=\s*', ' != ', normalized)
        normalized = re.sub(r'\s*<>\s*', ' <> ', normalized)
        return normalized.upper()
    
    def _normalize_join_clause(self, join_clause: str) -> str:
        """Normalize JOIN clause for comparison"""
        # Remove extra whitespace and normalize JOIN keywords
        normalized = re.sub(r'\s+', ' ', join_clause.strip())
        return normalized.upper()
    
    def _normalize_order_by(self, order_clause: str) -> str:
        """Normalize ORDER BY clause for comparison"""
        # Remove extra whitespace and normalize ASC/DESC
        normalized = re.sub(r'\s+', ' ', order_clause.strip())
        return normalized.upper()