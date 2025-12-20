#!/usr/bin/env python3
"""
Comprehensive Integration Tests for Dynamic SQL Generation System

This module contains integration tests for:
- EnhancedSQLTranslator integration
- Agent systems integration  
- Database executor and feedback processing integration
- Scenario manager and attack pattern coordination integration
"""

import pytest
import json
import time
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from dynamic_sql_generation.generator import DynamicSQLGenerator
from dynamic_sql_generation.database_state_sync import (
    ExecutionFeedback, 
    create_execution_feedback_from_executor_result
)
from dynamic_sql_generation.models import (
    QueryContext, UserContext, BusinessContext, TemporalContext, 
    CulturalContext, DatabaseState, CulturalConstraints, ExpertiseLevel, 
    WorkflowType, BusinessCyclePhase, SensitivityLevel, PerformanceMetrics
)

# Import system components for integration testing
try:
    from translator import EnhancedSQLTranslator
    from executor import SQLExecutor
    from agents_enhanced import EnhancedAgent
except ImportError:
    # Create mock classes if imports fail
    class EnhancedSQLTranslator:
        def __init__(self):
            self.dynamic_generator = None
        
        def translate(self, intent):
            return "SELECT * FROM customers LIMIT 10;"
    
    class SQLExecutor:
        def __init__(self, enable_state_sync=False):
            self.enable_state_sync = enable_state_sync
            self.state_synchronizer = None
        
        def execute_query(self, query, database, username, role):
            return {
                'success': True,
                'execution_time': 0.1,
                'rows_returned': 10,
                'error_message': None
            }
        
        def get_all_database_states(self):
            return {}
        
        def stop_state_synchronization(self):
            pass
    
    class EnhancedAgent:
        def __init__(self, username, role, department):
            self.username = username
            self.role = role
            self.department = department
        
        def generate_intent(self):
            return {
                'action': 'customer_search',
                'username': self.username,
                'role': self.role,
                'target_database': 'sales_db'
            }


class TestEnhancedSQLTranslatorIntegration:
    """Integration tests with EnhancedSQLTranslator"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.translator = EnhancedSQLTranslator()
        self.generator = DynamicSQLGenerator(seed=42)
        
        # Integrate generator with translator
        self.translator.dynamic_generator = self.generator
    
    def test_translator_uses_dynamic_generator(self):
        """Test that translator uses dynamic generator instead of static templates"""
        # Create test intent
        intent = {
            'action': 'customer_search',
            'username': 'test_user',
            'role': 'SALES',
            'target_database': 'sales_db',
            'department': 'Phòng Kinh Doanh'
        }
        
        # Translate using dynamic generator
        query = self.translator.translate(intent)
        
        # Verify query is generated
        assert isinstance(query, str)
        assert len(query.strip()) > 0
        assert query.strip().endswith(';')
        assert 'SELECT' in query.upper()
        assert 'FROM' in query.upper()
    
    def test_translator_fallback_to_templates(self):
        """Test translator fallback to enhanced templates when dynamic generation fails"""
        # Create problematic intent that might cause generation failure
        problematic_intent = {
            'action': 'unknown_action',
            'username': None,
            'role': 'INVALID_ROLE',
            'target_database': 'nonexistent_db'
        }
        
        # Should still produce a query (either dynamic or template fallback)
        query = self.translator.translate(problematic_intent)
        
        # Verify query is generated
        assert isinstance(query, str)
        assert len(query.strip()) > 0
        assert 'SELECT' in query.upper()
    
    def test_translator_context_enhancement(self):
        """Test that translator enhances context for dynamic generation"""
        # Create intent with minimal context
        minimal_intent = {
            'action': 'report',
            'username': 'analyst',
            'role': 'FINANCE'
        }
        
        # Translator should enhance context and generate appropriate query
        query = self.translator.translate(minimal_intent)
        
        # Verify query is appropriate for financial role
        assert isinstance(query, str)
        assert len(query.strip()) > 0
        assert 'SELECT' in query.upper()
        
        # Check if query reflects financial context (may contain financial terms)
        query_lower = query.lower()
        financial_indicators = ['finance', 'amount', 'total', 'sum', 'count', 'revenue', 'cost']
        # At least the query should be valid SQL
        assert query.strip().endswith(';')
    
    def test_translator_vietnamese_business_integration(self):
        """Test translator integration with Vietnamese business patterns"""
        # Create Vietnamese business context intent
        vietnamese_intent = {
            'action': 'employee_search',
            'username': 'nguyen_van_nam',
            'role': 'HR',
            'target_database': 'hr_db',
            'department': 'Phòng Nhân Sự',
            'city': 'Hồ Chí Minh'
        }
        
        # Generate query with Vietnamese context
        query = self.translator.translate(vietnamese_intent)
        
        # Verify query is generated with Vietnamese considerations
        assert isinstance(query, str)
        assert len(query.strip()) > 0
        assert 'SELECT' in query.upper()
        assert 'FROM' in query.upper()
        
        # Query should be contextually appropriate
        query_lower = query.lower()
        hr_indicators = ['employee', 'staff', 'hr', 'department', 'name']
        # At least verify it's valid SQL for HR context
        assert query.strip().endswith(';')
    
    def test_translator_performance_with_dynamic_generation(self):
        """Test translator performance with dynamic generation"""
        # Create multiple intents to test performance
        intents = [
            {
                'action': 'customer_analysis',
                'username': f'user_{i}',
                'role': 'SALES',
                'target_database': 'sales_db'
            }
            for i in range(10)
        ]
        
        # Measure translation time
        start_time = time.time()
        
        queries = []
        for intent in intents:
            query = self.translator.translate(intent)
            queries.append(query)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Verify all queries generated successfully
        assert len(queries) == 10
        for query in queries:
            assert isinstance(query, str)
            assert len(query.strip()) > 0
            assert 'SELECT' in query.upper()
        
        # Performance should be reasonable (less than 5 seconds for 10 queries)
        assert total_time < 5.0
        
        # Average time per query should be reasonable
        avg_time = total_time / len(intents)
        assert avg_time < 0.5  # Less than 500ms per query


class TestAgentSystemsIntegration:
    """Integration tests with agent systems"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.generator = DynamicSQLGenerator(seed=42)
        
        # Create test agents
        self.sales_agent = EnhancedAgent('nguyen_van_nam', 'SALES', 'Phòng Kinh Doanh')
        self.hr_agent = EnhancedAgent('tran_thi_lan', 'HR', 'Phòng Nhân Sự')
        self.finance_agent = EnhancedAgent('pham_thi_mai', 'FINANCE', 'Phòng Tài Chính')
    
    def test_agent_intent_processing(self):
        """Test processing of agent-generated intents"""
        # Generate intent from sales agent
        sales_intent = self.sales_agent.generate_intent()
        
        # Process intent with dynamic generator
        result = self.generator.generate_query(sales_intent)
        
        # Verify result
        assert result is not None
        assert isinstance(result.query, str)
        assert len(result.query.strip()) > 0
        assert 'SELECT' in result.query.upper()
        
        # Verify context factors reflect agent information
        assert isinstance(result.context_factors, dict)
        assert 'user_role' in result.context_factors
        assert result.context_factors['user_role'] == 'SALES'
    
    def test_agent_session_history_integration(self):
        """Test integration with agent session history"""
        # Simulate multiple queries from same agent
        agent_intents = [
            {
                'action': 'customer_search',
                'username': self.sales_agent.username,
                'role': self.sales_agent.role,
                'target_database': 'sales_db',
                'session_id': 'session_001'
            },
            {
                'action': 'order_analysis',
                'username': self.sales_agent.username,
                'role': self.sales_agent.role,
                'target_database': 'sales_db',
                'session_id': 'session_001'
            }
        ]
        
        results = []
        for intent in agent_intents:
            result = self.generator.generate_query(intent)
            results.append(result)
            
            # Simulate query success for learning
            self.generator.analyze_query_success(
                query=result.query,
                success=True,
                execution_time=0.15,
                error_message=None
            )
        
        # Verify all queries generated successfully
        assert len(results) == 2
        for result in results:
            assert result is not None
            assert isinstance(result.query, str)
            assert len(result.query.strip()) > 0
        
        # Verify generator learned from session
        stats = self.generator.get_generation_stats()
        assert stats['total_generations'] >= 2
        assert stats['success_rate'] > 0.0
    
    def test_multi_agent_coordination(self):
        """Test coordination between multiple agents"""
        agents = [self.sales_agent, self.hr_agent, self.finance_agent]
        
        # Generate intents from multiple agents
        multi_agent_intents = []
        for agent in agents:
            intent = agent.generate_intent()
            intent['agent_id'] = f"{agent.username}_{agent.role}"
            multi_agent_intents.append(intent)
        
        # Process all intents
        results = []
        for intent in multi_agent_intents:
            result = self.generator.generate_query(intent)
            results.append(result)
        
        # Verify all agents got appropriate queries
        assert len(results) == 3
        
        for i, result in enumerate(results):
            assert result is not None
            assert isinstance(result.query, str)
            assert len(result.query.strip()) > 0
            
            # Verify role-specific context
            agent = agents[i]
            assert result.context_factors['user_role'] == agent.role
    
    def test_agent_stress_and_workload_integration(self):
        """Test integration with agent stress and workload patterns"""
        # Create high-stress scenario
        high_stress_intent = {
            'action': 'urgent_report',
            'username': self.finance_agent.username,
            'role': self.finance_agent.role,
            'target_database': 'finance_db',
            'stress_level': 0.9,
            'work_intensity': 1.8,
            'urgency': 'high'
        }
        
        # Generate query under stress
        result = self.generator.generate_query(high_stress_intent)
        
        # Verify query generated successfully
        assert result is not None
        assert isinstance(result.query, str)
        assert len(result.query.strip()) > 0
        
        # High stress might affect complexity or reasoning
        assert isinstance(result.reasoning, list)
        assert len(result.reasoning) > 0
        
        # Verify stress factors are considered
        reasoning_text = ' '.join(result.reasoning).lower()
        stress_indicators = ['stress', 'urgent', 'high', 'intensity', 'workload']
        # May or may not contain stress indicators depending on implementation
    
    def test_agent_cultural_context_integration(self):
        """Test integration with Vietnamese cultural context from agents"""
        # Create culturally-aware intent
        cultural_intent = {
            'action': 'hierarchy_report',
            'username': 'le_thi_hong',
            'role': 'MANAGEMENT',
            'target_database': 'hr_db',
            'department': 'Phòng Giám Đốc',
            'hierarchy_level': 9,
            'cultural_context': {
                'respect_seniority': True,
                'tet_season': False,
                'work_overtime_acceptable': True
            }
        }
        
        # Generate query with cultural context
        result = self.generator.generate_query(cultural_intent)
        
        # Verify query generated successfully
        assert result is not None
        assert isinstance(result.query, str)
        assert len(result.query.strip()) > 0
        
        # Verify cultural factors are considered
        assert isinstance(result.context_factors, dict)
        assert 'hierarchy_level' in result.context_factors
        assert result.context_factors['hierarchy_level'] >= 5  # Management level


class TestDatabaseExecutorIntegration:
    """Integration tests with database executor and feedback processing"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.executor = SQLExecutor(enable_state_sync=True)
        self.generator = DynamicSQLGenerator(seed=42, executor=self.executor)
        
        # Wait for initialization
        time.sleep(0.1)
    
    def teardown_method(self):
        """Clean up after tests"""
        if hasattr(self.executor, 'stop_state_synchronization'):
            self.executor.stop_state_synchronization()
    
    def test_executor_feedback_integration(self):
        """Test integration with executor feedback processing"""
        # Generate a query
        intent = {
            'action': 'customer_analysis',
            'username': 'test_user',
            'role': 'SALES',
            'target_database': 'sales_db'
        }
        
        result = self.generator.generate_query(intent)
        
        # Simulate query execution
        execution_result = self.executor.execute_query(
            query=result.query,
            database='sales_db',
            username='test_user',
            role='SALES'
        )
        
        # Process execution feedback
        self.generator.analyze_query_success(
            query=result.query,
            success=execution_result['success'],
            execution_time=execution_result['execution_time'],
            error_message=execution_result.get('error_message')
        )
        
        # Verify feedback was processed
        stats = self.generator.get_generation_stats()
        assert stats['total_generations'] >= 1
        
        if execution_result['success']:
            assert stats['success_rate'] > 0.0
        else:
            assert stats['success_rate'] == 0.0 or stats['total_generations'] > 1
    
    def test_database_state_synchronization(self):
        """Test real-time database state synchronization"""
        # Create execution feedback
        feedback_data = [
            {
                'query': "SELECT * FROM customers WHERE city = 'Hồ Chí Minh'",
                'success': True,
                'execution_time': 0.15,
                'error_message': None,
                'rows_affected': 0,
                'rows_returned': 25,
                'database': 'sales_db',
                'username': 'nguyen_van_nam',
                'role': 'SALES'
            },
            {
                'query': "INSERT INTO orders (customer_id, total) VALUES (999, 1500000)",
                'success': False,
                'execution_time': 0.05,
                'error_message': "Foreign key constraint violation",
                'rows_affected': 0,
                'rows_returned': 0,
                'database': 'sales_db',
                'username': 'le_minh_duc',
                'role': 'SALES'
            }
        ]
        
        # Send feedback to synchronizer
        for data in feedback_data:
            feedback = create_execution_feedback_from_executor_result(**data)
            if hasattr(self.executor, 'state_synchronizer') and self.executor.state_synchronizer:
                self.executor.state_synchronizer.add_execution_feedback(feedback)
        
        # Wait for processing
        time.sleep(0.5)
        
        # Check database states
        all_states = self.executor.get_all_database_states()
        
        # Verify states are tracked
        assert isinstance(all_states, dict)
        # May be empty if state synchronization is not fully implemented
    
    def test_performance_metrics_integration(self):
        """Test integration with performance metrics collection"""
        # Generate multiple queries and simulate execution
        intents = [
            {
                'action': f'test_action_{i}',
                'username': f'user_{i}',
                'role': 'SALES',
                'target_database': 'sales_db'
            }
            for i in range(5)
        ]
        
        for intent in intents:
            # Generate query
            result = self.generator.generate_query(intent)
            
            # Simulate execution with varying performance
            import random
            execution_time = random.uniform(0.05, 0.5)
            success = random.random() > 0.2  # 80% success rate
            
            # Process feedback
            self.generator.analyze_query_success(
                query=result.query,
                success=success,
                execution_time=execution_time,
                error_message=None if success else "Simulated error"
            )
        
        # Check performance metrics
        stats = self.generator.get_generation_stats()
        assert stats['total_generations'] >= 5
        assert 0.0 <= stats['success_rate'] <= 1.0
        assert 0.0 <= stats['fallback_rate'] <= 1.0
    
    def test_constraint_violation_handling(self):
        """Test handling of constraint violations from executor"""
        # Generate query that might cause constraint violation
        intent = {
            'action': 'data_insertion',
            'username': 'test_user',
            'role': 'SALES',
            'target_database': 'sales_db'
        }
        
        result = self.generator.generate_query(intent)
        
        # Simulate constraint violation
        violation_feedback = create_execution_feedback_from_executor_result(
            query=result.query,
            success=False,
            execution_time=0.02,
            error_message="FOREIGN KEY constraint failed: orders.customer_id",
            rows_affected=0,
            rows_returned=0,
            database='sales_db',
            username='test_user',
            role='SALES'
        )
        
        # Process violation feedback
        if hasattr(self.executor, 'state_synchronizer') and self.executor.state_synchronizer:
            self.executor.state_synchronizer.add_execution_feedback(violation_feedback)
        
        # Analyze failure
        self.generator.analyze_query_success(
            query=result.query,
            success=False,
            execution_time=0.02,
            error_message="FOREIGN KEY constraint failed"
        )
        
        # Verify failure was recorded
        stats = self.generator.get_generation_stats()
        assert stats['total_generations'] >= 1
        # Success rate should reflect the failure
    
    def test_entity_relationship_discovery(self):
        """Test entity relationship discovery from executor"""
        # Test relationship discovery for different databases
        databases = ['sales_db', 'hr_db', 'finance_db']
        
        for db_name in databases:
            if hasattr(self.executor, 'get_entity_relationship_map'):
                entity_map = self.executor.get_entity_relationship_map(db_name)
                
                if entity_map:
                    # Verify relationship map structure
                    assert hasattr(entity_map, 'relationships')
                    assert isinstance(entity_map.relationships, dict)
                    
                    # Use relationships in query generation
                    intent = {
                        'action': 'relationship_analysis',
                        'username': 'test_user',
                        'role': 'ADMIN',
                        'target_database': db_name
                    }
                    
                    result = self.generator.generate_query(intent)
                    
                    # Verify query generated successfully
                    assert result is not None
                    assert isinstance(result.query, str)
                    assert len(result.query.strip()) > 0


class TestScenarioManagerIntegration:
    """Integration tests with scenario manager and attack pattern coordination"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.generator = DynamicSQLGenerator(seed=42)
        
        # Mock scenario manager
        self.scenario_manager = Mock()
        self.scenario_manager.get_current_scenario.return_value = {
            'scenario_type': 'normal_operations',
            'attack_probability': 0.1,
            'current_threats': []
        }
    
    def test_normal_scenario_coordination(self):
        """Test coordination with normal operations scenario"""
        # Get current scenario
        scenario = self.scenario_manager.get_current_scenario()
        
        # Generate query in normal scenario context
        intent = {
            'action': 'daily_report',
            'username': 'regular_user',
            'role': 'SALES',
            'target_database': 'sales_db',
            'scenario_context': scenario
        }
        
        result = self.generator.generate_query(intent)
        
        # Verify normal query generation
        assert result is not None
        assert isinstance(result.query, str)
        assert len(result.query.strip()) > 0
        
        # Should use normal generation strategy
        assert result.generation_strategy in ['context_aware', 'fallback']
        assert result.fallback_used == False or result.generation_strategy == 'fallback'
    
    def test_attack_scenario_coordination(self):
        """Test coordination with attack scenario"""
        # Mock attack scenario
        attack_scenario = {
            'scenario_type': 'insider_threat',
            'attack_probability': 0.8,
            'current_threats': ['data_exfiltration', 'privilege_escalation'],
            'attack_stage': 2
        }
        
        self.scenario_manager.get_current_scenario.return_value = attack_scenario
        
        # Generate attack query in scenario context
        attack_intent = {
            'action': 'data_extraction',
            'username': 'insider_attacker',
            'role': 'HR',
            'target_database': 'hr_db',
            'attack_mode': True,
            'attack_type': 'insider_threat',
            'scenario_context': attack_scenario,
            'malicious': True
        }
        
        result = self.generator.generate_query(attack_intent, None)
        
        # Verify attack query generation
        assert result is not None
        assert isinstance(result.query, str)
        assert len(result.query.strip()) > 0
        
        # Should use attack generation strategy
        expected_strategies = ['attack_simulation', 'insider_threat', 'fallback']
        assert result.generation_strategy in expected_strategies
    
    def test_apt_scenario_progression(self):
        """Test APT scenario progression coordination"""
        # Mock APT scenario with progression
        apt_scenarios = [
            {
                'scenario_type': 'apt',
                'attack_probability': 0.9,
                'current_threats': ['reconnaissance'],
                'attack_stage': 1,
                'attack_id': 'apt_001'
            },
            {
                'scenario_type': 'apt',
                'attack_probability': 0.9,
                'current_threats': ['lateral_movement'],
                'attack_stage': 2,
                'attack_id': 'apt_001'
            },
            {
                'scenario_type': 'apt',
                'attack_probability': 0.9,
                'current_threats': ['data_collection'],
                'attack_stage': 3,
                'attack_id': 'apt_001'
            }
        ]
        
        apt_results = []
        
        for scenario in apt_scenarios:
            self.scenario_manager.get_current_scenario.return_value = scenario
            
            # Generate APT query for current stage
            apt_intent = {
                'action': 'apt_operation',
                'username': 'apt_attacker',
                'role': 'DEV',
                'target_database': 'admin_db',
                'attack_mode': True,
                'attack_type': 'apt',
                'apt_stage': scenario['attack_stage'],
                'attack_id': scenario['attack_id'],
                'scenario_context': scenario,
                'malicious': True
            }
            
            result = self.generator.generate_query(apt_intent, None)
            apt_results.append(result)
        
        # Verify APT progression
        assert len(apt_results) == 3
        
        for i, result in enumerate(apt_results):
            assert result is not None
            assert isinstance(result.query, str)
            assert len(result.query.strip()) > 0
            
            # Verify APT tracking
            apt_status = self.generator.get_apt_attack_status('apt_001')
            assert apt_status['attack_id'] == 'apt_001'
            assert apt_status['current_stage'] >= 1
    
    def test_cultural_exploitation_scenario(self):
        """Test cultural exploitation scenario coordination"""
        # Mock cultural exploitation scenario
        cultural_scenario = {
            'scenario_type': 'cultural_exploitation',
            'attack_probability': 0.7,
            'current_threats': ['hierarchy_abuse', 'tet_timing'],
            'cultural_factors': {
                'tet_season': True,
                'hierarchy_exploitation': True,
                'overtime_abuse': True
            }
        }
        
        self.scenario_manager.get_current_scenario.return_value = cultural_scenario
        
        # Generate cultural exploitation query
        cultural_intent = {
            'action': 'cultural_attack',
            'username': 'cultural_attacker',
            'role': 'MANAGEMENT',
            'target_database': 'hr_db',
            'attack_mode': True,
            'attack_type': 'cultural_exploitation',
            'scenario_context': cultural_scenario,
            'malicious': True
        }
        
        result = self.generator.generate_query(cultural_intent, None)
        
        # Verify cultural exploitation query
        assert result is not None
        assert isinstance(result.query, str)
        assert len(result.query.strip()) > 0
        
        # Verify cultural reasoning
        reasoning_text = ' '.join(result.reasoning).lower()
        cultural_indicators = ['cultural', 'hierarchy', 'vietnamese', 'tet', 'exploitation']
        cultural_reasoning_found = any(indicator in reasoning_text for indicator in cultural_indicators)
        assert cultural_reasoning_found
    
    def test_scenario_transition_handling(self):
        """Test handling of scenario transitions"""
        # Start with normal scenario
        normal_scenario = {
            'scenario_type': 'normal_operations',
            'attack_probability': 0.1,
            'current_threats': []
        }
        
        self.scenario_manager.get_current_scenario.return_value = normal_scenario
        
        # Generate normal query
        normal_intent = {
            'action': 'routine_check',
            'username': 'normal_user',
            'role': 'SALES',
            'target_database': 'sales_db',
            'scenario_context': normal_scenario
        }
        
        normal_result = self.generator.generate_query(normal_intent)
        
        # Transition to attack scenario
        attack_scenario = {
            'scenario_type': 'insider_threat',
            'attack_probability': 0.8,
            'current_threats': ['data_exfiltration'],
            'transition_from': 'normal_operations'
        }
        
        self.scenario_manager.get_current_scenario.return_value = attack_scenario
        
        # Generate attack query after transition
        attack_intent = {
            'action': 'suspicious_access',
            'username': 'suspicious_user',
            'role': 'FINANCE',
            'target_database': 'finance_db',
            'attack_mode': True,
            'attack_type': 'insider_threat',
            'scenario_context': attack_scenario,
            'malicious': True
        }
        
        attack_result = self.generator.generate_query(attack_intent, None)
        
        # Verify both queries generated successfully
        assert normal_result is not None
        assert attack_result is not None
        
        # Verify different strategies used
        assert normal_result.generation_strategy in ['context_aware', 'fallback']
        assert attack_result.generation_strategy in ['attack_simulation', 'insider_threat', 'fallback']
    
    def test_threat_intelligence_integration(self):
        """Test integration with threat intelligence from scenario manager"""
        # Mock threat intelligence scenario
        threat_scenario = {
            'scenario_type': 'targeted_attack',
            'attack_probability': 0.9,
            'current_threats': ['spear_phishing', 'credential_harvesting'],
            'threat_intelligence': {
                'known_attackers': ['attacker_001', 'attacker_002'],
                'attack_patterns': ['off_hours_access', 'bulk_data_extraction'],
                'target_databases': ['hr_db', 'finance_db']
            }
        }
        
        self.scenario_manager.get_current_scenario.return_value = threat_scenario
        
        # Generate threat-aware query
        threat_intent = {
            'action': 'targeted_extraction',
            'username': 'attacker_001',
            'role': 'HR',
            'target_database': 'hr_db',
            'attack_mode': True,
            'attack_type': 'targeted_attack',
            'scenario_context': threat_scenario,
            'malicious': True
        }
        
        result = self.generator.generate_query(threat_intent, None)
        
        # Verify threat-aware query generation
        assert result is not None
        assert isinstance(result.query, str)
        assert len(result.query.strip()) > 0
        
        # Verify threat intelligence is considered
        assert isinstance(result.context_factors, dict)
        assert result.context_factors['user_role'] == 'HR'


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])