#!/usr/bin/env python3
"""
Test Database State Synchronization Integration

Tests the integration between executor, database state synchronization,
and dynamic SQL generation with real-time state updates.
"""

import time
import random
from datetime import datetime

# Import components
from executor import SQLExecutor
from dynamic_sql_generation.generator import DynamicSQLGenerator
from dynamic_sql_generation.database_state_sync import (
    ExecutionFeedback, 
    create_execution_feedback_from_executor_result
)


def test_database_state_synchronization():
    """Test database state synchronization integration"""
    print("🔄 TESTING DATABASE STATE SYNCHRONIZATION INTEGRATION")
    print("=" * 60)
    
    # Create executor with state synchronization enabled
    print("1. Creating executor with state synchronization...")
    executor = SQLExecutor(enable_state_sync=True)
    
    # Create dynamic SQL generator with executor integration
    print("2. Creating dynamic SQL generator with executor integration...")
    generator = DynamicSQLGenerator(seed=42, executor=executor)
    
    # Wait for synchronization to start
    time.sleep(1.0)
    
    # Test 1: Simulate some database operations
    print("\n3. Simulating database operations...")
    
    test_feedback_data = [
        {
            'query': "SELECT * FROM sales_db.customers WHERE city = 'Hồ Chí Minh'",
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
            'query': "SELECT COUNT(*) FROM hr_db.employees WHERE department = 'Phòng Tài Chính'",
            'success': True,
            'execution_time': 0.08,
            'error_message': None,
            'rows_affected': 0,
            'rows_returned': 1,
            'database': 'hr_db',
            'username': 'tran_thi_lan',
            'role': 'HR'
        },
        {
            'query': "INSERT INTO sales_db.orders (customer_id, total) VALUES (999, 1500000)",
            'success': False,
            'execution_time': 0.05,
            'error_message': "Foreign key constraint violation: customer_id 999 does not exist",
            'rows_affected': 0,
            'rows_returned': 0,
            'database': 'sales_db',
            'username': 'le_minh_duc',
            'role': 'SALES'
        },
        {
            'query': "UPDATE finance_db.invoices SET status = 'paid' WHERE invoice_id = 123",
            'success': True,
            'execution_time': 0.12,
            'error_message': None,
            'rows_affected': 1,
            'rows_returned': 0,
            'database': 'finance_db',
            'username': 'pham_thi_mai',
            'role': 'FINANCE'
        }
    ]
    
    # Send feedback to synchronizer
    for i, data in enumerate(test_feedback_data):
        feedback = create_execution_feedback_from_executor_result(**data)
        if executor.state_synchronizer:
            executor.state_synchronizer.add_execution_feedback(feedback)
        print(f"   • Sent feedback {i+1}: {data['query'][:50]}...")
    
    # Wait for processing
    print("   • Waiting for feedback processing...")
    time.sleep(2.0)
    
    # Test 2: Check database states
    print("\n4. Checking database states...")
    
    all_states = executor.get_all_database_states()
    print(f"   • Tracked databases: {list(all_states.keys())}")
    
    for db_name, state in all_states.items():
        print(f"\n   📊 {db_name.upper()} State:")
        print(f"      • Entity counts: {dict(list(state.entity_counts.items())[:3])}...")
        print(f"      • Constraint violations: {len(state.constraint_violations)}")
        print(f"      • Recent modifications: {len(state.recent_modifications)}")
        print(f"      • Avg query time: {state.performance_metrics.avg_query_time:.3f}s")
        print(f"      • Slow queries: {state.performance_metrics.slow_query_count}")
    
    # Test 3: Check performance metrics
    print("\n5. Checking performance metrics...")
    
    perf_metrics = executor.get_performance_metrics()
    if perf_metrics:
        print(f"   📈 Performance Metrics:")
        print(f"      • Overall avg query time: {perf_metrics['avg_query_time_overall']:.3f}s")
        print(f"      • Total slow queries: {perf_metrics['total_slow_queries']}")
        print(f"      • Databases tracked: {len(perf_metrics['databases_tracked'])}")
        print(f"      • Total feedback processed: {perf_metrics['total_feedback_processed']}")
        
        if perf_metrics['success_rates']:
            print(f"      • Success rates: {perf_metrics['success_rates']}")
        
        if perf_metrics['error_counts']:
            print(f"      • Error counts: {perf_metrics['error_counts']}")
    
    # Test 4: Test dynamic SQL generation with real-time state
    print("\n6. Testing dynamic SQL generation with real-time database state...")
    
    test_intents = [
        {
            'action': 'customer_analysis',
            'username': 'nguyen_van_nam',
            'role': 'SALES',
            'department': 'Phòng Kinh Doanh',
            'target_database': 'sales_db',
            'expertise_level': 'intermediate'
        },
        {
            'action': 'employee_report',
            'username': 'tran_thi_lan',
            'role': 'HR',
            'department': 'Phòng Nhân Sự',
            'target_database': 'hr_db',
            'expertise_level': 'advanced'
        },
        {
            'action': 'financial_analysis',
            'username': 'pham_thi_mai',
            'role': 'FINANCE',
            'department': 'Phòng Tài Chính',
            'target_database': 'finance_db',
            'expertise_level': 'expert'
        }
    ]
    
    for i, intent in enumerate(test_intents):
        print(f"\n   🔍 Test Intent {i+1}: {intent['action']} on {intent['target_database']}")
        
        # Generate query with real-time database state
        result = generator.generate_query(intent)
        
        print(f"      • Strategy: {result.generation_strategy}")
        print(f"      • Complexity: {result.complexity_level.name}")
        print(f"      • Fallback used: {result.fallback_used}")
        print(f"      • Generation time: {result.generation_time:.3f}s")
        print(f"      • Query length: {len(result.query)} chars")
        print(f"      • Query preview: {result.query[:80]}...")
        
        # Check if real-time database state was used
        db_state_info = generator.get_database_state_info(intent['target_database'])
        if db_state_info:
            print(f"      • Real-time state used: ✅")
            print(f"        - Entity counts: {len(db_state_info['entity_counts'])} tables")
            print(f"        - Constraint violations: {db_state_info['constraint_violations_count']}")
            print(f"        - Avg query time: {db_state_info['avg_query_time']:.3f}s")
        else:
            print(f"      • Real-time state used: ❌ (using defaults)")
        
        # Test entity relationships
        relationships = generator.get_entity_relationships(intent['target_database'])
        if relationships:
            print(f"      • Entity relationships: {len(relationships)} tables mapped")
            # Show sample relationships
            sample_table = list(relationships.keys())[0] if relationships else None
            if sample_table:
                related = relationships[sample_table]
                print(f"        - {sample_table} -> {related[:2]}..." if len(related) > 2 else f"        - {sample_table} -> {related}")
    
    # Test 5: Test pattern learning with feedback
    print("\n7. Testing pattern learning with execution feedback...")
    
    # Simulate successful query execution
    test_query = result.query
    generator.analyze_query_success(
        query=test_query,
        success=True,
        execution_time=0.25,
        error_message=None
    )
    
    # Simulate failed query execution
    generator.analyze_query_success(
        query="SELECT * FROM nonexistent_table",
        success=False,
        execution_time=0.01,
        error_message="Table 'nonexistent_table' doesn't exist"
    )
    
    # Check generation statistics
    stats = generator.get_generation_stats()
    print(f"   📊 Generation Statistics:")
    print(f"      • Total generations: {stats['total_generations']}")
    print(f"      • Success rate: {stats['success_rate']:.1%}")
    print(f"      • Fallback rate: {stats['fallback_rate']:.1%}")
    print(f"      • Learned patterns: {stats['learned_patterns']}")
    
    # Test 6: Test entity relationship discovery
    print("\n8. Testing entity relationship discovery...")
    
    for db_name in ['sales_db', 'hr_db', 'finance_db']:
        entity_map = executor.get_entity_relationship_map(db_name)
        if entity_map:
            print(f"   🔗 {db_name.upper()} Relationships:")
            relationship_count = sum(len(rels) for rels in entity_map.relationships.values())
            print(f"      • Total relationships: {relationship_count}")
            print(f"      • Tables with relationships: {len(entity_map.relationships)}")
            
            # Show sample relationships
            for table, rels in list(entity_map.relationships.items())[:2]:
                rel_info = [f"{rel.to_table}({rel.foreign_key})" for rel in rels[:2]]
                print(f"      • {table}: {', '.join(rel_info)}")
    
    # Cleanup
    print("\n9. Cleaning up...")
    executor.stop_state_synchronization()
    
    print("\n✅ DATABASE STATE SYNCHRONIZATION INTEGRATION TEST COMPLETED")
    print("=" * 60)
    
    return True


def test_performance_under_load():
    """Test performance under simulated load"""
    print("\n🚀 TESTING PERFORMANCE UNDER LOAD")
    print("=" * 40)
    
    # Create executor and generator
    executor = SQLExecutor(enable_state_sync=True)
    generator = DynamicSQLGenerator(seed=123, executor=executor)
    
    # Wait for initialization
    time.sleep(0.5)
    
    # Simulate high-frequency feedback
    print("1. Simulating high-frequency database operations...")
    
    databases = ['sales_db', 'hr_db', 'finance_db', 'marketing_db']
    users = ['user1', 'user2', 'user3', 'user4', 'user5']
    roles = ['SALES', 'HR', 'FINANCE', 'MARKETING']
    
    start_time = time.time()
    feedback_count = 100
    
    for i in range(feedback_count):
        # Generate random feedback
        database = random.choice(databases)
        user = random.choice(users)
        role = random.choice(roles)
        
        success = random.random() > 0.1  # 90% success rate
        execution_time = random.uniform(0.01, 2.0)
        
        feedback = create_execution_feedback_from_executor_result(
            query=f"SELECT * FROM {database}.table_{i % 10} WHERE id = {i}",
            success=success,
            execution_time=execution_time,
            error_message=None if success else f"Error {i}",
            rows_affected=0,
            rows_returned=random.randint(0, 100) if success else 0,
            database=database,
            username=user,
            role=role
        )
        
        if executor.state_synchronizer:
            executor.state_synchronizer.add_execution_feedback(feedback)
        
        if i % 20 == 0:
            print(f"   • Processed {i+1}/{feedback_count} operations...")
    
    processing_time = time.time() - start_time
    print(f"   • Completed {feedback_count} operations in {processing_time:.2f}s")
    print(f"   • Rate: {feedback_count/processing_time:.1f} operations/second")
    
    # Wait for processing
    time.sleep(2.0)
    
    # Check final state
    print("\n2. Checking final state after load test...")
    
    perf_metrics = executor.get_performance_metrics()
    if perf_metrics:
        print(f"   • Total feedback processed: {perf_metrics['total_feedback_processed']}")
        print(f"   • Databases tracked: {len(perf_metrics['databases_tracked'])}")
        print(f"   • Overall avg query time: {perf_metrics['avg_query_time_overall']:.3f}s")
    
    # Test generation performance
    print("\n3. Testing generation performance...")
    
    generation_start = time.time()
    generation_count = 20
    
    for i in range(generation_count):
        intent = {
            'action': f'test_action_{i}',
            'username': random.choice(users),
            'role': random.choice(roles),
            'target_database': random.choice(databases),
            'expertise_level': 'intermediate'
        }
        
        result = generator.generate_query(intent)
        
        if i % 5 == 0:
            print(f"   • Generated {i+1}/{generation_count} queries...")
    
    generation_time = time.time() - generation_start
    print(f"   • Generated {generation_count} queries in {generation_time:.2f}s")
    print(f"   • Rate: {generation_count/generation_time:.1f} queries/second")
    
    # Cleanup
    executor.stop_state_synchronization()
    
    print("\n✅ PERFORMANCE TEST COMPLETED")
    
    return True


if __name__ == "__main__":
    try:
        # Run integration test
        test_database_state_synchronization()
        
        # Run performance test
        test_performance_under_load()
        
        print("\n🎉 ALL TESTS PASSED!")
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()