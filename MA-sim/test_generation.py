#!/usr/bin/env python3
"""
Test dataset generation with fixes
"""

from agents_enhanced import EnhancedEmployeeAgent, EnhancedMaliciousAgent
from executor import SQLExecutor
from translator import EnhancedSQLTranslator
import json

def test_dataset_generation():
    """Test a small dataset generation to verify fixes"""
    print("🧪 Testing dataset generation with fixes...")
    
    # Load user config
    with open("simulation/users_config.json", 'r', encoding='utf-8') as f:
        user_config = json.load(f)
    
    users_map = user_config.get("users", {})
    
    # Create a few test agents
    test_users = list(users_map.items())[:3]  # Just 3 users for testing
    
    executor = SQLExecutor()
    translator = EnhancedSQLTranslator()
    
    print(f"Testing with users: {[u[0] for u in test_users]}")
    
    for username, role in test_users:
        print(f"\n👤 Testing user: {username} ({role})")
        
        # Create agent
        agent = EnhancedEmployeeAgent(1, username, role)
        print(f"   Agent username: {agent.username}")
        
        # Generate intent
        intent = agent.step()
        print(f"   Intent action: {intent['action']}")
        
        # Generate SQL
        sql = translator.translate(intent)
        print(f"   Generated SQL: {sql[:100]}...")
        
        # Execute (this adds SIM_META)
        result = executor.execute(intent, sql, client_profile={'source_ip': '192.168.1.100'})
        print(f"   Execution result: {result}")
    
    # Test malicious agent
    print(f"\n🔴 Testing malicious agent...")
    mal_agent = EnhancedMaliciousAgent(2, "test_insider", "SALES", is_insider=True)
    print(f"   Malicious agent username: {mal_agent.username}")
    
    mal_intent = mal_agent.step()
    print(f"   Malicious intent action: {mal_intent['action']}")
    
    mal_sql = translator.translate(mal_intent)
    print(f"   Malicious SQL: {mal_sql[:100]}...")
    
    mal_result = executor.execute(mal_intent, mal_sql, client_profile={'source_ip': '10.0.0.1'})
    print(f"   Malicious execution result: {mal_result}")

if __name__ == "__main__":
    test_dataset_generation()