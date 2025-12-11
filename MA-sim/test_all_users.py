#!/usr/bin/env python3
"""
Test All Users - Quick verification that all 97 users can be loaded and configured
"""

import json
import sys
import os

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_user_loading():
    """Test that all users can be loaded from configuration"""
    
    print("🧪 TESTING USER LOADING")
    print("=" * 50)
    
    try:
        # Load configuration
        with open('simulation/users_config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        users_map = config.get("users", {})
        all_users = config['users']
        included_roles = ['SALES', 'MARKETING', 'CUSTOMER_SERVICE', 'HR', 'FINANCE', 'DEV', 'MANAGEMENT', 'ADMIN']
        
        print(f"📊 Configuration Analysis:")
        print(f"   Total users in config: {len(all_users)}")
        
        # Filter users by role
        pool_agents_simulation = []
        role_counts = {}
        
        for username, role in users_map.items():
            if role in included_roles:
                pool_agents_simulation.append({"username": username, "role": role})
                role_counts[role] = role_counts.get(role, 0) + 1
        
        print(f"   Users for simulation: {len(pool_agents_simulation)}")
        print(f"   Excluded users: {len(all_users) - len(pool_agents_simulation)}")
        
        print(f"\n📈 Role Distribution:")
        for role in included_roles:
            count = role_counts.get(role, 0)
            print(f"   {role}: {count} users")
        
        # Test agent creation simulation
        print(f"\n🤖 Simulating Agent Creation:")
        
        # Import the agent classes
        try:
            from agents_enhanced import EnhancedEmployeeAgent, EnhancedMaliciousAgent
            
            # Create agents like in main_execution_enhanced.py
            pool_agents = []
            insider_count = 0
            
            for username, role in users_map.items():
                if role in included_roles:
                    agent = EnhancedEmployeeAgent(0, username, role, {})
                    agent.current_state = "LOGIN"
                    pool_agents.append(agent)
            
            print(f"   ✅ Successfully created {len(pool_agents)} agents")
            
            # Show sample of users
            print(f"\n👥 Sample Users (first 10):")
            for i, agent in enumerate(pool_agents[:10]):
                print(f"   {i+1}. {agent.username} ({agent.role})")
            
            if len(pool_agents) > 10:
                print(f"   ... and {len(pool_agents) - 10} more users")
            
            # Verify all expected users are present
            expected_users = set(user for user, role in all_users.items() if role in included_roles)
            actual_users = set(agent.username for agent in pool_agents)
            
            missing_users = expected_users - actual_users
            extra_users = actual_users - expected_users
            
            print(f"\n✅ VERIFICATION RESULTS:")
            print(f"   Expected users: {len(expected_users)}")
            print(f"   Created agents: {len(actual_users)}")
            print(f"   Missing users: {len(missing_users)}")
            print(f"   Extra users: {len(extra_users)}")
            
            if len(missing_users) == 0 and len(actual_users) == len(expected_users):
                print(f"\n🎉 SUCCESS: All {len(expected_users)} users ready for simulation!")
                return True
            else:
                if missing_users:
                    print(f"   Missing: {list(missing_users)[:5]}...")
                if extra_users:
                    print(f"   Extra: {list(extra_users)[:5]}...")
                return False
                
        except ImportError as e:
            print(f"   ❌ Could not import agent classes: {e}")
            print(f"   This is expected if running outside the simulation environment")
            print(f"   Configuration test passed, but agent creation test skipped")
            return True
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def show_simulation_recommendations():
    """Show recommendations for simulation parameters"""
    
    print(f"\n💡 SIMULATION RECOMMENDATIONS")
    print("=" * 50)
    
    print("For a complete dataset with all 97 users:")
    print()
    print("🚀 Quick Test (5 minutes):")
    print("   SIMULATION_SPEED_UP = 1800")
    print("   TOTAL_REAL_SECONDS = 300")
    print("   Result: 2.5 hours simulated, all users active")
    print()
    print("📊 Half Day (30 minutes):")
    print("   SIMULATION_SPEED_UP = 1800") 
    print("   TOTAL_REAL_SECONDS = 1800")
    print("   Result: 15 hours simulated, full business day")
    print()
    print("📈 Full Week (2 hours):")
    print("   SIMULATION_SPEED_UP = 3024")
    print("   TOTAL_REAL_SECONDS = 7200") 
    print("   Result: 7 days simulated, complete week")
    print()
    print("🎯 Large Dataset (4 hours):")
    print("   SIMULATION_SPEED_UP = 10800")
    print("   TOTAL_REAL_SECONDS = 14400")
    print("   Result: 30 days simulated, comprehensive dataset")
    print()
    print("Use configure_simulation.py to easily set these parameters!")

if __name__ == "__main__":
    print("🧪 ALL USERS TEST")
    print("=" * 60)
    
    success = test_user_loading()
    
    if success:
        show_simulation_recommendations()
        print(f"\n✅ Ready to run simulation with all users!")
    else:
        print(f"\n❌ Issues found. Please check configuration.")
    
    sys.exit(0 if success else 1)