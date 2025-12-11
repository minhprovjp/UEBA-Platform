#!/usr/bin/env python3
"""
Simulation Runner - Consolidated Simulation Execution and Management
Combines main execution, complete simulation runner, and configuration functionality
"""

import json
import time
import random
import sys
import threading
import uuid
import mysql.connector
import subprocess
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from agents_enhanced import EnhancedEmployeeAgent, EnhancedMaliciousAgent
from executor import SQLExecutor
from stats_utils import StatisticalGenerator
from obfuscator import SQLObfuscator
from corrected_enhanced_sql_library import CORRECTED_SQL_LIBRARY

class SimulationRunner:
    """Comprehensive simulation runner for Vietnamese Enterprise UBA Simulation"""
    
    def __init__(self):
        # Default configuration
        self.config = {
            "num_threads": 20,
            "simulation_speed_up": 1800,
            "start_date": datetime(2025, 12, 1, 5, 0, 0),
            "total_real_seconds": 3600,
            "db_password": "password",
            "user_rotation_interval": 60,
            "users_per_rotation": 15,
            "anomaly_percentage": 0.10,
            "insider_threat_percentage": 0.05,
            "external_hacker_count": 3,
            "enable_obfuscation": True
        }
        
        # Enhanced database configuration
        self.databases = [
            'sales_db', 'inventory_db', 'finance_db', 'marketing_db', 
            'support_db', 'hr_db', 'admin_db'
        ]
        
        # Role to database mapping
        self.role_database_access = {
            'SALES': ['sales_db', 'marketing_db', 'support_db'],
            'MARKETING': ['marketing_db', 'sales_db', 'support_db'],
            'CUSTOMER_SERVICE': ['support_db', 'sales_db', 'marketing_db'],
            'HR': ['hr_db', 'finance_db', 'admin_db'],
            'FINANCE': ['finance_db', 'sales_db', 'hr_db', 'inventory_db'],
            'DEV': self.databases,
            'MANAGEMENT': self.databases,
            'ADMIN': self.databases,
        }
        
        # Simulation state
        self.active_agents = []
        self.simulation_running = False
        self.start_time = None
        self.dataset_records = []
        
    def load_users(self):
        """Load users from configuration"""
        try:
            with open('simulation/users_config.json', 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            users_map = config.get("users", {})
            included_roles = ['SALES', 'MARKETING', 'CUSTOMER_SERVICE', 'HR', 'FINANCE', 'DEV', 'MANAGEMENT', 'ADMIN']
            
            pool_agents_simulation = []
            for username, role in users_map.items():
                if role in included_roles:
                    pool_agents_simulation.append({"username": username, "role": role})
            
            print(f"✅ Loaded {len(pool_agents_simulation)} users for simulation")
            return pool_agents_simulation
            
        except Exception as e:
            print(f"❌ Error loading users: {e}")
            return []
    
    def test_setup(self):
        """Test if setup is complete and ready for simulation"""
        print("🧪 TESTING SIMULATION SETUP")
        print("=" * 50)
        
        try:
            # Test user loading
            users = self.load_users()
            if not users:
                print("❌ No users loaded")
                return False
            
            print(f"✅ Users loaded: {len(users)}")
            
            # Test database connection
            try:
                conn = mysql.connector.connect(
                    host='localhost',
                    user='root',
                    password='root',
                    database='uba_db'
                )
                conn.close()
                print("✅ Database connection successful")
            except Exception as e:
                print(f"❌ Database connection failed: {e}")
                return False
            
            # Test sample user connection
            sample_user = users[0]
            try:
                conn = mysql.connector.connect(
                    host='localhost',
                    user=sample_user['username'],
                    password='password',
                    database='uba_db'
                )
                conn.close()
                print(f"✅ Sample user connection successful: {sample_user['username']}")
            except Exception as e:
                print(f"❌ Sample user connection failed: {e}")
                return False
            
            print("✅ Setup verification completed - ready for simulation")
            return True
            
        except Exception as e:
            print(f"❌ Setup test error: {e}")
            return False
    
    def configure_simulation(self):
        """Interactive simulation configuration"""
        print("⚙️ SIMULATION CONFIGURATION")
        print("=" * 50)
        
        configs = {
            "1": {"name": "Quick Test", "duration": "5 minutes", "speed": 1800, "seconds": 300},
            "2": {"name": "Half Day", "duration": "30 minutes", "speed": 1800, "seconds": 1800},
            "3": {"name": "Full Week", "duration": "2 hours", "speed": 3024, "seconds": 7200},
            "4": {"name": "Large Dataset", "duration": "4 hours", "speed": 10800, "seconds": 14400},
            "5": {"name": "Custom", "duration": "Custom", "speed": 0, "seconds": 0}
        }
        
        print("Available simulation configurations:")
        for key, config in configs.items():
            print(f"  {key}. {config['name']} - {config['duration']}")
        
        choice = input("\nSelect configuration (1-5): ").strip()
        
        if choice in configs:
            selected = configs[choice]
            if choice == "5":  # Custom
                try:
                    duration = int(input("Enter duration in minutes: "))
                    speed = int(input("Enter speed multiplier (1800 = 30min/sec): "))
                    self.config["total_real_seconds"] = duration * 60
                    self.config["simulation_speed_up"] = speed
                except ValueError:
                    print("Invalid input, using defaults")
            else:
                self.config["total_real_seconds"] = selected["seconds"]
                self.config["simulation_speed_up"] = selected["speed"]
            
            print(f"✅ Configuration set: {selected['name']}")
        else:
            print("Invalid choice, using defaults")
        
        # Anomaly configuration
        anomaly_choice = input("\nEnable anomalies? (y/n): ").strip().lower()
        if anomaly_choice == 'n':
            self.config["anomaly_percentage"] = 0.0
            self.config["insider_threat_percentage"] = 0.0
            self.config["external_hacker_count"] = 0
            print("✅ Clean dataset mode (no anomalies)")
        else:
            print("✅ Anomaly mode enabled")
    
    def create_agent(self, user_info, agent_id):
        """Create an agent (employee or malicious)"""
        username = user_info["username"]
        role = user_info["role"]
        
        # Determine if this should be a malicious agent
        is_malicious = False
        
        # Insider threat
        if random.random() < self.config["insider_threat_percentage"]:
            is_malicious = True
            agent = EnhancedMaliciousAgent(agent_id, username, role, is_insider=True)
        else:
            agent = EnhancedEmployeeAgent(agent_id, username, role)
        
        return agent
    
    def agent_worker(self, agent, executor, simulation_start_time):
        """Worker function for agent simulation"""
        try:
            while self.simulation_running:
                current_time = time.time()
                elapsed_real_time = current_time - simulation_start_time
                
                if elapsed_real_time >= self.config["total_real_seconds"]:
                    break
                
                # Calculate simulated time
                simulated_elapsed = elapsed_real_time * self.config["simulation_speed_up"]
                simulated_time = self.config["start_date"] + timedelta(seconds=simulated_elapsed)
                
                # Get next action from agent
                action = agent.get_next_action()
                if not action:
                    time.sleep(random.uniform(1, 3))
                    continue
                
                # Execute action
                result = executor.execute_action(agent, action, simulated_time)
                
                # Record result
                if result:
                    self.dataset_records.append(result)
                
                # Agent think time
                think_time = StatisticalGenerator.generate_pareto_delay(1, 5, 1.5)
                time.sleep(min(think_time, 10))  # Cap at 10 seconds
                
        except Exception as e:
            print(f"Agent {agent.username} error: {e}")
    
    def run_simulation(self):
        """Run the complete simulation"""
        print("🚀 STARTING VIETNAMESE ENTERPRISE UBA SIMULATION")
        print("=" * 60)
        
        # Load users
        users = self.load_users()
        if not users:
            print("❌ Cannot load users")
            return False
        
        # Initialize executor
        executor = SQLExecutor()
        
        # Create initial agents
        initial_agents = []
        for i in range(min(self.config["users_per_rotation"], len(users))):
            user_info = users[i]
            agent = self.create_agent(user_info, i)
            initial_agents.append(agent)
        
        print(f"✅ Created {len(initial_agents)} initial agents")
        
        # Start simulation
        self.simulation_running = True
        self.start_time = time.time()
        
        # Start agent threads
        with ThreadPoolExecutor(max_workers=self.config["num_threads"]) as thread_executor:
            futures = []
            
            for agent in initial_agents:
                future = thread_executor.submit(self.agent_worker, agent, executor, self.start_time)
                futures.append(future)
            
            # User rotation thread
            rotation_future = thread_executor.submit(self.user_rotation_worker, users, executor)
            futures.append(rotation_future)
            
            # Progress monitoring
            self.monitor_progress()
            
            # Stop simulation
            self.simulation_running = False
            
            # Wait for all threads to complete
            for future in futures:
                try:
                    future.result(timeout=10)
                except Exception as e:
                    print(f"Thread error: {e}")
        
        # Save results
        self.save_dataset()
        
        print("🎉 SIMULATION COMPLETED!")
        return True
    
    def user_rotation_worker(self, users, executor):
        """Worker for rotating users during simulation"""
        rotation_count = 0
        
        while self.simulation_running:
            time.sleep(self.config["user_rotation_interval"])
            
            if not self.simulation_running:
                break
            
            rotation_count += 1
            start_idx = (rotation_count * self.config["users_per_rotation"]) % len(users)
            end_idx = min(start_idx + self.config["users_per_rotation"], len(users))
            
            print(f"🔄 User rotation {rotation_count}: users {start_idx}-{end_idx}")
    
    def monitor_progress(self):
        """Monitor simulation progress"""
        while self.simulation_running:
            elapsed = time.time() - self.start_time
            progress = (elapsed / self.config["total_real_seconds"]) * 100
            
            simulated_elapsed = elapsed * self.config["simulation_speed_up"]
            simulated_time = self.config["start_date"] + timedelta(seconds=simulated_elapsed)
            
            print(f"📊 Progress: {progress:.1f}% | Records: {len(self.dataset_records)} | Simulated time: {simulated_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            time.sleep(30)  # Update every 30 seconds
            
            if elapsed >= self.config["total_real_seconds"]:
                break
    
    def save_dataset(self):
        """Save simulation dataset"""
        if not self.dataset_records:
            print("⚠️ No records to save")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"simulation_dataset_{timestamp}.csv"
        
        try:
            import pandas as pd
            df = pd.DataFrame(self.dataset_records)
            df.to_csv(filename, index=False)
            print(f"✅ Dataset saved: {filename} ({len(self.dataset_records)} records)")
        except Exception as e:
            print(f"❌ Error saving dataset: {e}")
    
    def run_complete_workflow(self):
        """Run the complete simulation workflow"""
        print("🚀 COMPLETE UBA SIMULATION WORKFLOW")
        print("=" * 60)
        
        # Step 1: Test setup
        print("🔍 Step 1: Testing setup...")
        if not self.test_setup():
            print("⚠️ Setup issues detected. Running database setup...")
            from database_manager import DatabaseManager
            db_manager = DatabaseManager()
            if not db_manager.complete_setup():
                print("❌ Setup failed!")
                return False
        
        # Step 2: Configure simulation
        print("\n⚙️ Step 2: Configuring simulation...")
        self.configure_simulation()
        
        # Step 3: Run simulation
        print("\n🚀 Step 3: Running simulation...")
        return self.run_simulation()

def main():
    """Main execution function"""
    runner = SimulationRunner()
    
    if len(sys.argv) > 1:
        action = sys.argv[1]
        
        if action == "test":
            runner.test_setup()
        elif action == "configure":
            runner.configure_simulation()
        elif action == "run":
            runner.run_simulation()
        elif action == "complete":
            runner.run_complete_workflow()
        else:
            print("Usage: python simulation_runner.py [test|configure|run|complete]")
    else:
        # Default: run complete workflow
        runner.run_complete_workflow()

if __name__ == "__main__":
    main()