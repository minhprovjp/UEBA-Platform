#!/usr/bin/env python3
"""
Enhanced Main Execution - Vietnamese Medium-Sized Sales Company Simulation
Integrates enriched SQL library with 7-database structure while maintaining all original performance settings
"""

import json
import time
import random
import sys
import threading
import uuid
import mysql.connector
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from agents_enhanced import EnhancedEmployeeAgent, EnhancedMaliciousAgent
from executor import SQLExecutor
from stats_utils import StatisticalGenerator
from obfuscator import SQLObfuscator
from corrected_enhanced_sql_library import CORRECTED_SQL_LIBRARY  # NEW: Fixed enhanced query library

# --- CẤU HÌNH TURBO (Preserved from original) ---
NUM_THREADS = 10           # 10 luồng spam
SIMULATION_SPEED_UP = 3600 # 1 giờ ảo / 1 giây thực
START_DATE = datetime(2025, 12, 11, 8, 0, 0)
TOTAL_REAL_SECONDS = 900   # Chạy 15 phút
DB_PASSWORD = "password"

# --- CẤU HÌNH ANOMALY (Flexible Anomaly Control) ---
ANOMALY_PERCENTAGE = 0.10          # 10% anomaly rate (0.0 = clean dataset, 1.0 = 100% anomalies)
INSIDER_THREAT_PERCENTAGE = 0.05   # 5% of employees become insider threats
EXTERNAL_HACKER_COUNT = 3          # Number of external hackers to create
ENABLE_OBFUSCATION = True          # Enable SQL obfuscation for attackers

# --- ENHANCED DATABASE CONFIGURATION ---
ENHANCED_DATABASES = [
    'sales_db', 'inventory_db', 'finance_db', 'marketing_db', 
    'support_db', 'hr_db', 'admin_db'
]

# Role to database mapping for enhanced system
ROLE_DATABASE_ACCESS = {
    'SALES': ['sales_db', 'marketing_db', 'support_db'],
    'MARKETING': ['marketing_db', 'sales_db', 'support_db'],
    'CUSTOMER_SERVICE': ['support_db', 'sales_db', 'marketing_db'],
    'HR': ['hr_db', 'finance_db', 'admin_db'],
    'FINANCE': ['finance_db', 'sales_db', 'hr_db', 'inventory_db'],
    'DEV': ENHANCED_DATABASES,  # Full access
    'MANAGEMENT': ENHANCED_DATABASES,  # Full access
    'ADMIN': ENHANCED_DATABASES,  # Full access
    'BAD_ACTOR': ['sales_db', 'marketing_db'],  # Limited access for insider threats
    'VULNERABLE': ['sales_db']  # Very limited access
}

# Biến đếm toàn cục
total_queries_sent = 0
lock = threading.Lock()

# --- CLIENT PROFILES (Enhanced for Vietnamese business context) ---
CLIENT_PROFILES = {
    "SALES_OFFICE": {
        "os": ["Win64", "Win32", "Windows"],
        "prog": ["Tableau", "PowerBIDesktop", "excel", "ODBC", "php", "java", "httpd"],
        "conn": ["libmysql"], 
        "ip_range": "192.168.10."
    },
    "MARKETING_OFFICE": {
        "os": ["Win64", "Windows", "macOS"],
        "prog": ["PowerBIDesktop", "Tableau", "ODBC", "php", "java", "chrome"],
        "conn": ["libmysql"],
        "ip_range": "192.168.15."
    },
    "CUSTOMER_SERVICE": {
        "os": ["Win64", "Windows"],
        "prog": ["php", "java", "httpd", "chrome", "ODBC"],
        "conn": ["libmysql"],
        "ip_range": "192.168.25."
    },
    "HR_OFFICE": {
        "os": ["Win64", "Windows"],
        "prog": ["java", "php", "httpd", "tomcat", "ODBC", "python"],
        "conn": ["libmysql"], 
        "ip_range": "192.168.20."
    },
    "FINANCE_OFFICE": {
        "os": ["Win64", "Windows"],
        "prog": ["excel", "ODBC", "java", "php", "SAP"],
        "conn": ["libmysql"],
        "ip_range": "192.168.30."
    },
    "DEV_WORKSTATION": {
        "os": ["Linux", "macOS", "Win64", "debian-linux-gnu", "darwin"],
        "prog": ["MySQLWorkbench", "dbeaver", "Sequel Pro", "phpMyAdmin", "java", "python", "php", "node", "ruby"],
        "conn": ["mysql-connector-python"], 
        "ip_range": "192.168.50."
    },
    "MANAGEMENT_OFFICE": {
        "os": ["Win64", "macOS", "Windows"],
        "prog": ["Tableau", "PowerBIDesktop", "excel", "ODBC", "java"],
        "conn": ["libmysql"],
        "ip_range": "192.168.40."
    },
    "ADMIN_WORKSTATION": {
        "os": ["Linux", "Win64", "debian-linux-gnu"],
        "prog": ["MySQLWorkbench", "dbeaver", "python", "java", "bash"],
        "conn": ["mysql-connector-python"],
        "ip_range": "192.168.60."
    },
    "HACKER_TOOLKIT": {
        "os": ["Linux", "debian-linux-gnu", "Win64", "Unknown"],
        "prog": ["python", "python3", "sqlmap", "java", "php", "ruby", "perl"],
        "conn": ["mysql-connector-python", "PyMySQL"], 
        "ip_range": "10.0.0."
    }
}

def generate_profile(role, is_malicious=False):
    """Enhanced profile generation for Vietnamese business context"""
    if is_malicious:
        base = CLIENT_PROFILES["HACKER_TOOLKIT"]
    else:
        # Map roles to appropriate client profiles
        profile_map = {
            "SALES": "SALES_OFFICE",
            "MARKETING": "MARKETING_OFFICE", 
            "CUSTOMER_SERVICE": "CUSTOMER_SERVICE",
            "HR": "HR_OFFICE",
            "FINANCE": "FINANCE_OFFICE",
            "DEV": "DEV_WORKSTATION",
            "MANAGEMENT": "MANAGEMENT_OFFICE",
            "ADMIN": "ADMIN_WORKSTATION"
        }
        
        profile_key = profile_map.get(role, "SALES_OFFICE")
        base = CLIENT_PROFILES[profile_key]

    # Generate Vietnamese-appropriate hostnames
    rnd_id = random.randint(100, 999)
    if is_malicious:
        src_host = random.choice(["kalibox", "unknown", f"pwned-{rnd_id}", "attacker-vm"])
    else:
        # Vietnamese office naming convention
        office_types = ["PC", "LAPTOP", "WS"]  # PC, Laptop, Workstation
        src_host = f"{role}-{rnd_id}-{random.choice(office_types)}"

    return {
        "client_os": random.choice(base["os"]),
        "program_name": random.choice(base["prog"]),
        "connector_name": random.choice(base["conn"]),
        "source_host": src_host,
        "source_ip": base["ip_range"] + str(random.randint(2, 250))
    }

# Enhanced Virtual Clock (preserved from original)
class VirtualClock:
    def __init__(self, start_time, speed_up):
        self.start_real = time.time()
        self.start_sim = start_time
        self.speed_up = speed_up

    def get_current_sim_time(self):
        """Tính thời gian ảo dựa trên thời gian trôi qua thực tế"""
        now = time.time()
        elapsed_real = now - self.start_real
        return self.start_sim + timedelta(seconds=elapsed_real * self.speed_up)

def load_enhanced_config():
    """Load enhanced configuration with Vietnamese users and 7-database structure"""
    try:
        with open("simulation/users_config.json", 'r', encoding='utf-8') as f:
            user_config = json.load(f)
        
        # Check if we have enhanced configuration
        if "company_info" in user_config and user_config["company_info"].get("databases") == 7:
            print("✅ Enhanced Vietnamese company configuration loaded")
            print(f"   Company: {user_config['company_info']['name']}")
            print(f"   Databases: {user_config['company_info']['databases']}")
            print(f"   Users: {len(user_config.get('users', {}))}")
        else:
            print("⚠️ Using basic configuration - consider running setup_enhanced_vietnamese_company.py")
        
        return user_config
    except Exception as e:
        print(f"❌ Failed to load configuration: {e}")
        print("💡 Make sure to run:")
        print("   1. python setup_enhanced_vietnamese_company.py")
        print("   2. python create_sandbox_user.py")
        sys.exit(1)

def configure_anomaly_scenario(scenario="balanced"):
    """Enhanced anomaly configuration with Vietnamese business context"""
    global ANOMALY_PERCENTAGE, INSIDER_THREAT_PERCENTAGE, EXTERNAL_HACKER_COUNT, ENABLE_OBFUSCATION
    
    scenarios = {
        "clean": {
            "anomaly_rate": 0.0,
            "insider_rate": 0.0,
            "hacker_count": 0,
            "obfuscation": False,
            "description": "Clean dataset - pure normal Vietnamese business operations"
        },
        "minimal": {
            "anomaly_rate": 0.02,
            "insider_rate": 0.01,
            "hacker_count": 1,
            "obfuscation": False,
            "description": "Minimal threats - very secure Vietnamese enterprise"
        },
        "balanced": {
            "anomaly_rate": 0.10,
            "insider_rate": 0.05,
            "hacker_count": 3,
            "obfuscation": True,
            "description": "Balanced scenario - realistic Vietnamese medium enterprise"
        },
        "high_threat": {
            "anomaly_rate": 0.25,
            "insider_rate": 0.15,
            "hacker_count": 5,
            "obfuscation": True,
            "description": "High threat - Vietnamese company under active attack"
        },
        "attack_simulation": {
            "anomaly_rate": 0.50,
            "insider_rate": 0.30,
            "hacker_count": 10,
            "obfuscation": True,
            "description": "Attack simulation - intensive security testing scenario"
        }
    }
    
    if scenario in scenarios:
        config = scenarios[scenario]
        ANOMALY_PERCENTAGE = config["anomaly_rate"]
        INSIDER_THREAT_PERCENTAGE = config["insider_rate"]
        EXTERNAL_HACKER_COUNT = config["hacker_count"]
        ENABLE_OBFUSCATION = config["obfuscation"]
        
        print(f"🎯 Configured for '{scenario}' scenario:")
        print(f"   Description: {config['description']}")
        print(f"   Anomaly Rate: {ANOMALY_PERCENTAGE*100:.1f}%")
        print(f"   Insider Threat Rate: {INSIDER_THREAT_PERCENTAGE*100:.1f}%")
        print(f"   External Hackers: {EXTERNAL_HACKER_COUNT}")
        print(f"   Obfuscation: {ENABLE_OBFUSCATION}")
    else:
        print(f"❌ Unknown scenario: {scenario}")
        print(f"Available scenarios: {list(scenarios.keys())}")

class EnhancedSQLGenerator:
    """Enhanced SQL generator using the enriched query library"""
    
    def __init__(self):
        self.sql_library = CORRECTED_SQL_LIBRARY
        self.query_cache = {}  # Cache queries by role and database
    
    def get_queries_for_role_and_database(self, role, database, complexity='ALL'):
        """Get appropriate queries for role and database combination"""
        cache_key = f"{role}_{database}_{complexity}"
        
        if cache_key not in self.query_cache:
            queries = self.sql_library.get_queries_by_database_and_role(database, role, complexity)
            self.query_cache[cache_key] = queries
        
        return self.query_cache[cache_key]
    
    def generate_sql_for_intent(self, intent, user_role):
        """Generate SQL based on intent and user role using enriched library"""
        action = intent.get('action', 'SELECT')
        
        # Determine appropriate database based on role and action
        accessible_databases = ROLE_DATABASE_ACCESS.get(user_role, ['sales_db'])
        target_database = random.choice(accessible_databases)
        
        # Determine query complexity based on role and action
        complexity = 'SIMPLE'
        if user_role in ['MANAGEMENT', 'ADMIN']:
            complexity = random.choice(['MEDIUM', 'COMPLEX'])
        elif user_role in ['FINANCE', 'DEV']:
            complexity = random.choice(['SIMPLE', 'MEDIUM'])
        
        # Get appropriate queries
        queries = self.get_queries_for_role_and_database(user_role, target_database, complexity)
        
        if queries:
            sql = random.choice(queries)
            # Add database context to intent
            intent['target_database'] = target_database
            return sql
        else:
            # Fallback to basic query if no enhanced queries available
            return f"SELECT COUNT(*) FROM {target_database}.customers"
    
    def generate_malicious_sql(self, attack_type='sql_injection'):
        """Generate malicious SQL for security testing"""
        return random.choice(self.sql_library.get_malicious_queries_enriched(attack_type))

def enhanced_user_worker(agent_template, sql_generator, v_clock, stop_event):
    """Enhanced worker with enriched SQL library and 7-database support"""
    global total_queries_sent
    
    # Generate profile for this worker
    my_profile = generate_profile(agent_template.role, agent_template.is_malicious)
    
    # Use SQLExecutor for database operations
    executor = SQLExecutor()
    
    while not stop_event.is_set():
        try:
            # Get virtual time
            sim_time = v_clock.get_current_sim_time()
            hour = sim_time.hour

            # Business hours and weekday check (unless malicious)
            if not agent_template.is_malicious:
                # Check if it's weekend (Saturday=5, Sunday=6)
                day_of_week = sim_time.weekday()
                if day_of_week >= 5:  # Weekend
                    # Very minimal weekend activity (only 5% chance)
                    if random.random() > 0.05:
                        time.sleep(0.001)
                        continue
                
                # Check business hours activity level
                activity_level = agent_template.get_activity_level(hour)
                if activity_level == 0.0:
                    time.sleep(0.001) 
                    continue
                # Reduce activity during low-activity periods
                elif activity_level < 0.5:
                    if random.random() > activity_level:
                        time.sleep(0.001)
                        continue

            # Generate action intent
            intent = agent_template.step()
            if intent['action'] in ["START", "LOGOUT"]:
                continue

            # Generate SQL using enhanced library
            if agent_template.is_malicious:
                # Malicious agents use attack queries
                attack_types = ['sql_injection', 'privilege_escalation', 'data_exfiltration']
                attack_type = random.choice(attack_types)
                sql = sql_generator.generate_malicious_sql(attack_type)
                intent['target_database'] = random.choice(ENHANCED_DATABASES)
            else:
                # Normal users use business queries
                sql = sql_generator.generate_sql_for_intent(intent, agent_template.role)
            
            # Apply obfuscation if needed
            if intent.get('obfuscate', False) or (agent_template.is_malicious and ENABLE_OBFUSCATION):
                sql = SQLObfuscator.obfuscate(sql)
            
            # Execute query
            ts_str = sim_time.isoformat()
            success = False
            
            # Fix intent structure for executor compatibility
            intent['database'] = intent.get('target_database', 'sales_db')
            
            try:
                success = executor.execute(intent, sql, sim_timestamp=ts_str, client_profile=my_profile)
                
                if success:
                    with lock: 
                        total_queries_sent += 1
                
                # Enhanced logging with database context
                if random.random() < 0.3:
                    db_info = intent.get('target_database', 'unknown')
                    role_info = f"{agent_template.role}"
                    if agent_template.is_malicious:
                        role_info += " (MALICIOUS)"
                    
                    print(f"[{ts_str}] {intent['user']} ({role_info}) | {db_info} | {intent['action']} -> {'OK' if success else 'FAIL'}")

            except Exception as e:
                success = False
                if random.random() < 0.1:  # Log some errors
                    print(f"[ERROR] {intent['user']}: {str(e)[:100]}")
            
            # Agent reaction
            agent_template.react(success)
            
            # Enhanced think time based on Vietnamese business patterns
            min_wait = 2
            mode_wait = 15
            
            # Adjust wait time based on role and action
            if agent_template.role in ['FINANCE', 'MANAGEMENT']:
                mode_wait = 30  # More deliberate actions
            elif agent_template.role in ['CUSTOMER_SERVICE', 'SALES']:
                mode_wait = 10  # Faster-paced work
            
            if "UPDATE" in intent['action'] or "CREATE" in intent['action']: 
                mode_wait *= 2  # Longer for modification operations
            
            sim_wait = StatisticalGenerator.generate_pareto_delay(min_wait, mode_wait)
            real_wait = sim_wait / v_clock.speed_up
            
            time.sleep(max(real_wait, 0.001))

        except Exception as e:
            time.sleep(0.1)

def main():
    """Enhanced main function with Vietnamese enterprise simulation"""
    # Parse command line arguments
    scenario = "balanced"  # Default scenario
    if len(sys.argv) > 1:
        scenario = sys.argv[1]
    
    # Configure anomaly scenario
    configure_anomaly_scenario(scenario)
    
    print(f"🚀 ENHANCED VIETNAMESE ENTERPRISE SIMULATION")
    print(f"=" * 60)
    print(f"🏢 Company: Công ty TNHH Thương mại ABC")
    print(f"📊 Databases: {len(ENHANCED_DATABASES)} specialized databases")
    print(f"⚡ Speed: x{SIMULATION_SPEED_UP} acceleration")
    print(f"🎯 Scenario: {scenario}")
    print(f"📅 Start Time: {START_DATE}")
    
    # Load enhanced configuration
    user_config = load_enhanced_config()
    users_map = user_config.get("users", {})
    
    # Initialize enhanced SQL generator
    sql_generator = EnhancedSQLGenerator()
    
    # Create agent pool with Vietnamese users
    pool_agents = []
    insider_count = 0
    
    # Create normal employees with insider threat potential
    for username, role in users_map.items():
        if role in ["SALES", "MARKETING", "CUSTOMER_SERVICE", "HR", "FINANCE", "DEV", "MANAGEMENT", "ADMIN"]:
            agent = EnhancedEmployeeAgent(0, username, role, {})  # Empty db_state for enhanced system
            agent.current_state = "LOGIN"
            
            # Create insider threats
            if random.random() < INSIDER_THREAT_PERCENTAGE:
                agent.is_malicious = True
                insider_count += 1
                print(f"🔴 Insider threat: {username} ({role})")
            
            pool_agents.append(agent)
    
    # Add external hackers
    hacker_count = 0
    for i in range(EXTERNAL_HACKER_COUNT):
        hacker = EnhancedMaliciousAgent(999 + i, {})  # Empty db_state for enhanced system
        if ENABLE_OBFUSCATION and i > 0:
            hacker.obfuscation_mode = True
        pool_agents.append(hacker)
        hacker_count += 1
        print(f"🔴 External hacker: hacker_{i+1}")
    
    # Print comprehensive statistics
    total_agents = len(pool_agents)
    total_malicious = insider_count + hacker_count
    actual_anomaly_rate = total_malicious / total_agents if total_agents > 0 else 0
    
    print(f"\n📊 ENHANCED SIMULATION STATISTICS:")
    print(f"   Vietnamese Users: {len(users_map)}")
    print(f"   Active Agents: {total_agents}")
    print(f"   Normal Employees: {total_agents - total_malicious}")
    print(f"   Insider Threats: {insider_count}")
    print(f"   External Hackers: {hacker_count}")
    print(f"   Target Anomaly Rate: {ANOMALY_PERCENTAGE*100:.1f}%")
    print(f"   Actual Anomaly Rate: {actual_anomaly_rate*100:.1f}%")
    print(f"   Obfuscation: {ENABLE_OBFUSCATION}")
    
    # Print database access summary
    print(f"\n🗄️ DATABASE ACCESS SUMMARY:")
    for db in ENHANCED_DATABASES:
        roles_with_access = [role for role, dbs in ROLE_DATABASE_ACCESS.items() if db in dbs]
        print(f"   {db}: {len(roles_with_access)} roles ({', '.join(roles_with_access[:3])}{'...' if len(roles_with_access) > 3 else ''})")
    
    # Initialize simulation components
    v_clock = VirtualClock(START_DATE, SIMULATION_SPEED_UP)
    stop_event = threading.Event()
    
    print(f"\n🚀 Starting enhanced simulation with {NUM_THREADS} threads...")
    
    # Run simulation
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        for _ in range(NUM_THREADS):
            agent = random.choice(pool_agents)
            executor.submit(enhanced_user_worker, agent, sql_generator, v_clock, stop_event)
            
        try:
            start_run = time.time()
            while (time.time() - start_run) < TOTAL_REAL_SECONDS:
                time.sleep(1)
                curr_sim = v_clock.get_current_sim_time()
                sys.stdout.write(f"\r⚡ Queries: {total_queries_sent} | Sim Time: {curr_sim.strftime('%Y-%m-%d %H:%M')} | Rate: {total_queries_sent/(time.time()-start_run):.1f}/s ")
                sys.stdout.flush()
        except KeyboardInterrupt:
            print("\n🛑 Stopping simulation...")
        finally:
            stop_event.set()
            
            # Final statistics
            elapsed_time = time.time() - start_run
            print(f"\n✅ SIMULATION COMPLETED")
            print(f"   Duration: {elapsed_time:.1f} seconds")
            print(f"   Total Queries: {total_queries_sent}")
            print(f"   Average Rate: {total_queries_sent/elapsed_time:.1f} queries/second")
            print(f"   Simulated Time: {curr_sim.strftime('%Y-%m-%d %H:%M')}")

if __name__ == "__main__":
    main()