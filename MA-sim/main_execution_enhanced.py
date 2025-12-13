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
from translator import EnhancedSQLTranslator
from stats_utils import StatisticalGenerator
from obfuscator import SQLObfuscator
from enriched_sql_library import EnrichedSQLLibrary  # NEW: Fixed enhanced query library

# --- CẤU HÌNH TURBO (Enhanced for all users) ---
NUM_THREADS = 20           # Increased threads to handle more users
SIMULATION_SPEED_UP = 1800 # 30 minutes simulated per 1 second real (slower for better coverage)
START_DATE = datetime(2025, 12, 1, 5, 0, 0)
TOTAL_REAL_SECONDS = 300  # Run for 1 hour real time (30 hours simulated time)
DB_PASSWORD = "password"

# User rotation settings for comprehensive coverage
USER_ROTATION_INTERVAL = 60  # Rotate users every 60 seconds
USERS_PER_ROTATION = 15      # Number of active users per rotation

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

# Role to database mapping for enhanced system (matches actual permissions)
ROLE_DATABASE_ACCESS = {
    'SALES': ['sales_db', 'marketing_db', 'support_db'],
    'MARKETING': ['marketing_db', 'sales_db', 'support_db'],
    'CUSTOMER_SERVICE': ['support_db', 'sales_db', 'marketing_db'],
    'HR': ['hr_db', 'finance_db', 'admin_db'],
    'FINANCE': ['finance_db', 'sales_db', 'hr_db', 'inventory_db'],
    'DEV': ENHANCED_DATABASES,  # Full access
    'MANAGEMENT': ['sales_db', 'hr_db', 'finance_db', 'marketing_db', 'support_db', 'inventory_db', 'admin_db'],
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
    """Enhanced SQL generator using both enriched query library and translator"""
    
    def __init__(self, db_state=None):
        self.sql_library = EnrichedSQLLibrary()
        self.translator = EnhancedSQLTranslator(db_state)
        self.query_cache = {}  # Cache queries by role and database
    
    def get_queries_for_role_and_database(self, role, database, complexity='ALL'):
        """Get appropriate queries for role and database combination"""
        cache_key = f"{role}_{database}_{complexity}"
        
        if cache_key not in self.query_cache:
            queries = self.sql_library.get_queries_by_database_and_role(database, role, complexity)
            self.query_cache[cache_key] = queries
        
        return self.query_cache[cache_key]
    
    def generate_sql_for_intent(self, intent, user_role):
        """Generate SQL based on intent and user role using both enriched library and translator"""
        action = intent.get('action', 'SELECT')
        target_database = intent.get('target_database', 'sales_db')
        
        # First try using the enhanced translator for better context-aware SQL
        try:
            sql = self.translator.translate(intent)
            if sql and not sql.startswith("SELECT 'Missing") and not sql.startswith("SELECT 'Error"):
                return sql
        except Exception as e:
            # Fall back to library-based generation
            pass
        
        # Fallback to enriched library - use enhanced SQL templates
        from enhanced_sql_templates import EnhancedSQLTemplates
        templates = EnhancedSQLTemplates()
        
        # Get appropriate queries for the specific database and role
        queries = templates.get_queries_by_database_and_role(target_database, user_role)
        
        if queries:
            sql = random.choice(queries)
            return sql
        else:
            # Final fallback to basic safe query based on database
            if target_database == 'sales_db':
                return "SELECT COUNT(*) FROM sales_db.customers WHERE status = 'active'"
            elif target_database == 'marketing_db':
                return "SELECT COUNT(*) FROM marketing_db.campaigns WHERE status = 'active'"
            elif target_database == 'support_db':
                return "SELECT COUNT(*) FROM support_db.support_tickets WHERE status = 'open'"
            elif target_database == 'hr_db':
                return "SELECT COUNT(*) FROM hr_db.employees WHERE status = 'active'"
            elif target_database == 'finance_db':
                return "SELECT COUNT(*) FROM finance_db.invoices WHERE status = 'paid'"
            elif target_database == 'inventory_db':
                return "SELECT COUNT(*) FROM inventory_db.inventory_levels WHERE current_stock > 0"
            elif target_database == 'admin_db':
                return "SELECT COUNT(*) FROM admin_db.system_logs WHERE log_level = 'info'"
            else:
                return "SELECT 1"
    
    def generate_malicious_sql(self, attack_type='sql_injection'):
        """Generate malicious SQL for security testing"""
        try:
            return self.translator._generate_malicious_sql({'attack_chain': attack_type}, {})
        except:
            return f"SELECT * FROM {attack_type}_attack"  # Fallback malicious query

def enhanced_user_worker(agent_template, sql_generator, v_clock, stop_event):
    """Enhanced worker with enriched SQL library and 7-database support"""
    global total_queries_sent
    
    # Generate profile for this worker
    my_profile = generate_profile(agent_template.role, agent_template.is_malicious)
    
    # Use SQLExecutor for database operations
    executor = SQLExecutor()
    
    while not stop_event.is_set():
        try:
            # Check stop event more frequently
            if stop_event.is_set():
                break
                
            # Get virtual time
            sim_time = v_clock.get_current_sim_time()
            hour = sim_time.hour

            # Strict Vietnamese business hours enforcement (unless malicious)
            if not agent_template.is_malicious:
                # Check if it's weekend (Saturday=5, Sunday=6)
                day_of_week = sim_time.weekday()
                if day_of_week >= 5:  # Weekend
                    # Absolutely no weekend activity for normal employees
                    time.sleep(0.1)
                    continue
                
                # Check if it's a Vietnamese holiday
                current_date = sim_time.date().isoformat()
                vietnamese_holidays = [
                    "2025-01-01",  # New Year
                    "2025-01-29",  # Tet (Lunar New Year)
                    "2025-04-30",  # Liberation Day
                    "2025-05-01",  # Labor Day
                    "2025-09-02"   # Independence Day
                ]
                if current_date in vietnamese_holidays:
                    # No activity on Vietnamese holidays for normal employees
                    time.sleep(0.1)
                    continue
                
                # Strict business hours check (8AM-6PM with lunch break 12-1PM)
                if not agent_template.is_work_hours(hour):
                    # Absolutely no activity outside work hours for normal employees
                    time.sleep(0.1)
                    continue
                
                # Vietnamese lunch break patterns (more realistic)
                # Traditional: 12-1PM strict break
                # Extended: 11:30AM-1:30PM flexible break  
                # Modern: Some activity during lunch but reduced
                if 12 <= hour < 13:
                    # Core lunch hour: 20% activity (some employees work through lunch)
                    if random.random() > 0.20:
                        time.sleep(0.05)
                        continue
                elif hour == 11 and sim_time.minute >= 30:
                    # Late morning lunch (11:30-12:00): 40% activity
                    if random.random() > 0.40:
                        time.sleep(0.03)
                        continue
                elif hour == 13 and sim_time.minute < 30:
                    # Extended lunch (1:00-1:30PM): 30% activity  
                    if random.random() > 0.30:
                        time.sleep(0.04)
                        continue
                
                # Check activity level during work hours
                activity_level = agent_template.get_activity_level(hour)
                if activity_level < 0.5:  # During low-activity periods
                    if random.random() > activity_level:
                        time.sleep(0.02)
                        continue
            else:
                # Malicious agents can work outside hours but with very low probability
                day_of_week = sim_time.weekday()
                if day_of_week >= 5:  # Weekend
                    if random.random() > 0.05:  # Only 5% chance on weekends
                        time.sleep(0.1)
                        continue
                
                # Very low activity outside business hours for malicious agents
                if not (8 <= hour < 18):  # Outside 8AM-6PM
                    if random.random() > 0.1:  # Only 10% chance outside hours
                        time.sleep(0.1)
                        continue

            # Check stop event before generating action
            if stop_event.is_set():
                break

            # Generate action intent
            intent = agent_template.step()
            if intent['action'] in ["START", "LOGOUT"]:
                continue

            # Enhanced malicious behavior with rule-bypassing
            if agent_template.is_malicious:
                # Check for rule-bypassing scenarios
                bypass_technique = intent.get('bypass_technique')
                timing_context = intent.get('timing_context')
                
                # Apply timing-based bypasses
                if timing_context == "off_hours" and (hour < 8 or hour > 18):
                    # Allow off-hours activity for sophisticated attackers
                    pass  # Bypass work hours restriction
                elif timing_context == "lunch_break" and (12 <= hour < 13):
                    # Exploit lunch break low monitoring
                    pass  # Bypass lunch break restriction
                elif timing_context == "vietnamese_holiday":
                    current_date = sim_time.date().isoformat()
                    vietnamese_holidays = ["2025-01-01", "2025-01-29", "2025-04-30", "2025-05-01", "2025-09-02"]
                    if current_date in vietnamese_holidays:
                        pass  # Bypass holiday restriction
                
                # Generate sophisticated attack SQL
                if bypass_technique:
                    # Use advanced attack patterns for rule bypassing
                    attack_types = ['advanced_sqli', 'privilege_escalation', 'data_exfiltration', 'backdoor_creation']
                    attack_type = random.choice(attack_types)
                else:
                    # Standard attack patterns
                    attack_types = ['sql_injection', 'privilege_escalation', 'data_exfiltration']
                    attack_type = random.choice(attack_types)
                
                sql = sql_generator.generate_malicious_sql(attack_type)
                
                # Enhanced database targeting for bypasses
                if bypass_technique == "network_segmentation_bypass":
                    # Target high-value databases across network segments
                    intent['target_database'] = random.choice(['hr_db', 'finance_db', 'admin_db'])
                else:
                    intent['target_database'] = random.choice(ENHANCED_DATABASES)
            else:
                # Normal users use business queries - ensure they only access allowed databases
                accessible_databases = ROLE_DATABASE_ACCESS.get(agent_template.role, ['sales_db'])
                
                # Map actions to appropriate databases to avoid permission failures
                action = intent.get('action', 'SELECT')
                if action in ['CREATE_ORDER', 'VIEW_ORDER', 'UPDATE_ORDER_STATUS', 'ADD_ITEM', 'SEARCH_ORDER', 'VIEW_CUSTOMER', 'SEARCH_CUSTOMER', 'UPDATE_CUSTOMER']:
                    # Order and customer operations should only happen in sales_db
                    if 'sales_db' in accessible_databases:
                        intent['target_database'] = 'sales_db'
                    else:
                        intent['target_database'] = accessible_databases[0]
                elif action in ['SEARCH_CAMPAIGN', 'VIEW_CAMPAIGN', 'UPDATE_CAMPAIGN', 'CREATE_CAMPAIGN', 'VIEW_LEADS', 'CREATE_LEAD', 'UPDATE_LEAD']:
                    # Campaign and lead operations should only happen in marketing_db
                    if 'marketing_db' in accessible_databases:
                        intent['target_database'] = 'marketing_db'
                    else:
                        intent['target_database'] = accessible_databases[0]
                elif action in ['VIEW_TICKET', 'CREATE_TICKET', 'UPDATE_TICKET', 'SEARCH_TICKET']:
                    # Support operations should only happen in support_db
                    if 'support_db' in accessible_databases:
                        intent['target_database'] = 'support_db'
                    else:
                        intent['target_database'] = accessible_databases[0]
                elif action in ['VIEW_EMPLOYEE', 'UPDATE_EMPLOYEE', 'CREATE_EMPLOYEE', 'SEARCH_EMPLOYEE', 'VIEW_PROFILE', 'CHECK_ATTENDANCE', 'UPDATE_SALARY']:
                    # HR operations should only happen in hr_db
                    if 'hr_db' in accessible_databases:
                        intent['target_database'] = 'hr_db'
                    else:
                        intent['target_database'] = accessible_databases[0]
                elif action in ['VIEW_INVOICE', 'CREATE_INVOICE', 'UPDATE_INVOICE', 'VIEW_EXPENSES', 'VIEW_REPORT']:
                    # Finance operations should only happen in finance_db
                    if 'finance_db' in accessible_databases:
                        intent['target_database'] = 'finance_db'
                    else:
                        intent['target_database'] = accessible_databases[0]
                else:
                    # For generic actions, use the primary database for the role
                    intent['target_database'] = accessible_databases[0]
                
                sql = sql_generator.generate_sql_for_intent(intent, agent_template.role)
            
            # Apply obfuscation if needed
            if intent.get('obfuscate', False) or (agent_template.is_malicious and ENABLE_OBFUSCATION):
                sql = SQLObfuscator.obfuscate(sql)
            
            # Check stop event before executing
            if stop_event.is_set():
                break
            
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
            
            # Optimized think time for better query generation
            min_wait = 1  # Reduced minimum wait
            mode_wait = 8  # Reduced mode wait for more activity
            
            # Adjust wait time based on role and action
            if agent_template.role in ['FINANCE', 'MANAGEMENT']:
                mode_wait = 12  # Reduced from 30 to 12
            elif agent_template.role in ['CUSTOMER_SERVICE', 'SALES']:
                mode_wait = 5   # Reduced from 10 to 5 for high-activity roles
            elif agent_template.role in ['DEV', 'ADMIN']:
                mode_wait = 6   # Moderate activity for technical roles
            
            if "UPDATE" in intent['action'] or "CREATE" in intent['action']: 
                mode_wait *= 1.5  # Reduced multiplier from 2 to 1.5
            
            sim_wait = StatisticalGenerator.generate_pareto_delay(min_wait, mode_wait)
            real_wait = sim_wait / v_clock.speed_up
            
            # Ensure minimum activity with shorter waits and check stop event during sleep
            sleep_time = max(real_wait, 0.001)
            sleep_intervals = max(1, int(sleep_time / 0.1))  # Sleep in 0.1s intervals
            for _ in range(sleep_intervals):
                if stop_event.is_set():
                    break
                time.sleep(min(0.1, sleep_time / sleep_intervals))

        except KeyboardInterrupt:
            break
        except Exception as e:
            if stop_event.is_set():
                break
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
    
    # Initialize enhanced SQL generator with database state
    db_state = user_config.get("db_state", {})
    sql_generator = EnhancedSQLGenerator(db_state)
    
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
    
    # Add external hackers with rule-bypassing capabilities
    hacker_count = 0
    for i in range(EXTERNAL_HACKER_COUNT):
        hacker = EnhancedMaliciousAgent(999 + i, {})  # Empty db_state for enhanced system
        
        # Assign different skill levels and bypass capabilities
        if i == 0:
            hacker.skill_level = "advanced"
            hacker.attack_origin = "international"
            hacker.detection_avoidance = 0.8
            print(f"🔴 Advanced APT hacker: advanced_persistent_threat")
        elif i == 1:
            hacker.skill_level = "intermediate"
            hacker.attack_origin = "domestic"
            hacker.detection_avoidance = 0.6
            print(f"🔴 Rule-bypassing hacker: rule_bypass_specialist")
        else:
            hacker.skill_level = "script_kiddie"
            hacker.attack_origin = "unknown"
            hacker.detection_avoidance = 0.3
            print(f"🔴 Script kiddie: script_kiddie_{i}")
        
        if ENABLE_OBFUSCATION and i > 0:
            hacker.obfuscation_mode = True
        
        pool_agents.append(hacker)
        hacker_count += 1
    
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
    
    print(f"\n🚀 Starting enhanced simulation with ALL USERS active...")
    print(f"   Total Users: {len(pool_agents)}")
    print(f"   Simulation Speed: {SIMULATION_SPEED_UP}x")
    print(f"   Real Time Duration: {TOTAL_REAL_SECONDS} seconds ({TOTAL_REAL_SECONDS/60:.1f} minutes)")
    print(f"   Simulated Time Duration: {TOTAL_REAL_SECONDS * SIMULATION_SPEED_UP / 3600:.1f} hours")
    
    # Create a thread for each user to ensure all users are active
    print(f"\n👥 Creating threads for all {len(pool_agents)} users...")
    
    # Set up signal handler for immediate Ctrl+C response
    import signal
    
    def signal_handler(signum, frame):
        stop_event.set()
        # Use os.write to avoid reentrant call issues
        import os
        os.write(1, b"\n\nCtrl+C detected - stopping simulation gracefully...\n")
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        start_run = time.time()
        
        # Start daemon threads manually instead of ThreadPoolExecutor
        threads = []
        
        print(f"✅ Starting {len(pool_agents)} user threads...")
        for i, agent in enumerate(pool_agents):
            thread = threading.Thread(
                target=enhanced_user_worker, 
                args=(agent, sql_generator, v_clock, stop_event),
                daemon=True  # Daemon threads will exit when main thread exits
            )
            thread.start()
            threads.append((agent.username, thread))
            
            # Add small delay to stagger thread starts
            if i % 10 == 0 and i > 0:
                time.sleep(0.1)
        
        print(f"✅ All {len(threads)} user threads started!")
        print(f"💡 Press Ctrl+C to stop the simulation gracefully")
        
        # Main monitoring loop
        last_report = time.time()
        while (time.time() - start_run) < TOTAL_REAL_SECONDS and not stop_event.is_set():
            # Sleep in smaller intervals to be more responsive to Ctrl+C
            for _ in range(10):  # Sleep 1 second total in 0.1s intervals
                if stop_event.is_set():
                    break
                time.sleep(0.1)
            
            if stop_event.is_set():
                break
            
            # Report progress every 30 seconds
            if time.time() - last_report >= 30:
                curr_sim = v_clock.get_current_sim_time()
                active_threads = len([t for _, t in threads if t.is_alive()])
                elapsed = time.time() - start_run
                remaining = TOTAL_REAL_SECONDS - elapsed
                
                print(f"\n📊 Progress Report:")
                print(f"   Elapsed: {elapsed/60:.1f}min | Remaining: {remaining/60:.1f}min")
                print(f"   Active Threads: {active_threads}/{len(threads)}")
                print(f"   Total Queries: {total_queries_sent:,}")
                print(f"   Query Rate: {total_queries_sent/elapsed:.1f}/sec")
                print(f"   Sim Time: {curr_sim.strftime('%Y-%m-%d %H:%M')}")
                print(f"💡 Press Ctrl+C to stop gracefully")
                
                last_report = time.time()
            
            # Update status line
            curr_sim = v_clock.get_current_sim_time()
            active_count = len([t for _, t in threads if t.is_alive()])
            elapsed = time.time() - start_run
            sys.stdout.write(f"\r⚡ Queries: {total_queries_sent:,} | Active: {active_count}/{len(threads)} | Elapsed: {elapsed/60:.1f}min | Sim: {curr_sim.strftime('%H:%M')} [Ctrl+C to stop]")
            sys.stdout.flush()
        
        if not stop_event.is_set():
            print(f"\n⏰ Simulation time completed!")
            
    except KeyboardInterrupt:
        print("\n🛑 Keyboard interrupt - stopping simulation...")
        stop_event.set()
    except Exception as e:
        print(f"\n❌ Simulation error: {e}")
        stop_event.set()
    finally:
        # Signal all threads to stop
        stop_event.set()
        
        # Wait a moment for threads to finish gracefully
        print("🔄 Waiting for threads to finish...")
        time.sleep(1)
        
        # Check if threads are still alive
        if 'threads' in locals():
            alive_threads = [name for name, t in threads if t.is_alive()]
            if alive_threads:
                print(f"⚠️ {len(alive_threads)} threads still running (will exit with main process)")
            else:
                print("✅ All threads stopped gracefully")
        
        # Final statistics
        elapsed_time = time.time() - start_run if 'start_run' in locals() else 0
        print(f"\n✅ SIMULATION COMPLETED")
        print(f"   Duration: {elapsed_time:.1f} seconds")
        print(f"   Total Queries: {total_queries_sent:,}")
        if elapsed_time > 0:
            print(f"   Average Rate: {total_queries_sent/elapsed_time:.1f} queries/second")
        if 'curr_sim' in locals():
            print(f"   Simulated Time: {curr_sim.strftime('%Y-%m-%d %H:%M')}")
        print(f"🎯 Simulation stopped successfully")

if __name__ == "__main__":
    main()