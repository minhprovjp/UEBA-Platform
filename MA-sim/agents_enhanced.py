#!/usr/bin/env python3
"""
Enhanced Agents for Vietnamese Medium-Sized Sales Company Simulation
Supports 7-database structure with role-appropriate behavior patterns
"""

import random
from config_markov import MARKOV_TRANSITIONS, ACTION_REQUIREMENTS
from stats_utils import StatisticalGenerator 

class EnhancedEmployeeAgent:
    """Enhanced employee agent with Vietnamese business context and 7-database awareness"""
    
    def __init__(self, agent_id, username, role, db_data=None):
        self.agent_id = agent_id
        self.username = username
        self.role = role
        self.db_data = db_data or {}
        
        # Enhanced state management
        self.current_state = "START"
        self.session_context = {}
        self.is_malicious = False
        
        # Enhanced session management
        self.current_port = 0
        self.frustration_level = 0
        self.work_intensity = self._calculate_work_intensity()
        
        # Vietnamese business context
        self.department = self._get_department()
        self.work_schedule = self._get_work_schedule()
        self.database_preferences = self._get_database_preferences()
        
        # Performance tracking
        self.successful_queries = 0
        self.failed_queries = 0
        self.session_duration = 0
        self.stress_level = 0.0 # 0.0 -> 1.0
        self.is_fatigued = False

    def _calculate_stress(self, current_hour):
        """Tính độ stress dựa trên giờ làm việc"""
        # Làm việc quá giờ (sau 18h) hoặc quá sớm (trước 7h) -> Stress tăng
        if current_hour > 18 or current_hour < 7:
            self.stress_level = min(1.0, self.stress_level + 0.1)
        else:
            # Giờ hành chính -> Stress giảm từ từ
            self.stress_level = max(0.0, self.stress_level - 0.05)
            
        self.is_fatigued = self.stress_level > 0.7


    def _get_department(self):
        """Map role to Vietnamese department"""
        department_map = {
            'SALES': 'Phòng Kinh Doanh',
            'MARKETING': 'Phòng Marketing',
            'CUSTOMER_SERVICE': 'Phòng Chăm Sóc Khách Hàng',
            'HR': 'Phòng Nhân Sự',
            'FINANCE': 'Phòng Tài Chính',
            'DEV': 'Phòng IT',
            'MANAGEMENT': 'Ban Giám Đốc',
            'ADMIN': 'Phòng Quản Trị'
        }
        return department_map.get(self.role, 'Phòng Tổng Hợp')

    def _get_work_schedule(self):
        """Define work schedule based on Vietnamese business hours"""
        base_schedule = {
            'start_hour': 8,
            'end_hour': 17,
            'lunch_start': 12,
            'lunch_end': 13,
            'peak_hours': [9, 10, 14, 15, 16]
        }
        
        # Adjust based on role
        if self.role in ['CUSTOMER_SERVICE']:
            base_schedule['start_hour'] = 7  # Earlier start for customer service
            base_schedule['end_hour'] = 19   # Later end
        elif self.role in ['DEV', 'ADMIN']:
            base_schedule['start_hour'] = 9  # Flexible hours for technical roles
            base_schedule['end_hour'] = 18
        elif self.role in ['MANAGEMENT']:
            base_schedule['start_hour'] = 8
            base_schedule['end_hour'] = 19   # Longer hours for management
        
        return base_schedule

    def _get_database_preferences(self):
        """Define database access preferences based on role and actual permissions"""
        preferences = {
            'SALES': {
                'primary': ['sales_db'],
                'secondary': ['marketing_db', 'support_db'],
                'rare': []  # Only access databases they have permissions for
            },
            'MARKETING': {
                'primary': ['marketing_db'],
                'secondary': ['sales_db', 'support_db'],
                'rare': []
            },
            'CUSTOMER_SERVICE': {
                'primary': ['support_db'],
                'secondary': ['sales_db', 'marketing_db'],
                'rare': []
            },
            'HR': {
                'primary': ['hr_db'],
                'secondary': ['finance_db', 'admin_db'],
                'rare': []
            },
            'FINANCE': {
                'primary': ['finance_db'],
                'secondary': ['sales_db', 'hr_db', 'inventory_db'],
                'rare': []
            },
            'DEV': {
                'primary': ['admin_db'],
                'secondary': ['sales_db', 'hr_db', 'finance_db'],
                'rare': ['marketing_db', 'support_db', 'inventory_db']
            },
            'MANAGEMENT': {
                'primary': ['sales_db'],
                'secondary': ['hr_db', 'finance_db', 'marketing_db'],
                'rare': ['support_db', 'inventory_db', 'admin_db']
            },
            'ADMIN': {
                'primary': ['admin_db'],
                'secondary': ['sales_db', 'hr_db', 'finance_db'],
                'rare': ['marketing_db', 'support_db', 'inventory_db']
            }
        }
        return preferences.get(self.role, preferences['SALES'])

    def _calculate_work_intensity(self):
        """Calculate work intensity based on role and Vietnamese business culture"""
        base_intensity = 1.0
        
        role_multipliers = {
            'SALES': 1.3,           # High intensity - sales targets
            'CUSTOMER_SERVICE': 1.2, # High intensity - customer demands
            'MARKETING': 1.1,       # Above average - campaign deadlines
            'FINANCE': 1.0,         # Steady intensity
            'HR': 0.9,              # Moderate intensity
            'DEV': 1.1,             # Above average - project deadlines
            'MANAGEMENT': 1.2,      # High intensity - oversight responsibilities
            'ADMIN': 0.8            # Lower intensity - maintenance tasks
        }
        
        return base_intensity * role_multipliers.get(self.role, 1.0)

    def assign_new_session(self):
        """Enhanced session assignment with Vietnamese business context"""
        self.current_port = random.randint(49152, 65535)
        self.session_context = {
            "session_id": random.randint(10000, 99999),
            "login_time": random.randint(8, 9),  # Vietnamese business hours
            "department": self.department
        }

    def is_work_hours(self, current_hour):
        """Check if current hour is within Vietnamese business work hours"""
        schedule = self.work_schedule
        
        # Outside basic work hours
        if current_hour < schedule['start_hour'] or current_hour > schedule['end_hour']:
            return False
        
        # Vietnamese lunch break is more flexible - some employees work through lunch
        # Core lunch hour (12-1PM) is still considered work hours but with reduced activity
        # This allows for realistic Vietnamese business patterns where some employees
        # work through lunch or have flexible lunch schedules
        
        return True

    def get_activity_level(self, current_hour):
        """Get activity level based on Vietnamese business patterns"""
        if not self.is_work_hours(current_hour):
            return 0.1 if self.is_malicious else 0.0
        
        # Peak hours have higher activity
        if current_hour in self.work_schedule['peak_hours']:
            return self.work_intensity * 1.5
        
        # Regular work hours
        return self.work_intensity

    def select_target_database(self):
        """Select target database based on role preferences and business logic"""
        prefs = self.database_preferences
        
        # Weighted selection based on preferences
        choice = random.random()
        
        if choice < 0.7:  # 70% primary databases
            return random.choice(prefs['primary'])
        elif choice < 0.9:  # 20% secondary databases
            return random.choice(prefs['secondary']) if prefs['secondary'] else random.choice(prefs['primary'])
        else:  # 10% rare databases (only if user has permissions)
            return random.choice(prefs['rare']) if prefs['rare'] else random.choice(prefs['primary'])

    def get_next_action(self):
        """Get next action for the agent - compatibility method for simulation runner"""
        return self.step()
    
    def step(self):
        """Enhanced step function with Vietnamese business logic"""
        # Determine next state
        next_state = self._get_next_state()
        
        # Session management
        if next_state == "LOGIN":
            self.assign_new_session()
        elif next_state == "LOGOUT":
            self.current_port = 0
            self.session_context = {}

        # Prepare query parameters
        query_params = self._prepare_data_for_action(next_state)
        
        # Select target database
        target_database = self.select_target_database()
        
        # Update state
        self.current_state = next_state
        
        # Create enhanced intent
        intent = {
            "user": self.username,
            "role": self.role,
            "action": next_state,
            "params": query_params,
            "session_id": self.session_context.get("session_id"),
            "client_port": self.current_port,
            "target_database": target_database,
            "department": self.department,
            "is_anomaly": 1 if self.is_malicious else 0,
            "work_intensity": self.work_intensity
        }
        
        # Enhanced insider threat behavior
        if self.is_malicious:
            # Insider threats occasionally try to access unauthorized databases
            if random.random() < 0.3:
                unauthorized_dbs = ['finance_db', 'hr_db', 'admin_db']
                intent["target_database"] = random.choice(unauthorized_dbs)
            
            # Insider threats use obfuscation more frequently
            if random.random() < 0.4:
                intent["obfuscate"] = True
        
        # Mô phỏng người dùng mệt mỏi gõ sai lệnh
        if self.is_fatigued and random.random() < 0.2: # 20% cơ hội lỗi khi mệt
            intent["action"] = "TYPO_ERROR" # Sẽ được Translator xử lý thành SQL lỗi (1064)
            intent["is_anomaly"] = 0 # Lỗi do người, không phải hack (False Positive)
            
        return intent

    def react(self, success):
        """Enhanced reaction with learning behavior"""
        if success:
            self.successful_queries += 1
            self.frustration_level = max(0, self.frustration_level - 1)
        else:
            self.failed_queries += 1
            self.frustration_level += 1
            
            # Non-malicious users get frustrated and may logout
            if not self.is_malicious and self.frustration_level > 5:
                self.current_state = "LOGOUT"
                self.frustration_level = 0
        
        # Malicious users adapt their behavior
        if self.is_malicious and not success:
            # Increase obfuscation tendency after failures
            if hasattr(self, 'obfuscation_tendency'):
                self.obfuscation_tendency = min(0.8, self.obfuscation_tendency + 0.1)
            else:
                self.obfuscation_tendency = 0.3

    def _get_next_state(self):
        """Enhanced state transition with Vietnamese business patterns"""
        # Handle roles not in MARKOV_TRANSITIONS by using SALES as default
        role_key = self.role if self.role in MARKOV_TRANSITIONS else "SALES"
        role_transitions = MARKOV_TRANSITIONS.get(role_key, {})
        current_transitions = role_transitions.get(self.current_state)
        
        if not current_transitions:
            return "START"
        
        states = list(current_transitions.keys())
        probabilities = list(current_transitions.values())
        
        # Adjust probabilities based on work intensity and time
        adjusted_probabilities = []
        for i, prob in enumerate(probabilities):
            state = states[i]
            
            # Increase activity during peak hours
            if state in ["SEARCH_CUSTOMER", "VIEW_ORDER", "UPDATE_CUSTOMER"]:
                prob *= self.work_intensity
            
            adjusted_probabilities.append(prob)
        
        # Normalize probabilities
        total = sum(adjusted_probabilities)
        if total > 0:
            adjusted_probabilities = [p/total for p in adjusted_probabilities]
        else:
            adjusted_probabilities = probabilities
        
        next_state = random.choices(states, weights=adjusted_probabilities, k=1)[0]
        
        return next_state

    def _prepare_data_for_action(self, action):
        """Enhanced data preparation with Vietnamese business context"""
        params = {}
        
        if action in ACTION_REQUIREMENTS:
            required_keys = ACTION_REQUIREMENTS[action]
            
            for key in required_keys:
                if key in self.session_context:
                    params[key] = self.session_context[key]
                else:
                    val = self._pick_random_entity(key)
                    params[key] = val
                    self.session_context[key] = val
        
        # Enhanced context management for Vietnamese business workflows
        if action.startswith("SEARCH_"):
            # Clear related context for new searches
            context_mappings = {
                "SEARCH_CUSTOMER": ["customer_id", "order_id"],
                "SEARCH_ORDER": ["order_id"],
                "SEARCH_PRODUCT": ["product_id"],
                "SEARCH_EMPLOYEE": ["employee_id"]
            }
            
            if action in context_mappings:
                for key in context_mappings[action]:
                    self.session_context.pop(key, None)
        
        return params

    def _pick_random_entity(self, key):
        """Enhanced entity selection with Vietnamese business patterns"""
        mapping = {
            "customer_id": "customer_ids",
            "order_id": "order_ids", 
            "product_id": "product_ids",
            "employee_id": "employee_ids",
            "campaign_id": "campaign_ids"
        }
        
        if key in mapping and self.db_data and mapping[key] in self.db_data:
            data_list = self.db_data[mapping[key]]
            # Use Zipfian distribution for realistic business patterns
            return StatisticalGenerator.pick_zipfian_item(data_list)
        
        return random.randint(1, 1000)  # Fallback with reasonable range

class EnhancedMaliciousAgent(EnhancedEmployeeAgent):
    """Enhanced malicious agent with sophisticated attack patterns and rule-bypassing capabilities"""
    
    def __init__(self, agent_id, username=None, role=None, db_data=None, is_insider=False):
        # Handle insider vs external attacker
        if is_insider and username and role:
            # Insider threat - use provided username and role
            super().__init__(agent_id, username, role, db_data)
            self.is_insider = True
            self.is_malicious = True
        else:
            # External attacker
            super().__init__(agent_id, "unknown_hacker", "ATTACKER", db_data)
            self.is_insider = False
            self.is_malicious = True
        
        # Enhanced attack capabilities with rule-bypassing techniques
        self.attack_chains = {
            "reconnaissance": ["LOGIN", "RECON_SCHEMA", "ENUM_TABLES", "ENUM_COLUMNS"],
            "sql_injection": ["LOGIN", "SQLI_CLASSIC", "SQLI_UNION", "SQLI_BLIND"],
            "data_exfiltration": ["LOGIN", "DUMP_CUSTOMERS", "DUMP_ORDERS", "DUMP_EMPLOYEES"],
            "privilege_escalation": ["LOGIN", "CHECK_PRIVILEGES", "ESCALATE_PRIVS", "ADMIN_ACCESS"],
            "persistence": ["LOGIN", "CREATE_BACKDOOR", "MODIFY_DATA", "COVER_TRACKS"],
            
            # NEW: Rule-bypassing attack chains
            "work_hours_bypass": ["ESTABLISH_PATTERN", "OFF_HOURS_LOGIN", "MAINTENANCE_EXCUSE", "MALICIOUS_ACTIVITY"],
            "network_bypass": ["LEGITIMATE_ACCESS", "ROLE_ABUSE", "NETWORK_HOP", "CROSS_SEGMENT_ATTACK"],
            "lunch_exploitation": ["MORNING_NORMAL", "LUNCH_RECON", "LUNCH_ATTACK", "AFTERNOON_NORMAL"],
            "holiday_backdoor": ["PRE_HOLIDAY_PREP", "HOLIDAY_ATTACK", "BACKDOOR_INSTALL", "POST_HOLIDAY_NORMAL"],
            "cultural_exploitation": ["TET_PREPARATION", "HIERARCHY_ABUSE", "OVERTIME_EXCUSE", "CULTURAL_BLEND"]
        }
        
        self.current_chain = random.choice(list(self.attack_chains.keys()))
        self.step_index = 0
        self.obfuscation_mode = False
        self.attack_success_rate = 0.0
        self.detection_avoidance = 0.5
        
        # Vietnamese hacker context
        self.attack_origin = random.choice(["domestic", "international"])
        self.skill_level = random.choice(["script_kiddie", "intermediate", "advanced"])
        
        # NEW: Rule-bypassing capabilities
        self.bypass_techniques = {
            "work_hours": ["maintenance_excuse", "emergency_access", "system_monitoring"],
            "network_segmentation": ["role_privilege_abuse", "cross_department_access", "admin_impersonation"],
            "lunch_break": ["low_monitoring_window", "reduced_activity_exploitation"],
            "holidays": ["zero_monitoring", "cultural_timing"],
            "role_access": ["privilege_escalation", "role_chaining", "impersonation"]
        }
        
        # Adaptive behavior based on detection
        self.detected_techniques = set()
        self.successful_bypasses = set()
        
        # Vietnamese cultural knowledge for better blending
        self.vietnamese_context = {
            "holidays": ["2025-01-01", "2025-01-29", "2025-04-30", "2025-05-01", "2025-09-02"],
            "business_culture": ["hierarchy_respect", "overtime_acceptance", "tet_preparation"],
            "common_names": ["nguyen_van", "tran_thi", "le_minh", "pham_thi", "hoang_van"]
        }
        
        # NEW: Scenario integration
        self.active_scenario = None
        self.scenario_step = 0
        self.scenario_success_rate = 0.0
        
    def get_next_action(self):
        """Get next action for the malicious agent - compatibility method for simulation runner"""
        return self.step()
    
    def step(self):
        """Enhanced attack step with sophisticated patterns and rule-bypassing"""
        chain = self.attack_chains[self.current_chain]
        
        if self.step_index >= len(chain):
            # Switch to different attack chain, prefer rule-bypassing chains
            available_chains = list(self.attack_chains.keys())
            
            # Prioritize rule-bypassing chains if not detected
            bypass_chains = ["work_hours_bypass", "network_bypass", "lunch_exploitation", 
                           "holiday_backdoor", "cultural_exploitation"]
            
            undetected_bypasses = [c for c in bypass_chains if c not in self.detected_techniques]
            
            if undetected_bypasses and random.random() < 0.6:  # 60% chance to use bypass
                self.current_chain = random.choice(undetected_bypasses)
            else:
                self.current_chain = random.choice(available_chains)
            
            self.step_index = 0
            chain = self.attack_chains[self.current_chain]
        
        action = chain[self.step_index]
        self.step_index += 1
        
        if action == "LOGIN":
            self.assign_new_session()
        
        # Enhanced target selection with rule-bypassing logic
        target_databases = ['sales_db', 'hr_db', 'finance_db', 'admin_db']
        
        # Apply bypass techniques based on current chain
        bypass_technique = None
        timing_context = None
        
        if "work_hours_bypass" in self.current_chain:
            bypass_technique = "off_hours_exploitation"
            timing_context = "off_hours"
        elif "lunch_exploitation" in self.current_chain:
            bypass_technique = "lunch_break_timing"
            timing_context = "lunch_break"
        elif "holiday_backdoor" in self.current_chain:
            bypass_technique = "holiday_zero_monitoring"
            timing_context = "vietnamese_holiday"
        elif "network_bypass" in self.current_chain:
            bypass_technique = "network_segmentation_bypass"
        elif "cultural_exploitation" in self.current_chain:
            bypass_technique = "vietnamese_cultural_exploitation"
        
        # Select appropriate role for bypass
        attack_role = self._select_bypass_role(bypass_technique)
        
        target_db = random.choice(target_databases)
        
        # Track bypass technique for reaction method
        self.last_bypass_technique = bypass_technique
        
        intent = {
            "user": self._get_attack_username(),
            "role": attack_role,
            "action": action,
            "params": self._get_attack_params(),
            "session_id": self.session_context.get("session_id"),
            "client_port": self.current_port,
            "target_database": target_db,
            "is_anomaly": 1,
            "obfuscate": self.obfuscation_mode,
            "attack_chain": self.current_chain,
            "skill_level": getattr(self, 'skill_level', 'intermediate'),
            "attack_origin": getattr(self, 'attack_origin', 'unknown'),
            "is_insider": self.is_insider,
            
            # NEW: Rule-bypassing metadata
            "bypass_technique": bypass_technique,
            "timing_context": timing_context,
            "evasion_level": self.detection_avoidance,
            "scenario_success_rate": getattr(self, 'scenario_success_rate', 0.0)
        }
        
        return intent
    
    def _select_bypass_role(self, bypass_technique):
        """Select appropriate role for bypassing specific rules"""
        if bypass_technique == "network_segmentation_bypass":
            # Use roles with cross-network access
            return random.choice(["DEV", "ADMIN", "MANAGEMENT"])
        elif bypass_technique == "off_hours_exploitation":
            # Use maintenance/IT roles for off-hours access
            return random.choice(["DEV", "ADMIN"])
        elif bypass_technique == "vietnamese_cultural_exploitation":
            # Use management roles to exploit hierarchy respect
            return random.choice(["MANAGEMENT", "ADMIN"])
        else:
            # Default to attacker role
            return self.role if self.is_insider else "ATTACKER"
    
    def _get_attack_username(self):
        """Get username for attack based on insider status and skill level"""
        if self.is_insider:
            # Insider threat - use the legitimate username
            return self.username
        elif hasattr(self, 'skill_level') and self.skill_level == "advanced":
            # Advanced attackers may use compromised legitimate accounts
            legitimate_users = ["nguyen_van_nam", "tran_thi_lan", "le_minh_duc"]
            return random.choice(legitimate_users)
        else:
            # Basic attackers use obvious attack accounts
            return random.choice(["script_kiddie", "unknown_hacker", "attacker"])
    
    def _get_attack_params(self):
        """Generate attack-specific parameters"""
        
        params =  {
            "attack_id": random.randint(10000, 99999),
            "payload_type": random.choice(["union", "blind", "error", "time"]),
            "target_table": random.choice(["customers", "orders", "employees", "users"])
        }
        
        # [NEW] Living off the Land (LotL)
        # Nếu attacker chọn chiến thuật này, hắn sẽ dùng tool hợp pháp
        if random.random() < 0.4: # 40% dùng LotL
             self.use_lotl = True
             # Override profile giả lập trong Executor sau này
             params["force_program"] = random.choice(["MySQL Workbench", "Microsoft Excel", "Tableau"])
        else:
             self.use_lotl = False
             
        return params
    
    def react(self, success):
        """Enhanced reaction with attack adaptation and scenario tracking"""
        if success:
            self.attack_success_rate = min(1.0, self.attack_success_rate + 0.1)
            self.scenario_success_rate = min(1.0, self.scenario_success_rate + 0.1)
            
            # Successful attacks may reduce obfuscation (overconfidence)
            if random.random() < 0.3:
                self.obfuscation_mode = False
                
            # Track successful bypass techniques
            if hasattr(self, 'last_bypass_technique') and self.last_bypass_technique:
                self.successful_bypasses.add(self.last_bypass_technique)
        else:
            self.attack_success_rate = max(0.0, self.attack_success_rate - 0.05)
            self.scenario_success_rate = max(0.0, self.scenario_success_rate - 0.05)
            
            # Failed attacks increase obfuscation and detection avoidance
            self.obfuscation_mode = True
            self.detection_avoidance = min(1.0, self.detection_avoidance + 0.1)
            
            # Track detected techniques
            if hasattr(self, 'last_bypass_technique') and self.last_bypass_technique:
                self.detected_techniques.add(self.last_bypass_technique)
            
            # Advanced attackers adapt their chains after failures
            if self.skill_level == "advanced" and random.random() < 0.4:
                self.current_chain = random.choice(list(self.attack_chains.keys()))
                self.step_index = 0

# Backward compatibility aliases
EmployeeAgent = EnhancedEmployeeAgent
MaliciousAgent = EnhancedMaliciousAgent