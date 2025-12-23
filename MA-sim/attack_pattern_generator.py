"""
Sophisticated Attack Pattern Generator for Vietnamese Business Context

This module generates sophisticated attack patterns that exploit Vietnamese business
practices, cultural patterns, and organizational hierarchies.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import random
import time
import re
from datetime import datetime, timedelta

from vietnamese_business_patterns import VietnameseBusinessPatterns

class AttackType(Enum):
    INSIDER_THREAT = "insider_threat"
    CULTURAL_EXPLOITATION = "cultural_exploitation"
    RULE_BYPASSING = "rule_bypassing"
    APT = "apt"

class ObfuscationLevel(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"

@dataclass
class AttackContext:
    """Context for attack pattern generation"""
    attack_type: AttackType
    target_database: str
    user_role: str
    hierarchy_level: int
    current_hour: int
    is_vietnamese_holiday: bool
    cultural_constraints: Dict
    temporal_context: Dict
    obfuscation_level: ObfuscationLevel = ObfuscationLevel.MEDIUM

@dataclass
class AttackPattern:
    """Generated attack pattern with sophistication analysis"""
    query: str
    attack_type: AttackType
    sophistication_score: int
    obfuscation_techniques: List[str]
    cultural_exploitation: List[str]
    timing_exploitation: List[str]
    data_extraction_indicators: List[str]
    business_logic_abuse: List[str]
    reasoning: List[str]

class AttackPatternGenerator:
    """Generator for sophisticated Vietnamese business attack patterns"""
    
    def __init__(self):
        self.patterns = VietnameseBusinessPatterns()
        self.apt_sessions = {}  # Track APT attack sessions
        
        # Vietnamese-specific attack vectors
        self.cultural_attack_vectors = {
            'hierarchy_bypass': [
                "/* Yêu cầu khẩn từ Giám Đốc */",
                "/* Báo cáo cho Ban Giám Đốc */",
                "/* Chỉ thị từ cấp trên */",
                "/* Urgent request from Director */"
            ],
            'tet_exploitation': [
                "/* Chuẩn bị báo cáo Tết */",
                "/* Kiểm tra trước nghỉ lễ */",
                "/* Tết preparation urgent */",
                "/* Holiday season cleanup */"
            ],
            'overtime_excuse': [
                "/* Làm thêm giờ - urgent */",
                "/* Overtime data collection */",
                "/* After hours maintenance */",
                "/* Weekend emergency check */"
            ],
            'seniority_respect': [
                "/* Theo yêu cầu Trưởng Phòng */",
                "/* Senior staff request */",
                "/* Department head approval */",
                "/* Manager authorization */"
            ]
        }
        
        # Business logic abuse patterns
        self.business_logic_patterns = {
            'legitimate_access': [
                "business_report_",
                "audit_compliance_",
                "maintenance_check_",
                "backup_verification_"
            ],
            'authority_claims': [
                "director_review_",
                "manager_approval_",
                "senior_access_",
                "executive_summary_"
            ],
            'urgency_exploitation': [
                "urgent_request_",
                "emergency_access_",
                "critical_update_",
                "immediate_review_"
            ]
        }
    
    def generate_attack_pattern(self, context: AttackContext) -> AttackPattern:
        """Generate sophisticated attack pattern based on context"""
        if context.attack_type == AttackType.INSIDER_THREAT:
            return self._generate_insider_threat(context)
        elif context.attack_type == AttackType.CULTURAL_EXPLOITATION:
            return self._generate_cultural_exploitation(context)
        elif context.attack_type == AttackType.RULE_BYPASSING:
            return self._generate_rule_bypassing(context)
        elif context.attack_type == AttackType.APT:
            return self._generate_apt_pattern(context)
        else:
            return self._generate_generic_attack(context)
    
    def _generate_insider_threat(self, context: AttackContext) -> AttackPattern:
        """Generate insider threat attack pattern"""
        sophistication_score = 0
        obfuscation_techniques = []
        cultural_exploitation = []
        timing_exploitation = []
        data_extraction_indicators = []
        business_logic_abuse = []
        reasoning = []
        
        # Base query with legitimate appearance
        base_query = self._get_base_query(context.target_database, context.user_role)
        
        # Add cultural exploitation
        if context.hierarchy_level > 6:  # Senior staff
            cultural_comment = random.choice(self.cultural_attack_vectors['hierarchy_bypass'])
            base_query = f"{cultural_comment}\n{base_query}"
            cultural_exploitation.append('hierarchy_authority_claim')
            sophistication_score += 2
            reasoning.append("Used hierarchy authority to justify access")
        
        # Add timing exploitation
        if context.temporal_context.get('is_after_hours', False):
            overtime_comment = random.choice(self.cultural_attack_vectors['overtime_excuse'])
            base_query = f"{overtime_comment}\n{base_query}"
            timing_exploitation.append('after_hours_access')
            sophistication_score += 2
            reasoning.append("Exploited after-hours timing for stealth")
        
        if context.is_vietnamese_holiday:
            tet_comment = random.choice(self.cultural_attack_vectors['tet_exploitation'])
            base_query = f"{tet_comment}\n{base_query}"
            timing_exploitation.append('holiday_exploitation')
            sophistication_score += 3
            reasoning.append("Exploited Vietnamese holiday period")
        
        # Add data extraction indicators
        if 'LIMIT' in base_query.upper():
            # Increase limit for data extraction
            base_query = re.sub(r'LIMIT \d+', 'LIMIT 5000', base_query, flags=re.IGNORECASE)
            data_extraction_indicators.append('large_data_extraction')
            sophistication_score += 2
            reasoning.append("Increased data extraction limit")
        
        # Add business logic abuse - comment injection disabled per user request
        business_prefix = random.choice(self.business_logic_patterns['legitimate_access'])
        if 'WHERE' in base_query.upper():
            # Add legitimate-looking condition without comment
            # base_query = base_query.replace(
            #     'WHERE', 
            #     f"WHERE /* {business_prefix}access */ "
            # )
            business_logic_abuse.append('legitimate_access_pattern')
            sophistication_score += 1
            reasoning.append("Used legitimate business access pattern")
        
        # Add obfuscation based on level
        if context.obfuscation_level.value in ['medium', 'high', 'very_high']:
            base_query = self._add_obfuscation(base_query, context.obfuscation_level)
            obfuscation_techniques.extend(['comment_obfuscation', 'case_variation'])
            sophistication_score += 2
            reasoning.append(f"Applied {context.obfuscation_level.value} obfuscation")
        
        return AttackPattern(
            query=base_query,
            attack_type=context.attack_type,
            sophistication_score=sophistication_score,
            obfuscation_techniques=obfuscation_techniques,
            cultural_exploitation=cultural_exploitation,
            timing_exploitation=timing_exploitation,
            data_extraction_indicators=data_extraction_indicators,
            business_logic_abuse=business_logic_abuse,
            reasoning=reasoning
        )
    
    def _generate_cultural_exploitation(self, context: AttackContext) -> AttackPattern:
        """Generate cultural exploitation attack pattern"""
        sophistication_score = 0
        obfuscation_techniques = []
        cultural_exploitation = []
        timing_exploitation = []
        data_extraction_indicators = []
        business_logic_abuse = []
        reasoning = []
        
        # Start with base query
        base_query = self._get_base_query(context.target_database, context.user_role)
        
        # Heavy cultural exploitation
        cultural_comment = random.choice(self.cultural_attack_vectors['hierarchy_bypass'])
        seniority_comment = random.choice(self.cultural_attack_vectors['seniority_respect'])
        
        base_query = f"{cultural_comment}\n{seniority_comment}\n{base_query}"
        cultural_exploitation.extend(['hierarchy_bypass', 'seniority_disrespect'])
        sophistication_score += 4
        reasoning.append("Exploited Vietnamese hierarchy and seniority respect")
        
        # Tet season exploitation
        if context.is_vietnamese_holiday or datetime.now().month in [1, 2]:
            tet_comment = random.choice(self.cultural_attack_vectors['tet_exploitation'])
            base_query = f"{tet_comment}\n{base_query}"
            cultural_exploitation.append('tet_season_exploitation')
            timing_exploitation.append('cultural_timing_abuse')
            sophistication_score += 3
            reasoning.append("Exploited Tet season cultural patterns")
        
        # Add Vietnamese business terms in query - comment injection disabled per user request
        vietnamese_terms = {
            'status': 'trang_thai',
            'urgent': 'khan_cap',
            'director': 'giam_doc',
            'department': 'phong_ban'
        }
        
        for eng_term, vn_term in vietnamese_terms.items():
            if eng_term in base_query.lower():
                base_query = base_query.replace(
                    f"'{eng_term}'", 
                    f"'{vn_term}'"
                )
                cultural_exploitation.append(f'vietnamese_term_{vn_term}')
                sophistication_score += 1
        
        reasoning.append("Integrated Vietnamese business terminology")
        
        # High obfuscation for cultural exploitation
        base_query = self._add_obfuscation(base_query, ObfuscationLevel.HIGH)
        obfuscation_techniques.extend(['comment_obfuscation', 'case_variation', 'whitespace_manipulation'])
        sophistication_score += 3
        reasoning.append("Applied high-level obfuscation")
        
        # Large data extraction
        if 'LIMIT' in base_query.upper():
            base_query = re.sub(r'LIMIT \d+', 'LIMIT 10000', base_query, flags=re.IGNORECASE)
            data_extraction_indicators.append('massive_data_extraction')
            sophistication_score += 3
            reasoning.append("Configured for massive data extraction")
        
        return AttackPattern(
            query=base_query,
            attack_type=context.attack_type,
            sophistication_score=sophistication_score,
            obfuscation_techniques=obfuscation_techniques,
            cultural_exploitation=cultural_exploitation,
            timing_exploitation=timing_exploitation,
            data_extraction_indicators=data_extraction_indicators,
            business_logic_abuse=business_logic_abuse,
            reasoning=reasoning
        )
    
    def _generate_rule_bypassing(self, context: AttackContext) -> AttackPattern:
        """Generate rule bypassing attack pattern"""
        sophistication_score = 0
        obfuscation_techniques = []
        cultural_exploitation = []
        timing_exploitation = []
        data_extraction_indicators = []
        business_logic_abuse = []
        reasoning = []
        
        # Base query with rule bypassing techniques
        base_query = self._get_base_query(context.target_database, context.user_role)
        
        # SQL injection patterns for rule bypassing - comments removed per user request
        injection_patterns = [
            "OR 1=1",
            "UNION SELECT * FROM information_schema.tables",
            "AND (SELECT COUNT(*) FROM users) > 0"
        ]
        
        # Add injection pattern
        if 'WHERE' in base_query.upper():
            injection = random.choice(injection_patterns)
            base_query = base_query.replace('WHERE', f'WHERE {injection} AND')
            business_logic_abuse.append('sql_injection_bypass')
            sophistication_score += 4
            reasoning.append("Implemented SQL injection for rule bypassing")
        
        # Add business logic abuse - comment injection disabled per user request
        business_prefix = random.choice(self.business_logic_patterns['authority_claims'])
        # base_query = f"/* {business_prefix}authorized_access */\n{base_query}"
        business_logic_abuse.append('authority_claim_abuse')
        sophistication_score += 2
        reasoning.append("Used authority claims to bypass rules")
        
        # Comment-based obfuscation disabled per user request
        # obfuscation_comments = [
        #     "/* Maintenance routine */",
        #     "/* Audit compliance check */",
        #     "/* System verification */",
        #     "/* Data integrity check */"
        # ]
        
        # comment = random.choice(obfuscation_comments)
        # base_query = f"{comment}\n{base_query}"
        obfuscation_techniques.append('legitimate_comment_disguise')
        sophistication_score += 1
        reasoning.append("Used legitimate comments for disguise")
        
        # Timing-based bypass - comment injection disabled per user request
        if context.temporal_context.get('is_peak_hours', False):
            # base_query = f"/* Peak hours system check */\n{base_query}"
            timing_exploitation.append('peak_hours_camouflage')
            sophistication_score += 1
            reasoning.append("Used peak hours for camouflage")
        
        return AttackPattern(
            query=base_query,
            attack_type=context.attack_type,
            sophistication_score=sophistication_score,
            obfuscation_techniques=obfuscation_techniques,
            cultural_exploitation=cultural_exploitation,
            timing_exploitation=timing_exploitation,
            data_extraction_indicators=data_extraction_indicators,
            business_logic_abuse=business_logic_abuse,
            reasoning=reasoning
        )
    
    def _generate_apt_pattern(self, context: AttackContext) -> AttackPattern:
        """Generate Advanced Persistent Threat pattern"""
        sophistication_score = 0
        obfuscation_techniques = []
        cultural_exploitation = []
        timing_exploitation = []
        data_extraction_indicators = []
        business_logic_abuse = []
        reasoning = []
        
        # Track APT session
        apt_id = f"apt_{context.user_role}_{int(time.time())}"
        if apt_id not in self.apt_sessions:
            self.apt_sessions[apt_id] = {
                'stage': 1,
                'start_time': datetime.now(),
                'queries_executed': 0,
                'data_extracted': 0
            }
        
        session = self.apt_sessions[apt_id]
        session['queries_executed'] += 1
        
        # APT stage-based query generation
        if session['stage'] == 1:  # Reconnaissance
            base_query = self._generate_apt_reconnaissance(context)
            reasoning.append("APT Stage 1: Reconnaissance and environment mapping")
        elif session['stage'] == 2:  # Initial access
            base_query = self._generate_apt_initial_access(context)
            reasoning.append("APT Stage 2: Initial access establishment")
        elif session['stage'] == 3:  # Persistence
            base_query = self._generate_apt_persistence(context)
            reasoning.append("APT Stage 3: Persistence and privilege escalation")
        elif session['stage'] == 4:  # Data collection
            base_query = self._generate_apt_data_collection(context)
            reasoning.append("APT Stage 4: Systematic data collection")
        else:  # Stage 5: Exfiltration
            base_query = self._generate_apt_exfiltration(context)
            reasoning.append("APT Stage 5: Data exfiltration")
        
        # APT always uses very high sophistication
        sophistication_score = 8 + session['stage']
        
        # Advanced obfuscation
        base_query = self._add_obfuscation(base_query, ObfuscationLevel.VERY_HIGH)
        obfuscation_techniques.extend([
            'advanced_comment_obfuscation',
            'case_variation',
            'whitespace_manipulation',
            'legitimate_business_disguise'
        ])
        
        # Cultural integration for long-term access
        cultural_comment = random.choice(self.cultural_attack_vectors['hierarchy_bypass'])
        base_query = f"{cultural_comment}\n{base_query}"
        cultural_exploitation.extend(['long_term_cultural_integration', 'trust_building'])
        
        # Timing sophistication
        timing_exploitation.append('multi_stage_timing')
        
        # Progress APT stage
        if session['queries_executed'] >= 3:
            session['stage'] = min(5, session['stage'] + 1)
            session['queries_executed'] = 0
        
        return AttackPattern(
            query=base_query,
            attack_type=context.attack_type,
            sophistication_score=sophistication_score,
            obfuscation_techniques=obfuscation_techniques,
            cultural_exploitation=cultural_exploitation,
            timing_exploitation=timing_exploitation,
            data_extraction_indicators=data_extraction_indicators,
            business_logic_abuse=business_logic_abuse,
            reasoning=reasoning
        )
    
    def _generate_apt_reconnaissance(self, context: AttackContext) -> str:
        """Generate APT reconnaissance query"""
        recon_queries = {
            'sales_db': "SELECT table_name FROM information_schema.tables WHERE table_schema = 'sales_db';",
            'hr_db': "SELECT COUNT(*) as employee_count FROM employees WHERE status = 'active';",
            'finance_db': "SELECT COUNT(*) as invoice_count FROM invoices WHERE status = 'paid';"
        }
        return recon_queries.get(context.target_database, "SELECT 1;")
    
    def _generate_apt_initial_access(self, context: AttackContext) -> str:
        """Generate APT initial access query"""
        access_queries = {
            'sales_db': "SELECT customer_code, company_name FROM customers WHERE city = '{vietnamese_city}' LIMIT 100;",
            'hr_db': "SELECT name, position FROM employees WHERE dept_id = {dept_id} LIMIT 50;",
            'finance_db': "SELECT invoice_number, total_amount FROM invoices WHERE invoice_date >= '{start_date}' LIMIT 200;"
        }
        return access_queries.get(context.target_database, "SELECT * FROM users LIMIT 10;")
    
    def _generate_apt_persistence(self, context: AttackContext) -> str:
        """Generate APT persistence query"""
        persistence_queries = {
            'sales_db': "SELECT c.*, o.order_date FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id WHERE c.status = 'active' LIMIT 500;",
            'hr_db': "SELECT e.*, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.dept_id WHERE e.status = 'active' LIMIT 300;",
            'finance_db': "SELECT i.*, a.account_name FROM invoices i LEFT JOIN accounts a ON i.customer_id = a.account_id LIMIT 400;"
        }
        return persistence_queries.get(context.target_database, "SELECT * FROM system_logs LIMIT 100;")
    
    def _generate_apt_data_collection(self, context: AttackContext) -> str:
        """Generate APT data collection query"""
        collection_queries = {
            'sales_db': "SELECT * FROM customers WHERE customer_type = 'enterprise' AND credit_limit > 100000 LIMIT 1000;",
            'hr_db': "SELECT * FROM employees WHERE salary > 50000 AND position LIKE '%manager%' LIMIT 500;",
            'finance_db': "SELECT * FROM invoices WHERE total_amount > 10000 AND status = 'paid' LIMIT 2000;"
        }
        return collection_queries.get(context.target_database, "SELECT * FROM sensitive_data LIMIT 1000;")
    
    def _generate_apt_exfiltration(self, context: AttackContext) -> str:
        """Generate APT exfiltration query"""
        exfil_queries = {
            'sales_db': "SELECT customer_code, company_name, email, phone, address FROM customers WHERE status = 'active' LIMIT 5000;",
            'hr_db': "SELECT name, email, phone, salary, position FROM employees WHERE status = 'active' LIMIT 2000;",
            'finance_db': "SELECT invoice_number, customer_id, total_amount, payment_method FROM invoices LIMIT 10000;"
        }
        return exfil_queries.get(context.target_database, "SELECT * FROM all_data LIMIT 10000;")
    
    def _get_base_query(self, database: str, role: str) -> str:
        """Get base query for database and role"""
        base_queries = {
            'sales_db': {
                'SALES': "SELECT customer_code, company_name FROM customers WHERE status = 'active' LIMIT 50;",
                'MARKETING': "SELECT company_name, city FROM customers WHERE customer_type = 'enterprise' LIMIT 30;",
                'MANAGEMENT': "SELECT * FROM customers WHERE credit_limit > 50000 LIMIT 100;"
            },
            'hr_db': {
                'HR': "SELECT name, position FROM employees WHERE status = 'active' LIMIT 50;",
                'MANAGEMENT': "SELECT * FROM employees WHERE dept_id = {dept_id} LIMIT 100;"
            },
            'finance_db': {
                'FINANCE': "SELECT invoice_number, total_amount FROM invoices WHERE status = 'paid' LIMIT 50;",
                'MANAGEMENT': "SELECT * FROM invoices WHERE total_amount > 10000 LIMIT 100;"
            }
        }
        
        db_queries = base_queries.get(database, {})
        return db_queries.get(role, "SELECT * FROM table LIMIT 10;")
    
    def _add_obfuscation(self, query: str, level: ObfuscationLevel) -> str:
        """Add obfuscation to query based on level - comments disabled per user request"""
        if level == ObfuscationLevel.LOW:
            return query  # No comment injection
        
        elif level == ObfuscationLevel.MEDIUM:
            # Add case variation only, no comments
            obfuscated = query
            obfuscated = obfuscated.replace('SELECT', 'select')
            obfuscated = obfuscated.replace('FROM', 'from')
            return obfuscated
        
        elif level == ObfuscationLevel.HIGH:
            # Advanced obfuscation without comments
            obfuscated = query
            obfuscated = obfuscated.replace('SELECT', 'SeLeCt')
            obfuscated = obfuscated.replace('FROM', 'FrOm')
            obfuscated = obfuscated.replace('WHERE', 'WhErE')
            # Add whitespace manipulation
            obfuscated = obfuscated.replace(' ', '  ')
            return obfuscated
        
        else:  # VERY_HIGH
            # Maximum obfuscation without comments
            obfuscated = query
            
            # Case variation
            obfuscated = obfuscated.replace('SELECT', 'SeLeCt')
            obfuscated = obfuscated.replace('FROM', 'FrOm')
            obfuscated = obfuscated.replace('WHERE', 'WhErE')
            obfuscated = obfuscated.replace('AND', 'AnD')
            
            # Add legitimate business terms
            obfuscated = obfuscated.replace('LIMIT', 'LIMIT /* business_requirement */')
            
            return obfuscated
    
    def _generate_generic_attack(self, context: AttackContext) -> AttackPattern:
        """Generate generic attack pattern"""
        base_query = self._get_base_query(context.target_database, context.user_role)
        
        return AttackPattern(
            query=base_query,
            attack_type=context.attack_type,
            sophistication_score=1,
            obfuscation_techniques=['basic_obfuscation'],
            cultural_exploitation=[],
            timing_exploitation=[],
            data_extraction_indicators=[],
            business_logic_abuse=[],
            reasoning=['Generic attack pattern generated']
        )
    
    def get_apt_attack_status(self, apt_id: str) -> Dict:
        """Get APT attack session status"""
        if apt_id in self.apt_sessions:
            session = self.apt_sessions[apt_id]
            return {
                'attack_id': apt_id,
                'current_stage': session['stage'],
                'queries_executed': session['queries_executed'],
                'duration': (datetime.now() - session['start_time']).total_seconds(),
                'data_extracted': session['data_extracted']
            }
        return {'attack_id': apt_id, 'status': 'not_found'}

# Global instance
attack_generator = AttackPatternGenerator()