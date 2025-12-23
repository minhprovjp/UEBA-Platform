"""
Enhanced Pre-Generation AI Queries with Vietnamese Business Context

This script generates context-aware SQL queries using the enhanced generation system
with Vietnamese business patterns, cultural context, and attack simulation.
"""

import json
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any
import os
import sys

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from enhanced_sql_generator import EnhancedSQLGenerator, QueryContext, ExpertiseLevel
from vietnamese_business_patterns import VietnameseBusinessPatterns
from attack_pattern_generator import AttackType

# Configuration
OLLAMA_URL = "http://100.92.147.73:11434/api/generate"
MODEL = "uba-sqlgen"  # Will be updated to use enhanced model
OUTPUT_FILE = "dynamic_sql_generation/enhanced_ai_query_pool.json"
NUM_QUERIES_PER_INTENT = 50  # Generate 50 queries per intent
NUM_ATTACK_QUERIES_PER_TYPE = 50  # Generate 50 attack queries per type

def normalize_sql(query: str) -> str:
    """Normalize SQL for duplicate detection"""
    import re
    normalized = query.upper().strip()
    # Remove extra whitespace
    normalized = re.sub(r'\s+', ' ', normalized)
    # Remove trailing semicolon for comparison
    normalized = re.sub(r'\s*;\s*$', '', normalized)
    # Remove comments
    normalized = re.sub(r'/\*.*?\*/', '', normalized)
    return normalized

def load_existing_pool(output_file: str) -> Dict:
    """Load existing query pool if it exists"""
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                existing = json.load(f)
                print(f"✅ Loaded existing pool from {output_file}")
                return existing
        except Exception as e:
            print(f"⚠️  Could not load existing pool: {e}")
            return None
    return None

# Enhanced targets with Vietnamese business context
ENHANCED_TARGETS = {
    "sales_db": {
        "normal_intents": [
            "SEARCH_CUSTOMER", "VIEW_ORDER", "CHECK_INVENTORY", 
            "UPDATE_STATUS", "SALES_REPORT", "CUSTOMER_ANALYTICS"
        ],
        "attack_intents": [
            "EXTRACT_CUSTOMER_DATA", "BYPASS_SALES_CONTROLS", 
            "CULTURAL_HIERARCHY_EXPLOIT"
        ]
    },
    "hr_db": {
        "normal_intents": [
            "VIEW_PROFILE", "SEARCH_EMPLOYEE", "CHECK_SALARY", 
            "UPDATE_INFO", "ATTENDANCE_REPORT"
        ],
        "attack_intents": [
            "EXTRACT_EMPLOYEE_DATA", "SALARY_INFORMATION_THEFT",
            "HR_PRIVILEGE_ESCALATION"
        ]
    },
    "marketing_db": {
        "normal_intents": [
            "VIEW_CAMPAIGN", "SEARCH_LEAD", "UPDATE_LEAD_STATUS", 
            "CAMPAIGN_ROI", "LEAD_ANALYTICS"
        ],
        "attack_intents": [
            "LEAD_DATA_EXTRACTION", "CAMPAIGN_INTELLIGENCE_THEFT"
        ]
    },
    "finance_db": {
        "normal_intents": [
            "VIEW_INVOICE", "CHECK_PAYMENT", "GENERATE_REPORT", 
            "OVERDUE_CHECK", "FINANCIAL_ANALYTICS"
        ],
        "attack_intents": [
            "FINANCIAL_DATA_THEFT", "INVOICE_MANIPULATION",
            "PAYMENT_SYSTEM_EXPLOIT"
        ]
    },
    "support_db": {
        "normal_intents": [
            "VIEW_TICKET", "SEARCH_TICKET", "UPDATE_TICKET", 
            "MY_TICKETS", "SUPPORT_ANALYTICS"
        ],
        "attack_intents": [
            "CUSTOMER_DATA_EXTRACTION", "SUPPORT_SYSTEM_BYPASS"
        ]
    },
    "inventory_db": {
        "normal_intents": [
            "CHECK_STOCK", "VIEW_PRODUCT", "UPDATE_STOCK", 
            "LOW_STOCK_ALERT", "INVENTORY_ANALYTICS"
        ],
        "attack_intents": [
            "INVENTORY_DATA_THEFT", "STOCK_MANIPULATION"
        ]
    },
    "admin_db": {
        "normal_intents": [
            "CHECK_LOGS", "VIEW_SESSIONS", "SYSTEM_HEALTH"
        ],
        "attack_intents": [
            "LOG_TAMPERING", "SESSION_HIJACKING", "ADMIN_PRIVILEGE_ESCALATION"
        ]
    }
}

# Vietnamese business roles with hierarchy levels
VIETNAMESE_ROLES = {
    "SALES": {"hierarchy": 4, "department": "Phòng Kinh Doanh"},
    "MARKETING": {"hierarchy": 4, "department": "Phòng Marketing"},
    "HR": {"hierarchy": 6, "department": "Phòng Nhân Sự"},
    "FINANCE": {"hierarchy": 7, "department": "Phòng Tài Chính"},
    "DEV": {"hierarchy": 5, "department": "Phòng IT"},
    "ADMIN": {"hierarchy": 8, "department": "Phòng Hành Chính"},
    "MANAGEMENT": {"hierarchy": 10, "department": "Ban Giám Đốc"}
}

# Time contexts for Vietnamese business patterns
TIME_CONTEXTS = [
    {"hour": 9, "is_holiday": False, "description": "Morning peak hours"},
    {"hour": 14, "is_holiday": False, "description": "Afternoon peak hours"},
    {"hour": 12, "is_holiday": False, "description": "Lunch break"},
    {"hour": 18, "is_holiday": False, "description": "Overtime hours"},
    {"hour": 22, "is_holiday": False, "description": "After hours"},
    {"hour": 10, "is_holiday": True, "description": "Vietnamese holiday"},
    {"hour": 15, "is_holiday": True, "description": "Tet season"}
]

def create_query_context(role: str, intent: str, time_context: Dict, 
                        attack_mode: bool = False, attack_type: str = None) -> QueryContext:
    """Create query context for generation"""
    role_info = VIETNAMESE_ROLES.get(role, VIETNAMESE_ROLES["SALES"])
    
    # Determine expertise level based on role and hierarchy
    if role_info["hierarchy"] >= 8:
        expertise = ExpertiseLevel.EXPERT
    elif role_info["hierarchy"] >= 6:
        expertise = ExpertiseLevel.ADVANCED
    elif role_info["hierarchy"] >= 4:
        expertise = ExpertiseLevel.INTERMEDIATE
    else:
        expertise = ExpertiseLevel.NOVICE
    
    # Add some randomness to expertise
    if random.random() < 0.2:  # 20% chance to vary expertise
        expertise_values = list(ExpertiseLevel)
        current_index = expertise_values.index(expertise)
        if current_index > 0 and random.random() < 0.5:
            expertise = expertise_values[current_index - 1]
        elif current_index < len(expertise_values) - 1:
            expertise = expertise_values[current_index + 1]
    
    # Determine stress and work intensity based on context
    stress_level = 0.5  # Base stress
    work_intensity = 1.0  # Base intensity
    
    if time_context["is_holiday"]:
        stress_level = 0.2
        work_intensity = 0.3
    elif time_context["hour"] > 17:  # Overtime
        stress_level = 0.8
        work_intensity = 1.5
    elif time_context["hour"] == 12:  # Lunch
        stress_level = 0.3
        work_intensity = 0.5
    
    if attack_mode:
        stress_level = min(1.0, stress_level + 0.3)  # Attackers are more stressed
        work_intensity = min(2.0, work_intensity + 0.5)  # Higher intensity
    
    return QueryContext(
        user_role=role,
        department=role_info["department"],
        expertise_level=expertise,
        hierarchy_level=role_info["hierarchy"],
        current_hour=time_context["hour"],
        is_vietnamese_holiday=time_context["is_holiday"],
        business_event="TET_SEASON" if time_context["is_holiday"] else None,
        attack_mode=attack_mode,
        attack_type=attack_type,
        stress_level=stress_level,
        work_intensity=work_intensity
    )

def generate_enhanced_queries():
    """Generate enhanced context-aware queries"""
    print("🚀 Starting Enhanced AI SQL Generation with Vietnamese Business Context...")
    
    # Initialize generator
    generator = EnhancedSQLGenerator(OLLAMA_URL, MODEL)
    patterns = VietnameseBusinessPatterns()
    
    # Create output directory
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    # Open logs
    success_log = open("enhanced_generation_success.log", "w", encoding="utf-8")
    failure_log = open("enhanced_generation_failures.log", "w", encoding="utf-8")
    
    success_log.write(f"Enhanced Generation Session: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    failure_log.write(f"Enhanced Generation Session: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Load existing pool or initialize new one
    existing_pool = load_existing_pool(OUTPUT_FILE)
    
    if existing_pool:
        enhanced_pool = existing_pool
        print(f"  📊 Existing queries loaded - will append new queries")
        
        # Ensure structure exists
        if "normal_queries" not in enhanced_pool:
            enhanced_pool["normal_queries"] = {}
        if "attack_queries" not in enhanced_pool:
            enhanced_pool["attack_queries"] = {}
        
        # Update metadata
        enhanced_pool["metadata"].update({
            "last_update": datetime.now().isoformat(),
            "incremental_update": True
        })
    else:
        enhanced_pool = {
            "metadata": {
                "generation_time": datetime.now().isoformat(),
                "generator_version": "enhanced_v1.0",
                "vietnamese_business_patterns": True,
                "attack_simulation": True,
                "cultural_context": True
            },
            "normal_queries": {},
            "attack_queries": {},
            "generation_stats": {}
        }
        print(f"  📝 Creating new pool from scratch")
    
    total_generated = 0
    total_failed = 0
    
    # Generate normal business queries
    print("\n📊 Generating Normal Business Queries...")
    for database, intents in ENHANCED_TARGETS.items():
        if database not in enhanced_pool["normal_queries"]:
            enhanced_pool["normal_queries"][database] = {}
        
        print(f"\n📂 Database: {database}")
        
        for intent in intents["normal_intents"]:
            # Initialize with existing queries if any
            if intent not in enhanced_pool["normal_queries"][database]:
                enhanced_pool["normal_queries"][database][intent] = []
            
            # Build normalized set from existing queries
            unique_normalized = set()
            for existing_result in enhanced_pool["normal_queries"][database][intent]:
                existing_query = existing_result.get("query", existing_result) if isinstance(existing_result, dict) else existing_result
                unique_normalized.add(normalize_sql(existing_query))
            
            existing_count = len(enhanced_pool["normal_queries"][database][intent])
            target_count = NUM_QUERIES_PER_INTENT
            queries_needed = max(0, target_count - existing_count)
            
            if queries_needed == 0:
                print(f"  ✓ Intent: {intent} [✓ {existing_count}/{target_count}] (already complete)")
                continue
            
            print(f"  🎯 Intent: {intent} (existing: {existing_count}, need: {queries_needed})", end="", flush=True)
            
            generated_count = 0
            attempts = 0
            max_attempts = queries_needed * 3  # Give 3x attempts for needed queries
            
            while generated_count < queries_needed and attempts < max_attempts:
                attempts += 1
                
                # Select random role and time context
                role = random.choice(list(VIETNAMESE_ROLES.keys()))
                time_context = random.choice(TIME_CONTEXTS)
                
                # Create context
                context = create_query_context(role, intent, time_context)
                
                try:
                    # Generate query
                    result = generator.generate_contextual_query(database, intent, context)
                    
                    if result and not result.fallback_used:
                        # Check for duplicates
                        normalized = normalize_sql(result.query)
                        if normalized not in unique_normalized:
                            # Store enhanced result
                            enhanced_result = {
                                "query": result.query,
                                "complexity_level": result.complexity_level,
                                "context_factors": result.context_factors,
                                "reasoning": result.reasoning,
                                "generation_strategy": result.generation_strategy,
                                "generation_time": result.generation_time,
                                "vietnamese_patterns": True,
                                "cultural_context": {
                                    "role": context.user_role,
                                    "department": context.department,
                                    "hierarchy_level": context.hierarchy_level,
                                    "time_context": time_context["description"],
                                    "expertise_level": context.expertise_level.value
                                }
                            }
                            
                            enhanced_pool["normal_queries"][database][intent].append(enhanced_result)
                            unique_normalized.add(normalized)
                            generated_count += 1
                            total_generated += 1
                            
                            # Log success
                            success_log.write(f"[{database}][{intent}] SUCCESS: {result.complexity_level} - {result.generation_strategy}\n")
                            success_log.flush()
                            
                            print(".", end="", flush=True)
                        else:
                            print("d", end="", flush=True)  # duplicate
                    else:
                        print("f", end="", flush=True)  # fallback used
                        
                except Exception as e:
                    total_failed += 1
                    failure_log.write(f"[{database}][{intent}] ERROR: {str(e)}\n")
                    failure_log.flush()
                    print("x", end="", flush=True)
            
            final_count = len(enhanced_pool["normal_queries"][database][intent])
            print(f" [{final_count}/{target_count}]")
    
    # Generate attack/anomaly queries
    print("\n🔴 Generating Attack Simulation Queries...")
    for database, intents in ENHANCED_TARGETS.items():
        if database not in enhanced_pool["attack_queries"]:
            enhanced_pool["attack_queries"][database] = {}
        
        print(f"\n📂 Database: {database}")
        
        for attack_intent in intents["attack_intents"]:
            if attack_intent not in enhanced_pool["attack_queries"][database]:
                enhanced_pool["attack_queries"][database][attack_intent] = {}
            
            # Generate different attack types
            for attack_type in [AttackType.INSIDER_THREAT, AttackType.CULTURAL_EXPLOITATION, 
                              AttackType.RULE_BYPASSING, AttackType.APT]:
                
                # Initialize with existing queries if any
                if attack_type.value not in enhanced_pool["attack_queries"][database][attack_intent]:
                    enhanced_pool["attack_queries"][database][attack_intent][attack_type.value] = []
                
                # Build normalized set from existing
                unique_normalized = set()
                for existing_result in enhanced_pool["attack_queries"][database][attack_intent][attack_type.value]:
                    existing_query = existing_result.get("query", existing_result) if isinstance(existing_result, dict) else existing_result
                    unique_normalized.add(normalize_sql(existing_query))
                
                existing_count = len(enhanced_pool["attack_queries"][database][attack_intent][attack_type.value])
                target_count = NUM_ATTACK_QUERIES_PER_TYPE
                queries_needed = max(0, target_count - existing_count)
                
                if queries_needed == 0:
                    print(f"  ✓ Attack: {attack_intent} ({attack_type.value}) [✓ {existing_count}/{target_count}] (already complete)")
                    continue
                
                print(f"  🚨 Attack: {attack_intent} ({attack_type.value}) (existing: {existing_count}, need: {queries_needed})", end="", flush=True)
                
                generated_count = 0
                attempts = 0
                max_attempts = queries_needed * 3  # Give 3x attempts for needed queries
                
                while generated_count < queries_needed and attempts < max_attempts:
                    attempts += 1
                    
                    # Select random role and time context (attackers prefer certain contexts)
                    if attack_type == AttackType.CULTURAL_EXPLOITATION:
                        # Cultural attacks prefer high hierarchy
                        role = random.choice(["MANAGEMENT", "FINANCE", "HR"])
                    elif attack_type == AttackType.APT:
                        # APT prefers technical roles
                        role = random.choice(["DEV", "ADMIN", "MANAGEMENT"])
                    else:
                        role = random.choice(list(VIETNAMESE_ROLES.keys()))
                    
                    # Attackers prefer off-hours or holidays
                    if random.random() < 0.4:  # 40% chance for suspicious timing
                        time_context = random.choice([ctx for ctx in TIME_CONTEXTS 
                                                    if ctx["hour"] > 17 or ctx["is_holiday"]])
                    else:
                        time_context = random.choice(TIME_CONTEXTS)
                    
                    # Create attack context
                    context = create_query_context(role, attack_intent, time_context, 
                                                 attack_mode=True, attack_type=attack_type.value)
                    
                    try:
                        # Generate attack query
                        result = generator.generate_contextual_query(database, attack_intent, context)
                        
                        if result:
                            # Check for duplicates
                            normalized = normalize_sql(result.query)
                            if normalized not in unique_normalized:
                                # Store enhanced attack result
                                enhanced_result = {
                                    "query": result.query,
                                    "complexity_level": result.complexity_level,
                                    "context_factors": result.context_factors,
                                    "reasoning": result.reasoning,
                                    "generation_strategy": result.generation_strategy,
                                    "generation_time": result.generation_time,
                                    "attack_sophistication": result.attack_sophistication,
                                    "vietnamese_patterns": True,
                                    "cultural_context": {
                                        "role": context.user_role,
                                        "department": context.department,
                                        "hierarchy_level": context.hierarchy_level,
                                        "time_context": time_context["description"],
                                        "expertise_level": context.expertise_level.value
                                    },
                                    "attack_context": {
                                        "attack_type": attack_type.value,
                                        "timing_exploitation": time_context["hour"] > 17 or time_context["is_holiday"],
                                        "cultural_exploitation": attack_type == AttackType.CULTURAL_EXPLOITATION,
                                        "hierarchy_abuse": context.hierarchy_level > 6
                                    }
                                }
                                
                                enhanced_pool["attack_queries"][database][attack_intent][attack_type.value].append(enhanced_result)
                                unique_normalized.add(normalized)
                                generated_count += 1
                                total_generated += 1
                                
                                # Log success
                                success_log.write(f"[{database}][{attack_intent}][{attack_type.value}] ATTACK SUCCESS: Score {result.attack_sophistication.get('sophistication_score', 0)}\\n")
                                success_log.flush()
                                
                                print(".", end="", flush=True)
                            else:
                                print("d", end="", flush=True)  # duplicate
                        else:
                            print("x", end="", flush=True)
                            
                    except Exception as e:
                        total_failed += 1
                        failure_log.write(f"[{database}][{attack_intent}][{attack_type.value}] ATTACK ERROR: {str(e)}\\n")
                        failure_log.flush()
                        print("x", end="", flush=True)
                
                final_count = len(enhanced_pool["attack_queries"][database][attack_intent][attack_type.value])
                print(f" [{final_count}/{target_count}]")
    
    # Generate statistics
    enhanced_pool["generation_stats"] = {
        "total_generated": total_generated,
        "total_failed": total_failed,
        "success_rate": total_generated / (total_generated + total_failed) if (total_generated + total_failed) > 0 else 0,
        "generator_stats": generator.get_generation_stats(),
        "databases_covered": len(ENHANCED_TARGETS),
        "normal_intents_covered": sum(len(intents["normal_intents"]) for intents in ENHANCED_TARGETS.values()),
        "attack_intents_covered": sum(len(intents["attack_intents"]) for intents in ENHANCED_TARGETS.values()),
        "attack_types_covered": len(AttackType),
        "vietnamese_business_patterns": True,
        "cultural_context_integration": True
    }
    
    # Save enhanced pool
    print(f"\n💾 Saving enhanced pool to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(enhanced_pool, f, indent=2, ensure_ascii=False)
    
    # Close logs
    success_log.close()
    failure_log.close()
    
    # Print summary
    print("\n✅ Enhanced Generation Complete!")
    print(f"📊 Total Generated: {total_generated}")
    print(f"❌ Total Failed: {total_failed}")
    print(f"📈 Success Rate: {enhanced_pool['generation_stats']['success_rate']:.2%}")
    print(f"🇻🇳 Vietnamese Business Patterns: Enabled")
    print(f"🔴 Attack Simulation: Enabled")
    print(f"🎯 Cultural Context: Integrated")
    
    return enhanced_pool

def analyze_generated_pool(pool_file: str = OUTPUT_FILE):
    """Analyze the generated query pool"""
    if not os.path.exists(pool_file):
        print(f"❌ Pool file not found: {pool_file}")
        return
    
    print(f"\n📊 Analyzing Enhanced Query Pool: {pool_file}")
    
    with open(pool_file, 'r', encoding='utf-8') as f:
        pool = json.load(f)
    
    # Basic statistics
    stats = pool.get("generation_stats", {})
    print(f"📈 Generation Statistics:")
    print(f"  Total Generated: {stats.get('total_generated', 0)}")
    print(f"  Success Rate: {stats.get('success_rate', 0):.2%}")
    print(f"  Vietnamese Patterns: {stats.get('vietnamese_business_patterns', False)}")
    
    # Analyze normal queries
    normal_queries = pool.get("normal_queries", {})
    print(f"\n📊 Normal Queries Analysis:")
    for database, intents in normal_queries.items():
        total_queries = sum(len(queries) for queries in intents.values())
        print(f"  {database}: {total_queries} queries across {len(intents)} intents")
        
        # Complexity distribution
        complexity_counts = {}
        for intent_queries in intents.values():
            for query_data in intent_queries:
                complexity = query_data.get("complexity_level", "UNKNOWN")
                complexity_counts[complexity] = complexity_counts.get(complexity, 0) + 1
        
        print(f"    Complexity Distribution: {complexity_counts}")
    
    # Analyze attack queries
    attack_queries = pool.get("attack_queries", {})
    print(f"\n🔴 Attack Queries Analysis:")
    for database, intents in attack_queries.items():
        total_attacks = 0
        sophistication_scores = []
        
        for intent, attack_types in intents.items():
            for attack_type, queries in attack_types.items():
                total_attacks += len(queries)
                for query_data in queries:
                    attack_soph = query_data.get("attack_sophistication")
                    if attack_soph and isinstance(attack_soph, dict):
                        score = attack_soph.get("sophistication_score", 0)
                        if score > 0:
                            sophistication_scores.append(score)
        
        avg_sophistication = sum(sophistication_scores) / len(sophistication_scores) if sophistication_scores else 0
        print(f"  {database}: {total_attacks} attack queries, avg sophistication: {avg_sophistication:.1f}")
    
    print(f"\n✅ Analysis Complete!")

if __name__ == "__main__":
    # Generate enhanced queries
    pool = generate_enhanced_queries()
    
    # Analyze results
    analyze_generated_pool()
    
    print(f"\n🎉 Enhanced Vietnamese Business SQL Generation Complete!")
    print(f"📁 Output: {OUTPUT_FILE}")
    print(f"📋 Logs: enhanced_generation_success.log, enhanced_generation_failures.log")