# Enhanced Vietnamese Enterprise Simulation - Complete Workflow Guide

## 🎯 **NEW ENHANCED WORKFLOW (Ready to Use)**

### **Step 1: Setup Enhanced Database Structure**
```bash
cd MA-sim
python setup_enhanced_vietnamese_company.py
```
**What this does:**
- Creates 7 specialized databases (sales, inventory, finance, marketing, support, hr, admin)
- Sets up 28 tables with proper relationships
- Configures UTF-8 support for Vietnamese characters
- Creates realistic Vietnamese business structure

### **Step 2: Create Vietnamese Users**
```bash
python create_sandbox_user.py
```
**What this does:**
- Creates 103 Vietnamese users with authentic names
- Uses underscore format (nguyen_van_nam)
- Sets up 8 departments with realistic distribution
- Configures role-based database permissions

### **Step 3: Run Enhanced Simulation**
```bash
python main_execution_enhanced.py [scenario]
```
**Available scenarios:**
- `clean` - 0% anomalies (pure normal behavior)
- `minimal` - 2% anomalies (very secure environment)
- `balanced` - 10% anomalies (realistic enterprise) **[DEFAULT]**
- `high_threat` - 25% anomalies (high-risk environment)
- `attack_simulation` - 50% anomalies (security testing)

**Examples:**
```bash
python main_execution_enhanced.py                    # Default balanced scenario
python main_execution_enhanced.py clean             # Clean dataset
python main_execution_enhanced.py attack_simulation # Heavy security testing
```

## 📊 **What You Get with Enhanced System**

### **Database Coverage**
- **7 databases** vs 3 in original system
- **28 tables** with realistic business relationships
- **273+ SQL queries** vs basic templates in original

### **Vietnamese Business Context**
- **103 authentic Vietnamese users** (Nguyễn Văn Nam, Trần Thị Hương, etc.)
- **8 departments** matching Vietnamese company structure
- **Realistic work patterns** based on Vietnamese business hours
- **Cultural context** in query patterns and user behavior

### **Enhanced Query Library**
- **Sales Operations**: Customer management, order processing, revenue analysis
- **Inventory Management**: Stock tracking, warehouse operations, adjustments
- **Financial Operations**: Invoice management, expense reporting, budget analysis
- **Marketing Activities**: Campaign management, lead tracking, ROI analysis
- **Customer Support**: Ticket management, response tracking, knowledge base
- **HR Operations**: Employee management, payroll, attendance tracking
- **System Administration**: Log analysis, session monitoring, reporting

### **Security Testing Capabilities**
- **4 attack vector categories**: SQL injection, privilege escalation, data exfiltration, reconnaissance
- **Sophisticated attackers**: Multiple skill levels and attack chains
- **Insider threats**: Configurable percentage of employees become malicious
- **Obfuscation patterns**: Realistic attack concealment techniques

## 🔧 **Configuration Options**

### **Performance Settings (Preserved from Original)**
```python
NUM_THREADS = 10           # Number of concurrent users
SIMULATION_SPEED_UP = 3600 # 1 day simulation per 1 second real time
TOTAL_REAL_SECONDS = 600   # Run for 10 minutes real time
```

### **Anomaly Configuration**
```python
ANOMALY_PERCENTAGE = 0.10          # 10% of queries are anomalous
INSIDER_THREAT_PERCENTAGE = 0.05   # 5% of employees become insider threats
EXTERNAL_HACKER_COUNT = 3          # Number of external attackers
ENABLE_OBFUSCATION = True          # Enable SQL obfuscation for attackers
```

### **Database Access Patterns**
Each role has realistic database access:
- **SALES**: sales_db, marketing_db, support_db
- **MARKETING**: marketing_db, sales_db, support_db
- **CUSTOMER_SERVICE**: support_db, sales_db, marketing_db
- **HR**: hr_db, finance_db, admin_db
- **FINANCE**: finance_db, sales_db, hr_db, inventory_db
- **DEV/ADMIN**: All databases (full access)
- **MANAGEMENT**: All databases (oversight access)

## 📈 **Performance Comparison**

| Aspect | Original System | Enhanced System | Improvement |
|--------|----------------|-----------------|-------------|
| **Databases** | 3 | 7 | +133% |
| **Tables** | ~10 | 28 | +180% |
| **SQL Queries** | ~50 built-in | 273+ library | +446% |
| **User Names** | Generic | Vietnamese authentic | 100% localized |
| **Business Logic** | Basic | Enterprise-grade | Realistic |
| **Security Testing** | Limited | Comprehensive | Multi-vector |

## 🚀 **Quick Start Commands**

### **Full Setup (First Time)**
```bash
# 1. Setup enhanced databases
python setup_enhanced_vietnamese_company.py

# 2. Create Vietnamese users  
python create_sandbox_user.py

# 3. Run simulation
python main_execution_enhanced.py balanced
```

### **Daily Usage (After Setup)**
```bash
# Just run the simulation with different scenarios
python main_execution_enhanced.py clean             # Clean data
python main_execution_enhanced.py balanced          # Normal operations
python main_execution_enhanced.py high_threat       # Security testing
```

## 🔍 **Monitoring and Output**

### **Real-time Monitoring**
The enhanced simulation provides real-time feedback:
```
⚡ Queries: 1,247 | Sim Time: 2025-01-01 14:23 | Rate: 12.4/s
[2025-01-01T14:23:15] nguyen_van_nam (SALES) | sales_db | SEARCH_CUSTOMER -> OK
[2025-01-01T14:23:16] tran_thi_lan (MARKETING) | marketing_db | VIEW_CAMPAIGN -> OK
[2025-01-01T14:23:17] unknown_hacker (ATTACKER) | admin_db | SQLI_CLASSIC -> FAIL
```

### **Enhanced Logging**
- **User context**: Vietnamese names and departments
- **Database context**: Which of the 7 databases was accessed
- **Role context**: Department and access level
- **Attack context**: Attack type and sophistication level
- **Performance metrics**: Query rate and success statistics

## 🛠️ **Troubleshooting**

### **Common Issues**

**1. Database Connection Errors**
```bash
# Make sure databases are created first
python setup_enhanced_vietnamese_company.py
```

**2. User Permission Errors**
```bash
# Recreate users with proper permissions
python create_sandbox_user.py
```

**3. Import Errors**
```bash
# Make sure you're in the MA-sim directory
cd MA-sim
python main_execution_enhanced.py
```

### **Verification Commands**
```bash
# Test enhanced database structure
python test_enhanced_database_structure.py

# Test Vietnamese users
python test_vietnamese_company.py

# Test query library
python test_enriched_query_library.py

# Test complete integration
python test_enhanced_integration_simple.py
```

## 📋 **File Structure Summary**

### **Core Enhanced Files**
- `main_execution_enhanced.py` - **NEW**: Enhanced simulation engine
- `agents_enhanced.py` - **NEW**: Enhanced agent behavior
- `enriched_sql_library.py` - **NEW**: 273+ business queries
- `setup_enhanced_vietnamese_company.py` - **NEW**: 7-database setup

### **Updated Files**
- `create_sandbox_user.py` - **UPDATED**: Vietnamese users with underscore format

### **Preserved Files**
- `main_execution_mt.py` - **PRESERVED**: Original system (still works)
- `setup_full_environment.py` - **PRESERVED**: Original 3-database setup
- All other core files preserved for backward compatibility

## 🎯 **System Status: FULLY OPERATIONAL** ✅

The enhanced Vietnamese enterprise simulation system is now **completely integrated and tested**. All components are working correctly:

### **✅ Verified Working Features**
- **7-database structure** with realistic Vietnamese business data
- **103 authentic Vietnamese users** with proper underscore format
- **Role-based database access** with realistic permission patterns
- **Flexible anomaly scenarios** (clean, minimal, balanced, high_threat, attack_simulation)
- **Insider threat simulation** with configurable percentages
- **External hacker simulation** with sophisticated attack patterns
- **SQL obfuscation** for realistic attack concealment
- **Enhanced query library** with 273+ business-appropriate queries
- **Performance optimization** maintaining 10+ queries/second throughput

### **🚀 Ready to Use Commands**

**Quick Start (Recommended):**
```bash
cd MA-sim
python main_execution_enhanced.py balanced    # Realistic enterprise scenario
```

**Other Scenarios:**
```bash
python main_execution_enhanced.py clean             # 0% anomalies - pure business
python main_execution_enhanced.py minimal           # 2% anomalies - secure environment  
python main_execution_enhanced.py high_threat       # 25% anomalies - under attack
python main_execution_enhanced.py attack_simulation # 50% anomalies - security testing
```

### **📊 Expected Output**
- **Query Rate**: 10-15 queries/second
- **Vietnamese Users**: Names like `nguyen_van_nam`, `tran_thi_lan`, `le_minh_duc`
- **Database Coverage**: All 7 databases (sales_db, hr_db, finance_db, etc.)
- **Realistic Patterns**: Business hours activity, role-appropriate database access
- **Security Events**: Insider threats and external attacks when configured

The enhanced system is now **production-ready** and provides significantly richer, more realistic Vietnamese enterprise simulation capabilities while maintaining full backward compatibility with your existing workflow!