# MA-sim: Enhanced Vietnamese Company Simulation

A comprehensive User Behavior Analytics (UBA) simulation system modeling a realistic Vietnamese medium-sized sales company with 97 employees across 7 specialized databases. Features authentic Vietnamese business patterns, network segmentation, and sophisticated rule-bypassing attack scenarios.

## ⚡ Quick Dataset Generation

**Generate a Vietnamese enterprise dataset in 3 commands:**

```bash
# 1. Setup (one-time)
python setup_enhanced_vietnamese_company.py && python create_sandbox_user.py

# 2. Run simulation (5+ minutes for good data)
python main_execution_enhanced.py balanced

# 3. Export CSV dataset
python perf_log_dataset_creator.py
```

**Result**: `final_clean_dataset_30d.csv` with realistic Vietnamese business data including sophisticated rule-bypassing attacks.

## 🚀 Quick Start - Dataset Generation

### Prerequisites
- Python 3.8+
- MySQL 8.0+ running on localhost:3306
- Required packages: `pip install -r requirements.txt`

### Step 1: Database Setup
```bash
# Setup 7-database Vietnamese company structure
python setup_enhanced_vietnamese_company.py

# Create 97 Vietnamese users with proper permissions
python create_sandbox_user.py

# Populate databases with sample data (optional but recommended)
python populate_sample_data.py
```

### Step 2: Run Simulation to Generate Raw Data
```bash
# Run simulation with different threat scenarios
python main_execution_enhanced.py [scenario]
```

**Available Scenarios:**
- `clean` - Pure normal operations (0% anomalies)
- `minimal` - Very secure environment (2% anomalies)  
- `balanced` - Realistic enterprise (10% anomalies) **[DEFAULT]**
- `high_threat` - Active attack scenario (25% anomalies)
- `attack_simulation` - Intensive testing (50% anomalies)

**Example Commands:**
```bash
# Generate balanced dataset (recommended)
python main_execution_enhanced.py balanced

# Generate high-threat dataset with rule-bypassing attacks
python main_execution_enhanced.py high_threat

# Generate clean dataset for baseline
python main_execution_enhanced.py clean
```

### Step 3: Convert Raw Data to CSV Dataset
```bash
# Generate CSV dataset from MySQL performance schema
python perf_log_dataset_creator.py

# This creates: final_clean_dataset_30d.csv
```

### Step 4: Analyze Generated Dataset (Optional)
```bash
# Analyze business hours patterns
python business_hours_analysis.py

# General dataset analysis
python analyze_dataset.py

# Simple statistical analysis
python simple_analyze.py
```

## 📁 Core Files

| File | Purpose |
|------|---------|
| `main_execution_enhanced.py` | Main simulation runner |
| `agents_enhanced.py` | Enhanced agent system with Vietnamese context |
| `executor.py` | Database execution with 7-database support |
| `translator.py` | Intent-to-SQL translation |
| `enhanced_scheduler.py` | Time-based scheduling |
| `enhanced_scenarios.py` | Attack scenario management |
| `setup_enhanced_vietnamese_company.py` | Database setup |
| `create_sandbox_user.py` | User creation |

## 🗄️ Database Architecture

**7 Specialized Databases:**
1. **sales_db** - Customers, orders, products
2. **hr_db** - Employees, payroll, attendance  
3. **inventory_db** - Stock, warehouses, movements
4. **finance_db** - Invoices, payments, expenses
5. **marketing_db** - Campaigns, leads, activities
6. **support_db** - Tickets, responses, knowledge base
7. **admin_db** - System logs, sessions, reports

## 🎯 Attack Scenarios

1. **Insider Salary Theft** - Employee accessing payroll illegally
2. **External Hack Attempt** - Multi-stage SQL injection attack
3. **Sales Snooping** - Cross-department unauthorized access
4. **Privilege Escalation** - Rights elevation attacks
5. **Data Exfiltration** - Systematic data theft
6. **Lateral Movement** - Network traversal
7. **Financial Fraud** - Accounting manipulation
8. **Customer Data Breach** - Personal data theft
9. **Supply Chain Attack** - Inventory manipulation
10. **Social Engineering** - Credential compromise

## 🇻🇳 Vietnamese Business Context

### Work Schedule & Culture
- **Work Hours**: 8:00-18:00 (role-dependent, strict enforcement)
- **Extended Lunch Break**: 11:30-13:30 (flexible Vietnamese style)
  - 11:30-12:00: 40% activity (early lunch)
  - 12:00-13:00: 20% activity (core lunch hour)
  - 13:00-13:30: 30% activity (extended lunch)
- **Weekends**: Absolutely no activity for normal employees
- **Holidays**: Vietnamese national holidays (Tet, Liberation Day, etc.)

### Business Environment
- **Currency**: Vietnamese Dong (VND)
- **Names**: Authentic Vietnamese employee names
- **Companies**: Vietnamese business naming conventions
- **Network Segmentation**: Department-based IP ranges
- **Compliance**: Vietnamese data protection standards

## 📊 Simulation Features

### Core Capabilities
- **97 Vietnamese Employees** across 8 departments
- **Role-based Database Access** with realistic permissions
- **Strict Vietnamese Work Hours** enforcement (8AM-6PM weekdays only)
- **Extended Lunch Break Patterns** (11:30AM-1:30PM flexible)
- **Network Segmentation** by department with realistic IP ranges
- **Sophisticated Attack Chains** with multi-step scenarios
- **Real-time Statistics** and monitoring
- **Configurable Anomaly Rates** for different testing scenarios

### Network Architecture
- **Sales**: `192.168.10.x` - Sales team network
- **Marketing**: `192.168.15.x` - Marketing department
- **HR**: `192.168.20.x` - HR department (sensitive)
- **Customer Service**: `192.168.25.x` - Support team
- **Finance**: `192.168.30.x` - Finance (high security)
- **Management**: `192.168.40.x` - Executive level
- **IT/Dev**: `192.168.50.x` - Development team
- **Admin**: `192.168.60.x` - System administration
- **Attackers**: `10.0.0.x` - External threats

### Recent Improvements ✨
- ✅ **Fixed client_ip column** - Now shows proper IP addresses instead of hostnames
- ✅ **Enhanced lunch break patterns** - Realistic Vietnamese flexible lunch hours
- ✅ **Strict work hours enforcement** - No weekend/holiday activity for normal employees
- ✅ **Network segmentation** - Department-based IP ranges for realistic enterprise simulation
- ✅ **FULLY INTEGRATED enhanced files** - All 6/6 enhanced components now active
- ✅ **Structured attack scenarios** - 10 sophisticated rule-bypassing attack patterns
- ✅ **Advanced scheduling system** - Vietnamese business hour management
- ✅ **APT-level malicious agents** - Advanced persistent threats with cultural knowledge
- ✅ **Vietnamese cultural exploitation** - Attacks that exploit local business practices
- ✅ **Time-based evasion** - Sophisticated timing-based attack patterns

## 🔧 Requirements

- Python 3.8+
- MySQL 8.0+
- Required packages: `pip install -r requirements.txt`

## 📈 Output & Data Quality

### Generated Dataset Features
- **Realistic IP Addresses** - Proper network segmentation by department
- **Vietnamese Work Patterns** - Authentic business hours and lunch breaks
- **High-Quality Anomalies** - Sophisticated attack scenarios
- **Network-based Detection** - Cross-segment traffic analysis
- **Temporal Patterns** - Time-based behavioral analysis

### Use Cases
- **UBA System Training** - Machine learning model development
- **Anomaly Detection** - Algorithm testing and validation
- **Security Research** - Vietnamese enterprise threat modeling
- **Incident Response Training** - Realistic attack scenario practice
- **Compliance Testing** - Vietnamese data protection standards
- **Network Security Analysis** - Department-based access patterns

### Complete Dataset Generation Workflow
```bash
# 1. Setup (one-time only)
python setup_enhanced_vietnamese_company.py
python create_sandbox_user.py
python populate_sample_data.py

# 2. Generate simulation data (run for desired duration)
python main_execution_enhanced.py balanced

# 3. Convert to CSV dataset
python perf_log_dataset_creator.py

# 4. Analyze results
python business_hours_analysis.py
python analyze_dataset.py
```

### Advanced Dataset Generation
```bash
# Generate multiple scenario datasets
python main_execution_enhanced.py clean        # Baseline data
python perf_log_dataset_creator.py            # Export clean dataset

python main_execution_enhanced.py high_threat  # Attack data
python perf_log_dataset_creator.py            # Export attack dataset

# Test rule-bypassing scenarios
python enhanced_scenarios.py                   # Test scenarios
```

## 🔍 Quality Assurance & Advanced Features

### Recent Enhancements (December 2024)
1. **Client IP Column** - Fixed to show proper IP addresses (192.168.x.x) instead of hostnames
2. **Vietnamese Work Hours** - Strict enforcement of 8AM-6PM weekdays only
3. **Extended Lunch Breaks** - Realistic 11:30AM-1:30PM flexible patterns
4. **Network Segmentation** - Department-based IP ranges for enterprise realism
5. **Weekend/Holiday Enforcement** - Zero activity for normal employees on non-work days
6. **Rule-Bypassing Attacks** - 10 sophisticated scenarios that circumvent security controls
7. **Advanced Persistent Threats** - APT-level attackers with Vietnamese cultural knowledge
8. **Time-Based Evasion** - Attacks spread across time windows to avoid detection

### Rule-Bypassing Capabilities
- **Work Hours Bypass** - Off-hours attacks with maintenance excuses
- **Network Segmentation Bypass** - Cross-segment attacks via role abuse
- **Lunch Break Exploitation** - Attacks during reduced monitoring periods
- **Holiday Backdoor Installation** - Zero-monitoring window exploitation
- **Cultural Exploitation** - Vietnamese business practice abuse
- **Legitimate Tool Abuse** - Tableau, Excel, PowerBI weaponization

### Dataset Quality Features
- **Realistic IP Addresses** - Proper network segmentation by department
- **Vietnamese Work Patterns** - Authentic business hours and cultural practices
- **Sophisticated Anomalies** - Rule-bypassing attacks for advanced UBA training
- **Multi-Stage Attacks** - Complex attack chains with persistence mechanisms
- **Cultural Context** - Vietnamese-specific exploitation techniques

## 📊 Dataset Generation Guide

### Quick Dataset Generation (5 minutes)
```bash
# 1. One-time setup
python setup_enhanced_vietnamese_company.py && python create_sandbox_user.py

# 2. Generate 5-minute simulation
python main_execution_enhanced.py balanced

# 3. Export to CSV
python perf_log_dataset_creator.py

# Result: final_clean_dataset_30d.csv with realistic Vietnamese enterprise data
```

### Production Dataset Generation (30+ minutes)
```bash
# 1. Setup with sample data
python setup_enhanced_vietnamese_company.py
python create_sandbox_user.py
python populate_sample_data.py

# 2. Run extended simulation (30+ minutes for rich dataset)
python main_execution_enhanced.py high_threat

# 3. Export comprehensive dataset
python perf_log_dataset_creator.py

# 4. Analyze quality
python business_hours_analysis.py
```

### Dataset Customization
```bash
# Modify simulation parameters in main_execution_enhanced.py:
SIMULATION_SPEED_UP = 1800    # 30 minutes simulated per 1 second real
TOTAL_REAL_SECONDS = 300      # 5 minutes real time = 150 hours simulated
ANOMALY_PERCENTAGE = 0.10     # 10% anomaly rate

# Then run:
python main_execution_enhanced.py balanced
python perf_log_dataset_creator.py
```

### Output Files
- **final_clean_dataset_30d.csv** - Main dataset with all features
- **simulation/users_config.json** - User configuration
- **MySQL Performance Schema** - Raw simulation data

### Troubleshooting
```bash
# Check MySQL connection
python -c "import mysql.connector; print('MySQL OK')"

# Test user permissions
python create_sandbox_user.py

# Verify database structure
python setup_enhanced_vietnamese_company.py

# Test scenarios
python enhanced_scenarios.py
```

For detailed technical information, see `MERGE_SUMMARY.md` and `RULE_BYPASSING_SUMMARY.md`.

---

## 🎯 Key Improvements Summary

This simulation has been enhanced to provide **enterprise-grade realism** for Vietnamese business environments:

- ✅ **Authentic Network Architecture** - Department-based IP segmentation
- ✅ **Cultural Accuracy** - Vietnamese work patterns and holidays  
- ✅ **Data Quality** - Fixed IP addresses and realistic patterns
- ✅ **Advanced Threat Modeling** - Rule-bypassing attacks and APT scenarios
- ✅ **Security Realism** - Sophisticated multi-stage attacks with cultural exploitation
- ✅ **Compliance Ready** - Vietnamese data protection standards

Perfect for training advanced UBA systems on realistic Vietnamese enterprise data with sophisticated threat scenarios!

---

## 🚀 Complete Dataset Generation Commands

### Method 1: Quick Start (5 minutes)
```bash
cd MA-sim

# Setup (run once)
python setup_enhanced_vietnamese_company.py
python create_sandbox_user.py

# Generate dataset
python main_execution_enhanced.py balanced
python perf_log_dataset_creator.py

# Result: final_clean_dataset_30d.csv
```

### Method 2: Production Quality (30+ minutes)
```bash
cd MA-sim

# Full setup with sample data
python setup_enhanced_vietnamese_company.py
python create_sandbox_user.py
python populate_sample_data.py

# Extended simulation for rich dataset
python main_execution_enhanced.py high_threat

# Export and analyze
python perf_log_dataset_creator.py
python business_hours_analysis.py
```

### Method 3: Multiple Scenarios
```bash
cd MA-sim

# Setup once
python setup_enhanced_vietnamese_company.py
python create_sandbox_user.py

# Generate different datasets
python main_execution_enhanced.py clean && python perf_log_dataset_creator.py
python main_execution_enhanced.py balanced && python perf_log_dataset_creator.py  
python main_execution_enhanced.py high_threat && python perf_log_dataset_creator.py
```

### Expected Output
- **final_clean_dataset_30d.csv** - Main dataset with Vietnamese enterprise patterns
- **Realistic IP addresses** - Department-based network segmentation
- **Rule-bypassing attacks** - Sophisticated APT-level threats
- **Vietnamese cultural context** - Authentic business patterns and holidays
- **High-quality anomalies** - Perfect for UBA system training

### Dataset Features
- 📊 **97 Vietnamese employees** across 8 departments
- 🏢 **7 specialized databases** (sales, HR, finance, marketing, support, inventory, admin)
- 🌐 **Network segmentation** with department-based IP ranges
- ⏰ **Vietnamese work patterns** (8AM-6PM, extended lunch breaks)
- 🚨 **10 rule-bypassing scenarios** for advanced threat detection
- 🇻🇳 **Cultural exploitation** techniques specific to Vietnamese business

Ready for enterprise-grade UBA system training and security research!