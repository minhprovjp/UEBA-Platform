# MA-sim Quick Start Guide

## Consolidated Structure Overview

The MA-sim folder has been streamlined into **3 main modules** that handle all functionality:

### 🗄️ Database Manager (`database_manager.py`)
**All database setup, management, and fixes**
```bash
# Complete setup (recommended for first time)
python database_manager.py complete

# Individual operations
python database_manager.py clean      # Clean all databases
python database_manager.py structure  # Create database structure
python database_manager.py users     # Create all 97 users
```

### 🚀 Simulation Runner (`simulation_runner.py`)
**All simulation execution and configuration**
```bash
# Complete workflow (recommended)
python simulation_runner.py complete

# Individual operations
python simulation_runner.py test      # Test setup
python simulation_runner.py configure # Configure simulation
python simulation_runner.py run      # Run simulation only
```

### 📊 Analysis Tools (`analysis_tools.py`)
**All dataset analysis and quality assessment**
```bash
# Analyze latest dataset
python analysis_tools.py dataset

# Analyze specific dataset
python analysis_tools.py dataset your_dataset.csv

# Database structure analysis
python analysis_tools.py database

# Quality assessment only
python analysis_tools.py quality your_dataset.csv
```

## Essential Library Files (Keep These)

- `agents_enhanced.py` - Vietnamese business agent behaviors
- `corrected_enhanced_sql_library.py` - SQL query library
- `executor.py` - SQL execution engine
- `stats_utils.py` - Statistical behavior modeling
- `config_markov.py` - User behavior patterns
- `obfuscator.py` - Malicious SQL obfuscation

## Configuration Files

- `requirements.txt` - Python dependencies
- `schema_fix.sql` - Database schema fixes
- `simulation/users_config.json` - User configuration

## Complete Workflow

### 1. First Time Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Complete database setup
python database_manager.py complete
```

### 2. Test Setup
```bash
# Verify everything is working
python simulation_runner.py test
```

### 3. Run Simulation
```bash
# Interactive simulation with configuration
python simulation_runner.py complete
```

### 4. Analyze Results
```bash
# Comprehensive analysis
python analysis_tools.py dataset simulation_dataset_*.csv
```

## Simulation Modes Available

1. **Quick Test** - 5 minutes (300 seconds)
2. **Half Day** - 30 minutes (1800 seconds) 
3. **Full Week** - 2 hours (7200 seconds)
4. **Large Dataset** - 4 hours (14400 seconds)
5. **Custom** - User-defined duration and speed

## Anomaly Options

- **Clean Dataset** - No anomalies (0% error rate)
- **Realistic Dataset** - 10% anomaly rate with insider threats and external hackers

## Key Features Preserved

✅ All 97 Vietnamese users with proper roles
✅ 7-database enterprise structure
✅ Markov chain behavior modeling
✅ Statistical user activity patterns
✅ Insider threat simulation
✅ External hacker simulation
✅ SQL obfuscation for attackers
✅ Comprehensive quality analysis
✅ Temporal pattern analysis
✅ Role-based database access

## Files Removed (29 total)

All redundant setup, fix, analysis, and execution files have been consolidated. The functionality is preserved but now organized in the 3 main modules above.

## Documentation Preserved

All `.md` files containing research, summaries, and guides have been kept for reference.