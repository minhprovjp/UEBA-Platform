# Vietnamese Enterprise UBA Simulation

A comprehensive User Behavior Analytics (UBA) simulation system for a Vietnamese medium-sized sales company. Generates realistic MySQL database activity logs for security research and anomaly detection.

## 🏢 System Overview

**Company Profile**: Vietnamese medium-sized sales enterprise
- **Users**: 103 Vietnamese employees with authentic names
- **Databases**: 7 specialized databases (sales, inventory, finance, marketing, support, hr, admin)
- **Tables**: 35 tables with realistic Vietnamese business data
- **Performance**: 98.3% success rate, <2% error rate

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- MySQL Server 8.0+
- MySQL root user with password 'root'

### Installation
```bash
# 1. Clone or download the MA-sim folder
# 2. Navigate to MA-sim directory
cd MA-sim

# 3. Run automated setup
python setup.py
```

The setup script will:
- ✅ Install required Python packages
- ✅ Test MySQL connection
- ✅ Create 7 specialized databases
- ✅ Setup 35 tables with Vietnamese business data
- ✅ Create 103 Vietnamese users with role-based permissions
- ✅ Verify system functionality

### Usage

```bash
# Generate clean dataset (0% anomalies)
python main_execution_enhanced.py clean

# Generate normal business dataset (5% anomalies)
python main_execution_enhanced.py normal

# Generate attack scenario (25% anomalies)
python main_execution_enhanced.py attack

# Analyze dataset quality
python correct_database_analysis.py
```

## 📊 Database Structure

### 7 Specialized Databases
- **sales_db**: Customer and sales management (7 tables)
- **inventory_db**: Warehouse and stock management (4 tables)
- **finance_db**: Financial records and accounting (6 tables)
- **marketing_db**: Marketing campaigns and leads (5 tables)
- **support_db**: Customer support tickets (4 tables)
- **hr_db**: Human resources management (5 tables)
- **admin_db**: System administration (4 tables)

### Vietnamese Business Context
- Authentic Vietnamese company names and addresses
- Realistic Vietnamese employee names with proper formatting
- Vietnamese business terminology and processes
- Proper Vietnamese currency (VND) and number formatting

## 🎯 Simulation Scenarios

### Clean Scenario (`clean`)
- **Anomaly Rate**: 0%
- **Use Case**: Baseline normal behavior
- **Output**: Pure business operations

### Normal Scenario (`normal`)
- **Anomaly Rate**: 5%
- **Insider Threats**: 5% of employees
- **Use Case**: Realistic business environment

### Attack Scenario (`attack`)
- **Anomaly Rate**: 25%
- **External Attackers**: 3 active
- **Obfuscation**: 50% of attacks
- **Use Case**: Security testing and red team exercises

## 📁 Core Files

### Essential System Files
- `main_execution_enhanced.py` - Main simulation engine
- `agents_enhanced.py` - Vietnamese user behavior models
- `corrected_enhanced_sql_library.py` - Business query library
- `create_sandbox_user.py` - User creation system
- `setup_enhanced_vietnamese_company.py` - Database structure
- `executor.py` - MySQL query execution engine

### Configuration
- `config_markov.py` - Markov chain behavior configuration
- `simulation/users_config.json` - Vietnamese user definitions

### Analysis Tools
- `correct_database_analysis.py` - Dataset quality analysis
- `fix_column_errors.py` - Schema validation tool

### Setup and Maintenance
- `setup.py` - Automated installation script
- `schema_fix.sql` - Database schema creation

## 🔧 Troubleshooting

### MySQL Connection Issues
```bash
# Check MySQL service
sudo systemctl status mysql

# Reset root password
sudo mysql -u root -p
ALTER USER 'root'@'localhost' IDENTIFIED BY 'root';
FLUSH PRIVILEGES;
```

### Permission Issues
```bash
# Grant necessary permissions
GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost';
FLUSH PRIVILEGES;
```

### Package Installation Issues
```bash
# Install packages manually
pip install mysql-connector-python pandas numpy faker
```

## 📊 Performance Metrics

- **Success Rate**: 98.3%
- **Error Rate**: <2%
- **Query Generation**: 60-80 queries/second
- **Simulation Speed**: 3600x real-time acceleration
- **Dataset Quality**: Production-ready

## 🏆 Features

### Vietnamese Enterprise Authenticity
- ✅ 103 Vietnamese employees with authentic names
- ✅ Realistic Vietnamese business processes
- ✅ Proper Vietnamese company structure
- ✅ Vietnamese currency and formatting

### Advanced Security Simulation
- ✅ Role-based database access control
- ✅ Insider threat simulation
- ✅ External attacker behavior
- ✅ SQL injection and privilege escalation attacks
- ✅ Query obfuscation techniques

### High-Quality Dataset Generation
- ✅ Zero table existence errors
- ✅ Zero permission errors
- ✅ Correct column names and schemas
- ✅ Realistic query patterns
- ✅ Comprehensive business scenarios

## 📋 System Requirements

- **Python**: 3.8 or higher
- **MySQL**: 8.0 or higher
- **RAM**: 4GB minimum, 8GB recommended
- **Storage**: 2GB for databases and logs
- **OS**: Windows, Linux, or macOS

## 🆘 Support

For issues or questions:
1. Check the troubleshooting section above
2. Verify MySQL connection and permissions
3. Run `python correct_database_analysis.py` for system diagnostics
4. Review the generated log files for detailed error information

## 📄 License

This simulation system is designed for security research and educational purposes.
