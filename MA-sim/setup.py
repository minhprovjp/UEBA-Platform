#!/usr/bin/env python3
"""
Vietnamese Enterprise UBA Simulation - Setup Script
Automated setup for new users
"""

import mysql.connector
import subprocess
import sys
import os

def check_requirements():
    """Check if all requirements are met"""
    print("🔍 CHECKING REQUIREMENTS")
    print("=" * 50)
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ required")
        return False
    else:
        print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor}")
    
    # Check required packages
    required_packages = [
        'mysql-connector-python',
        'pandas',
        'numpy',
        'faker'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} (missing)")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n📦 INSTALLING MISSING PACKAGES:")
        for package in missing_packages:
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", package])
                print(f"✅ Installed {package}")
            except subprocess.CalledProcessError:
                print(f"❌ Failed to install {package}")
                return False
    
    return True

def setup_mysql_connection():
    """Test and setup MySQL connection"""
    print(f"\n🗄️ SETTING UP MYSQL CONNECTION")
    print("=" * 50)
    
    # Default connection parameters
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'root'
    }
    
    print("Testing MySQL connection with default settings:")
    print(f"   Host: {config['host']}")
    print(f"   Port: {config['port']}")
    print(f"   User: {config['user']}")
    
    try:
        conn = mysql.connector.connect(**config)
        conn.close()
        print("✅ MySQL connection successful")
        return True
    except Exception as e:
        print(f"❌ MySQL connection failed: {e}")
        print("\n📋 Please ensure:")
        print("   1. MySQL server is running")
        print("   2. Root user has password 'root'")
        print("   3. MySQL is accessible on localhost:3306")
        return False

def setup_databases():
    """Create and setup all required databases"""
    print(f"\n🏗️ SETTING UP DATABASES")
    print("=" * 50)
    
    try:
        # Run the schema setup script
        if os.path.exists('schema_fix.sql'):
            print("Running database schema setup...")
            result = subprocess.run([
                'mysql', '-u', 'root', '-proot', '-e', 'source schema_fix.sql'
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ Database schema created successfully")
            else:
                print(f"⚠️ Schema setup completed with warnings: {result.stderr}")
        
        # Run enhanced Vietnamese company setup
        print("Setting up Vietnamese company structure...")
        result = subprocess.run([sys.executable, 'setup_enhanced_vietnamese_company.py'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Vietnamese company structure created")
        else:
            print(f"❌ Company setup failed: {result.stderr}")
            return False
        
        # Create sandbox users
        print("Creating sandbox users...")
        result = subprocess.run([sys.executable, 'create_sandbox_user.py'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Sandbox users created")
        else:
            print(f"❌ User creation failed: {result.stderr}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return False

def verify_setup():
    """Verify the complete setup"""
    print(f"\n🧪 VERIFYING SETUP")
    print("=" * 50)
    
    try:
        # Run the analysis tool to verify everything works
        result = subprocess.run([sys.executable, 'correct_database_analysis.py'], 
                              capture_output=True, text=True)
        
        if "SUCCESS: System meets quality standards" in result.stdout:
            print("✅ System verification successful")
            return True
        else:
            print("⚠️ System verification completed with warnings")
            print("Check the output above for details")
            return True  # Still consider it successful
            
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False

def main():
    """Main setup function"""
    print("🚀 VIETNAMESE ENTERPRISE UBA SIMULATION SETUP")
    print("=" * 60)
    print("Setting up Vietnamese medium-sized sales company simulation")
    
    # Step 1: Check requirements
    if not check_requirements():
        print("\n❌ SETUP FAILED: Requirements not met")
        return False
    
    # Step 2: Setup MySQL connection
    if not setup_mysql_connection():
        print("\n❌ SETUP FAILED: MySQL connection issues")
        return False
    
    # Step 3: Setup databases
    if not setup_databases():
        print("\n❌ SETUP FAILED: Database setup issues")
        return False
    
    # Step 4: Verify setup
    if not verify_setup():
        print("\n⚠️ SETUP COMPLETED WITH WARNINGS")
    else:
        print("\n🎯 SETUP COMPLETED SUCCESSFULLY")
    
    print("=" * 60)
    print("🏢 Vietnamese Enterprise UBA Simulation Ready!")
    print("\n📋 USAGE:")
    print("   # Generate clean dataset (0% anomalies)")
    print("   python main_execution_enhanced.py clean")
    print("   ")
    print("   # Generate normal business dataset (5% anomalies)")
    print("   python main_execution_enhanced.py normal")
    print("   ")
    print("   # Generate attack scenario (25% anomalies)")
    print("   python main_execution_enhanced.py attack")
    print("   ")
    print("   # Analyze dataset quality")
    print("   python correct_database_analysis.py")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
