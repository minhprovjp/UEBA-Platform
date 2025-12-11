#!/usr/bin/env python3
"""
Simulation Configuration Helper
Easily adjust simulation parameters for different scenarios
"""

import json
import os

def configure_simulation():
    """Interactive configuration for simulation parameters"""
    
    print("🔧 SIMULATION CONFIGURATION HELPER")
    print("=" * 50)
    
    # Current configuration
    print("📊 Current Configuration:")
    print("   NUM_THREADS = 20")
    print("   SIMULATION_SPEED_UP = 1800 (30 min simulated per 1 sec real)")
    print("   TOTAL_REAL_SECONDS = 3600 (1 hour real time)")
    print("   Simulated Duration = 30 hours")
    
    print("\n🎯 Recommended Configurations:")
    print()
    
    configs = {
        "1": {
            "name": "Quick Test (5 minutes real, 2.5 hours simulated)",
            "NUM_THREADS": 20,
            "SIMULATION_SPEED_UP": 1800,
            "TOTAL_REAL_SECONDS": 300,
            "description": "Fast test to verify all users are working"
        },
        "2": {
            "name": "Half Day Simulation (30 minutes real, 15 hours simulated)",
            "NUM_THREADS": 20,
            "SIMULATION_SPEED_UP": 1800,
            "TOTAL_REAL_SECONDS": 1800,
            "description": "Covers a full business day"
        },
        "3": {
            "name": "Full Week Simulation (2 hours real, 7 days simulated)",
            "NUM_THREADS": 20,
            "SIMULATION_SPEED_UP": 3024,  # 7 days * 24 hours / 2 hours
            "TOTAL_REAL_SECONDS": 7200,
            "description": "Complete week of business activity"
        },
        "4": {
            "name": "Comprehensive Dataset (4 hours real, 30 days simulated)",
            "NUM_THREADS": 20,
            "SIMULATION_SPEED_UP": 10800,  # 30 days * 24 hours / 4 hours
            "TOTAL_REAL_SECONDS": 14400,
            "description": "Large dataset for ML training"
        },
        "5": {
            "name": "Custom Configuration",
            "description": "Set your own parameters"
        }
    }
    
    for key, config in configs.items():
        print(f"{key}. {config['name']}")
        print(f"   {config['description']}")
        if key != "5":
            real_hours = config['TOTAL_REAL_SECONDS'] / 3600
            sim_hours = config['TOTAL_REAL_SECONDS'] * config['SIMULATION_SPEED_UP'] / 3600
            sim_days = sim_hours / 24
            print(f"   Real Time: {real_hours:.1f} hours | Simulated: {sim_days:.1f} days")
        print()
    
    choice = input("Select configuration (1-5): ").strip()
    
    if choice in configs and choice != "5":
        config = configs[choice]
        print(f"\n✅ Selected: {config['name']}")
        
        # Generate the configuration code
        config_code = f"""
# Update these values in main_execution_enhanced.py:

NUM_THREADS = {config['NUM_THREADS']}
SIMULATION_SPEED_UP = {config['SIMULATION_SPEED_UP']}
TOTAL_REAL_SECONDS = {config['TOTAL_REAL_SECONDS']}

# This will simulate:
# - Real time: {config['TOTAL_REAL_SECONDS']/3600:.1f} hours
# - Simulated time: {config['TOTAL_REAL_SECONDS'] * config['SIMULATION_SPEED_UP'] / 3600 / 24:.1f} days
# - All {97} users will be active
"""
        
        print(config_code)
        
        # Offer to update the file automatically
        update = input("Update main_execution_enhanced.py automatically? (y/n): ").strip().lower()
        
        if update == 'y':
            update_main_execution(config)
            print("✅ Configuration updated!")
        else:
            print("📝 Please update the values manually in main_execution_enhanced.py")
            
    elif choice == "5":
        print("\n🔧 Custom Configuration:")
        
        try:
            real_hours = float(input("Real time duration (hours): "))
            sim_days = float(input("Simulated time duration (days): "))
            
            total_real_seconds = int(real_hours * 3600)
            simulation_speed_up = int(sim_days * 24 * 3600 / total_real_seconds)
            
            custom_config = {
                "NUM_THREADS": 20,
                "SIMULATION_SPEED_UP": simulation_speed_up,
                "TOTAL_REAL_SECONDS": total_real_seconds
            }
            
            print(f"\n📊 Custom Configuration:")
            print(f"   Real Time: {real_hours} hours")
            print(f"   Simulated Time: {sim_days} days")
            print(f"   Speed Up Factor: {simulation_speed_up}x")
            
            update = input("Apply this configuration? (y/n): ").strip().lower()
            if update == 'y':
                update_main_execution(custom_config)
                print("✅ Custom configuration applied!")
                
        except ValueError:
            print("❌ Invalid input. Please enter numeric values.")
    
    else:
        print("❌ Invalid choice.")

def update_main_execution(config):
    """Update the main_execution_enhanced.py file with new configuration"""
    
    file_path = "main_execution_enhanced.py"
    
    try:
        # Read the file
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Update the configuration values
        import re
        
        # Update NUM_THREADS
        content = re.sub(
            r'NUM_THREADS = \d+',
            f'NUM_THREADS = {config["NUM_THREADS"]}',
            content
        )
        
        # Update SIMULATION_SPEED_UP
        content = re.sub(
            r'SIMULATION_SPEED_UP = \d+',
            f'SIMULATION_SPEED_UP = {config["SIMULATION_SPEED_UP"]}',
            content
        )
        
        # Update TOTAL_REAL_SECONDS
        content = re.sub(
            r'TOTAL_REAL_SECONDS = \d+',
            f'TOTAL_REAL_SECONDS = {config["TOTAL_REAL_SECONDS"]}',
            content
        )
        
        # Write back to file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
            
        return True
        
    except Exception as e:
        print(f"❌ Error updating file: {e}")
        return False

def show_current_config():
    """Show current configuration from the file"""
    
    try:
        with open("main_execution_enhanced.py", 'r', encoding='utf-8') as f:
            content = f.read()
        
        import re
        
        num_threads = re.search(r'NUM_THREADS = (\d+)', content)
        speed_up = re.search(r'SIMULATION_SPEED_UP = (\d+)', content)
        total_seconds = re.search(r'TOTAL_REAL_SECONDS = (\d+)', content)
        
        if all([num_threads, speed_up, total_seconds]):
            threads = int(num_threads.group(1))
            speed = int(speed_up.group(1))
            seconds = int(total_seconds.group(1))
            
            real_hours = seconds / 3600
            sim_hours = seconds * speed / 3600
            sim_days = sim_hours / 24
            
            print("📊 Current Configuration:")
            print(f"   Threads: {threads}")
            print(f"   Speed Up: {speed}x")
            print(f"   Real Duration: {real_hours:.1f} hours")
            print(f"   Simulated Duration: {sim_days:.1f} days")
            print(f"   Expected Users: 97")
            
        else:
            print("❌ Could not parse current configuration")
            
    except Exception as e:
        print(f"❌ Error reading configuration: {e}")

if __name__ == "__main__":
    print("🚀 SIMULATION CONFIGURATION TOOL")
    print("=" * 60)
    
    while True:
        print("\nOptions:")
        print("1. Configure simulation parameters")
        print("2. Show current configuration")
        print("3. Exit")
        
        choice = input("\nSelect option (1-3): ").strip()
        
        if choice == "1":
            configure_simulation()
        elif choice == "2":
            show_current_config()
        elif choice == "3":
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please select 1-3.")