# Flexible Anomaly Configuration Guide

## Overview

The MA-sim simulation system provides **flexible anomaly configuration** that allows you to generate realistic datasets for different cybersecurity scenarios. This guide demonstrates how to configure various anomaly patterns for UBA training and testing.

## 🎯 Supported Anomaly Scenarios

### 1. Normal Business Operations
**Use Case**: Establish baseline behavior patterns
```python
config = {
    "anomaly_rate": 0.05,        # 5% anomalies
    "insider_threat_rate": 0.02, # 2% insider threats
    "external_attackers": 1,     # Minimal external threats
    "obfuscation_rate": 0.3      # 30% obfuscation
}
```
**Characteristics**:
- Low anomaly rate reflects normal business environment
- Minimal insider threats (disgruntled employees, accidents)
- Single external attacker (opportunistic attacks)
- Low obfuscation (basic attack attempts)

### 2. Under Cyber Attack
**Use Case**: Active attack scenario simulation
```python
config = {
    "anomaly_rate": 0.25,        # 25% anomalies
    "insider_threat_rate": 0.1,  # 10% insider threats
    "external_attackers": 3,     # Multiple attack vectors
    "obfuscation_rate": 0.7      # 70% obfuscation
}
```
**Characteristics**:
- High anomaly rate (organization under attack)
- Increased insider threats (compromised accounts)
- Multiple external attackers (coordinated attack)
- High obfuscation (sophisticated evasion techniques)

### 3. Insider Threat Focus
**Use Case**: Insider threat detection training
```python
config = {
    "anomaly_rate": 0.15,        # 15% anomalies
    "insider_threat_rate": 0.25, # 25% insider threats (high)
    "external_attackers": 1,     # Minimal external activity
    "obfuscation_rate": 0.2      # 20% obfuscation
}
```
**Characteristics**:
- Moderate anomaly rate
- **High insider threat rate** (primary focus)
- Minimal external threats
- Low obfuscation (insiders less likely to obfuscate)

### 4. Advanced Persistent Threat (APT)
**Use Case**: Stealthy, long-term attack simulation
```python
config = {
    "anomaly_rate": 0.08,        # 8% anomalies (stealthy)
    "insider_threat_rate": 0.05, # 5% insider threats
    "external_attackers": 2,     # Persistent attackers
    "obfuscation_rate": 0.9      # 90% obfuscation (highly evasive)
}
```
**Characteristics**:
- **Low anomaly rate** (stealthy operations)
- Moderate insider threats (compromised accounts)
- Persistent external attackers
- **Very high obfuscation** (advanced evasion)

### 5. Red Team Exercise
**Use Case**: Penetration testing simulation
```python
config = {
    "anomaly_rate": 0.4,         # 40% anomalies
    "insider_threat_rate": 0.15, # 15% insider simulation
    "external_attackers": 5,     # Multiple red team members
    "obfuscation_rate": 0.8      # 80% obfuscation
}
```
**Characteristics**:
- **Very high anomaly rate** (aggressive testing)
- Moderate insider simulation
- **Multiple attackers** (red team members)
- High obfuscation (testing evasion capabilities)

## 🔧 Configuration Parameters

### Core Parameters

| Parameter | Range | Description |
|-----------|-------|-------------|
| `anomaly_rate` | 0.0 - 1.0 | Percentage of queries that are anomalous |
| `insider_threat_rate` | 0.0 - 1.0 | Percentage of normal users that become malicious |
| `external_attackers` | 1 - 10 | Number of external malicious agents |
| `obfuscation_rate` | 0.0 - 1.0 | Percentage of attacks that use obfuscation |

### Advanced Parameters

| Parameter | Range | Description |
|-----------|-------|-------------|
| `normal_users` | 5 - 50 | Number of legitimate business users |
| `attack_sophistication` | 1 - 5 | Complexity of attack patterns |
| `temporal_patterns` | Boolean | Enable time-based attack patterns |
| `multi_stage_attacks` | Boolean | Enable complex attack chains |

## 📊 Generated Dataset Characteristics

### Query Distribution by Scenario

| Scenario | Normal | Anomaly | Insider | External | Obfuscated |
|----------|--------|---------|---------|----------|------------|
| Normal Business | 95% | 5% | 2% | 3% | 1.5% |
| Under Attack | 75% | 25% | 10% | 15% | 17.5% |
| Insider Focus | 85% | 15% | 12.5% | 2.5% | 3% |
| APT | 92% | 8% | 4% | 4% | 7.2% |
| Red Team | 60% | 40% | 15% | 25% | 32% |

### Attack Type Distribution

**Normal Business Operations**:
- 60% reconnaissance queries
- 30% data access attempts  
- 10% privilege escalation

**Under Cyber Attack**:
- 40% SQL injection attempts
- 30% data exfiltration
- 20% reconnaissance
- 10% system manipulation

**Insider Threat Focus**:
- 50% unauthorized data access
- 30% privilege abuse
- 20% data exfiltration

**Advanced Persistent Threat**:
- 70% stealthy reconnaissance
- 20% lateral movement
- 10% data collection

**Red Team Exercise**:
- 35% vulnerability exploitation
- 25% privilege escalation
- 25% data exfiltration
- 15% persistence mechanisms

## 🚀 Implementation Examples

### Basic Configuration
```python
from main_execution_mt import CLIENT_PROFILES
from agents import EmployeeAgent, MaliciousAgent

# Configure scenario
scenario_config = {
    "anomaly_rate": 0.15,
    "insider_threat_rate": 0.05,
    "external_attackers": 2,
    "obfuscation_rate": 0.6
}

# Create agents based on configuration
agents = create_scenario_agents(scenario_config)
```

### Dynamic Configuration
```python
# Adjust configuration based on time of day
if current_hour >= 18 or current_hour <= 6:  # After hours
    scenario_config["anomaly_rate"] *= 2.0  # Higher anomaly rate
    scenario_config["insider_threat_rate"] *= 1.5  # More insider activity

# Adjust based on day of week
if is_weekend:
    scenario_config["anomaly_rate"] *= 0.5  # Lower weekend activity
```

### Scenario Transitions
```python
# Simulate escalating attack scenario
scenarios = [
    {"name": "Normal", "duration": 1000, "config": normal_config},
    {"name": "Initial Compromise", "duration": 500, "config": apt_config},
    {"name": "Active Attack", "duration": 300, "config": attack_config}
]

for scenario in scenarios:
    run_scenario(scenario["config"], scenario["duration"])
```

## 📈 Validation and Metrics

### Key Metrics to Monitor

1. **Anomaly Distribution**
   - Target vs. actual anomaly rates
   - Insider vs. external threat ratio
   - Attack type diversity

2. **Behavioral Realism**
   - Database access patterns
   - Query complexity distribution
   - Temporal behavior patterns

3. **Evasion Techniques**
   - Obfuscation method distribution
   - Success rate of evasive queries
   - Detection difficulty metrics

### Validation Commands
```bash
# Test specific scenario
python demo_anomaly_scenarios.py

# Run comprehensive scenario testing
python test_anomaly_scenarios.py

# Validate configuration flexibility
python test_complete_simulation.py
```

## 🎯 Use Case Recommendations

### UBA System Training
- **Baseline Training**: Use "Normal Business Operations" (95% normal traffic)
- **Anomaly Detection**: Use "Under Cyber Attack" (balanced normal/anomaly)
- **Insider Detection**: Use "Insider Threat Focus" (high insider rate)

### Security Tool Testing
- **SIEM Tuning**: Use "APT" scenario (low-noise, high-sophistication)
- **DLP Testing**: Use "Insider Threat Focus" (data exfiltration patterns)
- **WAF Testing**: Use "Red Team Exercise" (diverse attack vectors)

### Research and Development
- **Algorithm Development**: Use configurable scenarios with known ground truth
- **Benchmark Creation**: Use standardized scenario configurations
- **Comparative Analysis**: Use multiple scenarios with controlled variables

## 🔧 Customization Guide

### Creating Custom Scenarios
```python
def create_custom_scenario():
    return {
        "name": "Custom Scenario",
        "config": {
            "anomaly_rate": 0.12,        # 12% anomalies
            "insider_threat_rate": 0.08, # 8% insider threats
            "external_attackers": 2,     # 2 external attackers
            "obfuscation_rate": 0.5,     # 50% obfuscation
            # Custom parameters
            "attack_sophistication": 3,   # Medium sophistication
            "temporal_patterns": True,    # Enable time-based patterns
            "target_databases": ["sales_db", "hr_db"]  # Specific targets
        }
    }
```

### Parameter Tuning Guidelines

1. **Start Conservative**: Begin with low anomaly rates and increase gradually
2. **Balance Realism**: Ensure insider/external ratios match your environment
3. **Consider Context**: Adjust obfuscation rates based on attacker sophistication
4. **Validate Results**: Monitor generated patterns for realism and diversity

The flexible anomaly configuration system enables realistic, diverse, and controllable dataset generation for comprehensive UBA system training and testing.