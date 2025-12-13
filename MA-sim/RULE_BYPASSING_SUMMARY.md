# Rule-Bypassing Scenarios - Vietnamese Enterprise Simulation

## 📋 System Rules Analysis

### 1. **Work Hours Rules**
- **Rule**: Employees work 8AM-6PM weekdays only
- **Enforcement**: Strict - no weekend/holiday activity for normal users
- **Bypass Techniques**:
  - ✅ Maintenance excuse during off-hours
  - ✅ Emergency access justification
  - ✅ Weekend low-monitoring exploitation (5% malicious activity allowed)

### 2. **Vietnamese Lunch Break Rules**
- **Rule**: Extended lunch break 11:30AM-1:30PM with reduced activity
- **Activity Levels**: 11:30-12:00 (40%), 12:00-13:00 (20%), 13:00-13:30 (30%)
- **Bypass Techniques**:
  - ✅ Exploit minimal monitoring during core lunch (12-1PM)
  - ✅ Use early/extended lunch windows for reconnaissance
  - ✅ Blend attacks with legitimate lunch-time activity

### 3. **Role-Based Access Control Rules**
- **Rule**: Strict database access by role
  - SALES: sales_db, marketing_db, support_db
  - HR: hr_db, finance_db, admin_db
  - FINANCE: finance_db, sales_db, hr_db, inventory_db
  - DEV/ADMIN: Full access to all databases
- **Bypass Techniques**:
  - ✅ Role escalation chains (CUSTOMER_SERVICE → SALES → MARKETING → FINANCE → ADMIN)
  - ✅ Cross-department impersonation
  - ✅ Abuse of DEV/ADMIN privileges for unauthorized access

### 4. **Network Segmentation Rules**
- **Rule**: Department-based IP ranges
  - Sales: 192.168.10.x
  - Marketing: 192.168.15.x
  - HR: 192.168.20.x
  - Finance: 192.168.30.x
  - External threats: 10.0.0.x
- **Bypass Techniques**:
  - ✅ Network hopping through legitimate cross-department roles
  - ✅ Management privilege escalation across segments
  - ✅ DEV/ADMIN network access abuse

### 5. **Vietnamese Holiday Rules**
- **Rule**: Zero activity on Vietnamese holidays
- **Holidays**: New Year, Tet, Liberation Day, Labor Day, Independence Day
- **Bypass Techniques**:
  - ✅ Holiday backdoor installation (zero monitoring)
  - ✅ Cultural timing exploitation
  - ✅ Patriotic holiday cover for malicious activities

### 6. **Behavioral Pattern Rules**
- **Rule**: Consistent user behavior patterns
- **Enforcement**: Activity level monitoring, timing patterns
- **Bypass Techniques**:
  - ✅ Establish legitimate patterns before attacks
  - ✅ Time-based evasion across multiple windows
  - ✅ Legitimate tool abuse (Tableau, Excel, PowerBI)

## 🚨 Advanced Rule-Bypassing Scenarios

### 1. **WORK_HOURS_BYPASS**
**Objective**: Exploit off-hours access with maintenance excuses
- Establish legitimate daytime patterns
- Use "system maintenance" justification for off-hours access
- Exploit weekend low-monitoring (5% malicious activity allowed)
- Create backdoors during minimal supervision

### 2. **NETWORK_SEGMENTATION_BYPASS**
**Objective**: Cross network segments using legitimate roles
- Start from authorized network segment
- Abuse DEV privileges for cross-network access
- Escalate through management roles
- Final attack from admin network segment

### 3. **ROLE_ESCALATION_CHAIN**
**Objective**: Chain legitimate roles for unauthorized access
- Begin with lowest privilege (CUSTOMER_SERVICE)
- Escalate through SALES → MARKETING → FINANCE
- Achieve ADMIN privileges through role chaining
- Export all data with ultimate privileges

### 4. **LUNCH_BREAK_EXPLOITATION**
**Objective**: Exploit reduced monitoring during Vietnamese lunch
- Normal morning activity to establish pattern
- Early lunch reconnaissance (11:30-12:00, 40% activity)
- Core lunch attacks (12:00-13:00, 20% activity)
- Extended lunch data theft (13:00-13:30, 30% activity)

### 5. **HOLIDAY_BACKDOOR_ACCESS**
**Objective**: Install backdoors during Vietnamese holidays
- Pre-holiday preparation and pattern establishment
- Tet holiday exploitation (zero monitoring)
- Labor Day malware installation
- Independence Day permission modification

### 6. **CROSS_DEPARTMENT_IMPERSONATION**
**Objective**: Impersonate employees from different departments
- Use legitimate Vietnamese employee names
- Access unauthorized departments with spoofed identity
- Exploit hierarchy respect culture
- Blend with normal cross-department communications

### 7. **LEGITIMATE_TOOL_ABUSE**
**Objective**: Abuse authorized business tools for attacks
- Tableau for unauthorized data visualization/export
- Excel for financial manipulation
- PowerBI for unauthorized reporting
- MySQLWorkbench for direct database access

### 8. **TIME_BASED_EVASION**
**Objective**: Spread attacks across time to avoid pattern detection
- Early morning reconnaissance (low activity window)
- Lunch break exploitation (reduced monitoring)
- End-of-day cleanup evasion
- Weekend persistence maintenance

### 9. **MULTI_STAGE_PERSISTENCE**
**Objective**: Create multiple backdoors across systems
- Initial compromise through vulnerable account
- Establish foothold with first backdoor
- Lateral movement with additional backdoors
- Administrative persistence for long-term control

### 10. **VIETNAMESE_CULTURAL_EXPLOITATION**
**Objective**: Exploit Vietnamese business culture
- Tet preparation period increased activity
- Vietnamese naming convention spoofing
- Hierarchy respect culture exploitation
- Overtime work culture abuse

## 🎯 Implementation Features

### Enhanced Malicious Agents
- **Skill Levels**: Script kiddie, Intermediate, Advanced APT
- **Detection Avoidance**: 0.3 - 0.8 based on skill level
- **Adaptive Behavior**: Learn from detected techniques
- **Cultural Knowledge**: Vietnamese holidays, business culture, naming conventions

### Rule-Bypassing Metadata
- **Bypass Technique**: Specific method used to circumvent rules
- **Timing Context**: When the bypass occurs (off_hours, lunch_break, holiday)
- **Evasion Level**: Sophistication of the evasion attempt
- **Cultural Context**: Vietnamese-specific exploitation methods

### Integration with Existing System
- **Seamless Integration**: Works with existing role-based access controls
- **Enhanced Logging**: Captures bypass attempts and techniques
- **Realistic Patterns**: Maintains believable user behavior
- **Advanced Detection**: Provides sophisticated anomalies for UBA training

## 📊 Benefits for UBA Training

### 1. **Sophisticated Threat Modeling**
- Real-world attack patterns that bypass security controls
- Cultural context for Vietnamese enterprise environments
- Time-based evasion techniques

### 2. **Advanced Anomaly Detection**
- Rule-bypassing behaviors for ML model training
- Subtle pattern deviations that require sophisticated detection
- Multi-stage attack sequences

### 3. **Enterprise Security Testing**
- Realistic insider threat scenarios
- Network segmentation bypass testing
- Cultural exploitation awareness

### 4. **Compliance and Audit**
- Vietnamese business hour compliance testing
- Role-based access control validation
- Holiday and cultural pattern analysis

This enhanced simulation provides enterprise-grade training data for UBA systems that need to detect sophisticated attackers who understand and can bypass traditional security rules.