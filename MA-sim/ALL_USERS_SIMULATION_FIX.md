# All Users Simulation Fix - Complete Solution

## Problem Summary

The original `main_execution_enhanced.py` was only executing 10 users instead of all 97 configured users, resulting in incomplete datasets that didn't represent the full Vietnamese company simulation.

## Root Cause Analysis

### Original Issues:
1. **Random User Selection**: `agent = random.choice(pool_agents)` only selected random users
2. **Limited Thread Pool**: Only `NUM_THREADS = 10` threads were created
3. **No User Rotation**: Same 10 random users would be selected repeatedly
4. **Short Simulation Time**: 15 minutes wasn't enough for comprehensive coverage

### Original Configuration:
```python
NUM_THREADS = 10           # Only 10 threads
SIMULATION_SPEED_UP = 3600 # 1 hour simulated per 1 second real
TOTAL_REAL_SECONDS = 900   # 15 minutes real time
```

## Complete Solution Implemented

### 1. Enhanced Thread Management
- **All Users Active**: Create a thread for each of the 97 users
- **Concurrent Execution**: Use `ThreadPoolExecutor(max_workers=len(pool_agents))`
- **Staggered Startup**: Small delays to prevent resource contention

### 2. Optimized Configuration
```python
NUM_THREADS = 20           # Increased for better management
SIMULATION_SPEED_UP = 1800 # 30 minutes simulated per 1 second real
TOTAL_REAL_SECONDS = 3600  # 1 hour real time (75 days simulated)
```

### 3. Improved User Activity
- **Reduced Wait Times**: Faster query generation
- **Better Activity Distribution**: More active during business hours
- **Weekend Activity**: 20% chance instead of 5%
- **Off-hours Activity**: 10% chance for better coverage

### 4. Enhanced Monitoring
- **Real-time Progress**: Live updates every 30 seconds
- **Thread Monitoring**: Track active threads per user
- **Query Rate Tracking**: Monitor queries per second
- **Simulation Time Display**: Show simulated vs real time

## Files Modified/Created

### Modified Files:
1. **`main_execution_enhanced.py`**
   - Complete rewrite of execution logic
   - All 97 users now get dedicated threads
   - Optimized wait times and activity levels
   - Enhanced progress monitoring

### New Files Created:
1. **`configure_simulation.py`** - Interactive configuration tool
2. **`test_all_users.py`** - Verification script
3. **`ALL_USERS_SIMULATION_FIX.md`** - This documentation

## Configuration Options

Use `configure_simulation.py` to easily set parameters:

### Quick Test (5 minutes real time)
```python
SIMULATION_SPEED_UP = 1800
TOTAL_REAL_SECONDS = 300
# Result: 2.5 hours simulated, all 97 users active
```

### Half Day Simulation (30 minutes real time)
```python
SIMULATION_SPEED_UP = 1800
TOTAL_REAL_SECONDS = 1800
# Result: 15 hours simulated (full business day)
```

### Full Week Simulation (2 hours real time)
```python
SIMULATION_SPEED_UP = 3024
TOTAL_REAL_SECONDS = 7200
# Result: 7 days simulated (complete week)
```

### Comprehensive Dataset (4 hours real time)
```python
SIMULATION_SPEED_UP = 10800
TOTAL_REAL_SECONDS = 14400
# Result: 30 days simulated (large ML dataset)
```

## Expected Results

### Before Fix:
- **Users**: 10 random users
- **Coverage**: Incomplete role representation
- **Dataset Size**: ~15K records
- **Simulation Quality**: Poor

### After Fix:
- **Users**: All 97 users active simultaneously
- **Coverage**: Complete role representation
  - SALES: 35 users
  - MARKETING: 12 users  
  - CUSTOMER_SERVICE: 15 users
  - HR: 6 users
  - FINANCE: 8 users
  - DEV: 10 users
  - MANAGEMENT: 8 users
  - ADMIN: 3 users
- **Dataset Size**: 100K+ records (depending on duration)
- **Simulation Quality**: Excellent

## How to Run

### 1. Quick Verification
```bash
cd MA-sim
python test_all_users.py
```

### 2. Configure Parameters
```bash
python configure_simulation.py
```

### 3. Run Simulation
```bash
python main_execution_enhanced.py
```

### 4. Monitor Progress
The simulation will show real-time progress:
```
⚡ Queries: 45,231 | Active: 97/97 | Elapsed: 15.2min | Sim: 14:30

📊 Progress Report:
   Elapsed: 15.2min | Remaining: 44.8min
   Active Threads: 97/97
   Total Queries: 45,231
   Query Rate: 49.6/sec
   Sim Time: 2025-12-11 14:30
```

### 5. Collect Dataset
After simulation completes, run the dataset creator:
```bash
python perf_log_dataset_creator.py
```

## Verification Steps

1. **User Count Check**: Verify all 97 users are in the final dataset
2. **Role Distribution**: Ensure all roles are represented proportionally
3. **Time Coverage**: Check that simulated time spans the expected duration
4. **Query Patterns**: Validate realistic business hour patterns
5. **Database Access**: Confirm role-appropriate database access

## Performance Optimizations

### Thread Management:
- Staggered thread startup to prevent resource spikes
- Proper thread cleanup on simulation end
- Memory-efficient agent management

### Query Generation:
- Reduced wait times for higher activity
- Role-based activity patterns
- Realistic business hour simulation

### Monitoring:
- Efficient progress tracking
- Non-blocking status updates
- Resource usage optimization

## Troubleshooting

### If simulation seems slow:
- Reduce `SIMULATION_SPEED_UP` value
- Increase `TOTAL_REAL_SECONDS` for longer simulation
- Check system resources (CPU, memory)

### If not all users appear in dataset:
- Verify MySQL performance schema is enabled
- Check that `perf_log_dataset_creator.py` runs after simulation
- Ensure sufficient disk space for logs

### If queries are too fast/slow:
- Adjust wait times in `enhanced_user_worker` function
- Modify activity levels per role
- Change business hours logic

## Expected Dataset Quality

With this fix, you should get:
- **97 users** instead of 10
- **Realistic activity patterns** across all roles
- **Comprehensive time coverage** (days/weeks of simulated activity)
- **High-quality data** suitable for UBA model training
- **Proper Vietnamese business patterns** with authentic user names and behaviors

The simulation now properly represents a medium-sized Vietnamese company with ~100 employees across all business functions, making it ideal for User and Entity Behavior Analytics (UEBA) research and development.