#!/usr/bin/env python3
"""
Enhanced Simulation Scheduler for Vietnamese Medium-Sized Sales Company
Manages time-based simulation with realistic Vietnamese business patterns
"""

import random
from datetime import datetime, timedelta
from translator import EnhancedSQLTranslator

class EnhancedSimulationScheduler:
    """
    Enhanced scheduler that manages agent activities based on Vietnamese business hours,
    work patterns, and realistic timing constraints
    """
    
    def __init__(self, start_time, agents, sql_generator=None, db_state=None):
        self.current_time = start_time
        self.agents = agents
        self.sql_generator = sql_generator
        self.translator = EnhancedSQLTranslator(db_state)
        
        # Agent cooldowns - when each agent can act next
        self.agent_cooldowns = {}
        
        # Vietnamese business context
        self.vietnamese_holidays = [
            "2025-01-01",  # New Year
            "2025-01-29",  # Tet (Lunar New Year)
            "2025-04-30",  # Liberation Day
            "2025-05-01",  # Labor Day
            "2025-09-02"   # Independence Day
        ]
        
        # Work intensity by hour (Vietnamese business patterns)
        self.hourly_intensity = {
            6: 0.1, 7: 0.2, 8: 0.7, 9: 1.0, 10: 1.0,  # Morning ramp-up
            11: 0.9, 12: 0.3, 13: 0.2,                  # Lunch break
            14: 0.8, 15: 1.0, 16: 0.9, 17: 0.7,        # Afternoon peak
            18: 0.4, 19: 0.2, 20: 0.1, 21: 0.05        # Evening wind-down
        }
        # [NEW] Sự kiện doanh nghiệp (Hợp lệ nhưng volume cao)
        self.active_projects = {
            "MONTHLY_CLOSING": False, # Quyết toán tháng (Finance/Sales peak)
            "MARKETING_PUSH": False   # Chiến dịch MKT lớn
        }
        
    def check_business_events(self):
        """Kích hoạt sự kiện dựa trên ngày"""
        day = self.current_time.day
        
        # Cuối tháng (ngày 28-31): Quyết toán
        if day >= 28:
            self.active_projects["MONTHLY_CLOSING"] = True
        else:
            self.active_projects["MONTHLY_CLOSING"] = False
                
    def _get_dynamic_intensity(self, current_time):
        hour = current_time.hour
        minute = current_time.minute
        
        # --- 1. TÍNH BASE INTENSITY THEO GIỜ (Circadian Rhythm) ---
        intensity = 0.1 # Mặc định (đêm/OT vắng)

        # 1. ĐẦU GIỜ SÁNG (8:00 - 9:00)
        if hour == 8:
            intensity = 0.3 + (minute / 60) * 0.6
            
        # 2. CA SÁNG CAO ĐIỂM (9:00 - 11:00)
        elif 9 <= hour < 11:
            intensity = random.uniform(0.9, 1.0)
            
        # 3. GẦN TRƯA (11:00 - 12:00)
        elif hour == 11:
            intensity = max(0.1, 0.9 - (minute / 60) * 0.8)
            
        # 4. NGHỈ TRƯA (12:00 - 13:30)
        elif hour == 12:
            intensity = 0.05
        elif hour == 13 and minute < 30:
            intensity = 0.1 + (minute / 30) * 0.4
            
        # 5. CA CHIỀU & TAN TẦM (13:30 - 17:30)
        elif (hour == 13 and minute >= 30) or (14 <= hour < 16):
            intensity = random.uniform(0.85, 1.0)
        elif hour == 16:
            intensity = 0.9 - (minute / 60) * 0.3
        elif hour == 17:
            if minute < 30: intensity = 0.6 - (minute / 30) * 0.2
            else: intensity = 0.4 - ((minute - 30) / 30) * 0.3

        # --- 2. ÁP DỤNG LOGIC DỰ ÁN (Business Events) ---
        # Nếu đang quyết toán tháng, mọi người làm việc căng hơn (tăng 50%)
        # Nhưng vẫn phải tuân thủ giới hạn max là 1.0 (hoặc 1.2 nếu muốn overload)
        if self.active_projects.get("MONTHLY_CLOSING", False):
            # Chỉ tăng cường độ vào giờ làm việc, không tăng vào giờ nghỉ trưa/đêm
            if intensity > 0.2: 
                intensity = min(1.0, intensity * 1.5)

        return intensity

    def tick(self, minutes=1):
        """
        Advance simulation time and process agent actions
        
        Args:
            minutes: Number of minutes to advance
            
        Returns:
            List of log entries for actions taken during this tick
        """
        self.current_time += timedelta(minutes=minutes)
        logs = []
        
        # [NEW] Cập nhật trạng thái sự kiện mỗi khi qua ngày mới hoặc mỗi tick
        if self.current_time.hour == 0 and self.current_time.minute == 0:
            self.check_business_events()
        
        # Current time context
        current_hour = self.current_time.hour
        current_date = self.current_time.date().isoformat()
        is_weekend = self.current_time.weekday() >= 5
        is_holiday = current_date in self.vietnamese_holidays
        current_intensity = self._get_dynamic_intensity(self.current_time)
        
        for agent in self.agents:
            # Check if agent should be active
            if not self._should_agent_be_active(agent, current_hour, is_weekend, is_holiday, current_intensity):
                # Force logout if agent was active but shouldn't be now
                if agent.current_state != "START":
                    agent.current_state = "START"
                    agent.session_context = {}
                continue
            
            # Check cooldown period
            if agent.agent_id in self.agent_cooldowns:
                if self.current_time < self.agent_cooldowns[agent.agent_id]:
                    continue  # Still in cooldown
            
            # Generate agent action
            intent = agent.step()
            
            # Skip non-actionable states
            if intent['action'] in ["START"]:
                continue
            
            # Translate intent to SQL
            sql = self.translator.translate(intent)
            
            # Create log entry
            log_entry = {
                "timestamp": self.current_time.isoformat(),
                "user": agent.username,
                "role": agent.role,
                "action": intent['action'],
                "database": intent.get('target_database', 'sales_db'),
                "query": sql,
                "is_anomaly": intent.get('is_anomaly', 0),
                "session_id": intent.get('session_id'),
                "work_intensity": round(current_intensity, 2)
            }
            
            logs.append(log_entry)
            
            # Set cooldown for next action
            wait_minutes = self._calculate_wait_time(intent, agent, current_intensity)
            self.agent_cooldowns[agent.agent_id] = self.current_time + timedelta(minutes=wait_minutes)
        
        return logs

    def _should_agent_be_active(self, agent, current_hour, is_weekend, is_holiday, intensity):
        """
        Determine if an agent should be active based on Vietnamese business patterns
        """
        # Malicious agents can be active anytime (but with lower probability outside hours)
        if agent.is_malicious:
            if is_weekend or is_holiday:
                return random.random() < 0.1  # 10% chance during off-days
            elif 22 <= current_hour or current_hour < 6:
                return random.random() < 0.2  # 20% chance during night
            else:
                return random.random() < 0.8  # 80% chance during day
        
        # Normal employees follow Vietnamese business hours
        if is_weekend or is_holiday:
            return False  # No work on weekends/holidays
        
        # Role-specific work schedules
        work_schedule = self._get_work_schedule(agent.role)
        
        # Check if within work hours
        if not (work_schedule['start_hour'] <= current_hour < work_schedule['end_hour']):
            return False
        
        # Check lunch break
        if work_schedule['lunch_start'] <= current_hour < work_schedule['lunch_end']:
            return random.random() < 0.2  # 20% chance during lunch
        
        # Apply hourly intensity
        intensity = self.hourly_intensity.get(current_hour, 0.5)
        
        # Giữ ngoại lệ cho CS (trực trưa) và Dev (OT)
        if agent.role == "CUSTOMER_SERVICE" and 12 <= current_hour < 13:
            return random.random() < 0.4
        if agent.role == "DEV" and 18 <= current_hour < 21:
            return random.random() < 0.3

        # So sánh random với intensity hiện tại (đã tính theo phút)
        # Intensity thấp (vd: 11:55) -> Khả năng active thấp
        return random.random() < intensity

    def _get_work_schedule(self, role):
        """Get work schedule for different roles in Vietnamese company"""
        schedules = {
            'SALES': {
                'start_hour': 8, 'end_hour': 18,
                'lunch_start': 12, 'lunch_end': 13
            },
            'MARKETING': {
                'start_hour': 8, 'end_hour': 17,
                'lunch_start': 12, 'lunch_end': 13
            },
            'CUSTOMER_SERVICE': {
                'start_hour': 7, 'end_hour': 19,  # Extended hours
                'lunch_start': 12, 'lunch_end': 13
            },
            'HR': {
                'start_hour': 8, 'end_hour': 17,
                'lunch_start': 12, 'lunch_end': 13
            },
            'FINANCE': {
                'start_hour': 8, 'end_hour': 17,
                'lunch_start': 12, 'lunch_end': 13
            },
            'DEV': {
                'start_hour': 9, 'end_hour': 18,  # Flexible hours
                'lunch_start': 12, 'lunch_end': 13
            },
            'MANAGEMENT': {
                'start_hour': 8, 'end_hour': 19,  # Longer hours
                'lunch_start': 12, 'lunch_end': 13
            },
            'ADMIN': {
                'start_hour': 8, 'end_hour': 17,
                'lunch_start': 12, 'lunch_end': 13
            }
        }
        
        return schedules.get(role, schedules['SALES'])

    def _calculate_wait_time(self, intent, agent, intensity):
        """
        Calculate realistic wait time between actions based on Vietnamese business patterns
        """
        base_wait = 2  # Base 2 minutes
        
        # Action-specific wait times
        action_multipliers = {
            'LOGIN': 0.5,
            'LOGOUT': 0.3,
            'SEARCH_CUSTOMER': 1.0,
            'VIEW_CUSTOMER': 1.5,
            'UPDATE_CUSTOMER': 2.0,
            'CREATE_ORDER': 3.0,
            'SEARCH_ORDER': 1.0,
            'VIEW_ORDER': 1.5,
            'UPDATE_ORDER_STATUS': 2.0,
            'SEARCH_EMPLOYEE': 1.0,
            'VIEW_PROFILE': 1.5,
            'CHECK_ATTENDANCE': 1.0,
            'VIEW_PAYROLL': 2.0,
            'UPDATE_SALARY': 3.0,
            'SEARCH_CAMPAIGN': 1.0,
            'VIEW_CAMPAIGN': 1.5,
            'CREATE_LEAD': 2.5,
            'UPDATE_LEAD': 2.0,
            'SEARCH_TICKET': 1.0,
            'VIEW_TICKET': 1.5,
            'CREATE_TICKET': 2.5,
            'UPDATE_TICKET': 2.0,
            'VIEW_INVOICE': 1.5,
            'CREATE_INVOICE': 3.0,
            'VIEW_EXPENSES': 1.5,
            'CHECK_STOCK': 1.0,
            'UPDATE_INVENTORY': 2.5
        }
        
        # Role-specific multipliers (Vietnamese work pace)
        role_multipliers = {
            'SALES': 1.0,           # Standard pace
            'MARKETING': 1.1,       # Slightly slower (creative work)
            'CUSTOMER_SERVICE': 0.8, # Faster pace (customer pressure)
            'HR': 1.2,             # Slower pace (careful work)
            'FINANCE': 1.3,         # Slowest pace (accuracy critical)
            'DEV': 1.1,             # Moderate pace
            'MANAGEMENT': 1.5,      # Slower pace (decision making)
            'ADMIN': 1.0            # Standard pace
        }
        
        # Time of day multiplier (Vietnamese energy patterns)
        hour_multipliers = {
            8: 1.2,   # Slow start
            9: 1.0,   # Peak morning
            10: 0.9,  # High energy
            11: 1.0,  # Pre-lunch
            12: 2.0,  # Lunch break
            13: 1.5,  # Post-lunch slowdown
            14: 1.0,  # Afternoon recovery
            15: 0.9,  # Peak afternoon
            16: 1.0,  # Standard
            17: 1.2,  # End of day slowdown
            18: 1.5   # Overtime fatigue
        }
        
        action = intent.get('action', 'LOGIN')
        role = agent.role
        
        # wait_time = base_wait
        # wait_time *= action_multipliers.get(action, 1.0)
        # wait_time *= role_multipliers.get(role, 1.0)
        # wait_time *= hour_multipliers.get(current_hour, 1.0)
        
        # # Add randomness (±50%)
        # wait_time *= random.uniform(0.5, 1.5)
        
        # # Malicious agents act faster (more urgent)
        # if agent.is_malicious:
        #     wait_time *= 0.7
        
        wait = base_wait * action_multipliers.get(intent.get('action'), 1.0)
        
        # [LOGIC MỚI] Điều chỉnh theo Intensity
        if intensity > 0:
            # Intensity 0.9 (Cao điểm) -> wait nhỏ -> spam query nhiều
            # Intensity 0.2 (Gần nghỉ) -> wait lớn -> ít query
            wait = wait / intensity
        else:
            wait = wait * 5
            
        # Add randomness
        wait *= random.uniform(0.7, 1.3)
        
        # [NEW] Low-and-Slow Attack Logic
        # Nếu là Hacker chế độ tàng hình -> Nghỉ rất lâu giữa các lệnh
        if agent.is_malicious and getattr(agent, 'attack_mode', '') == 'low_slow':
            # Nghỉ từ 10 đến 60 phút ảo giữa các lần query
            return random.randint(10, 60) 
        
        return max(1, int(wait))

    def get_simulation_stats(self):
        """Get current simulation statistics"""
        active_agents = len([a for a in self.agents if a.current_state != "START"])
        malicious_agents = len([a for a in self.agents if a.is_malicious])
        
        return {
            "current_time": self.current_time.isoformat(),
            "total_agents": len(self.agents),
            "active_agents": active_agents,
            "malicious_agents": malicious_agents,
            "current_hour": self.current_time.hour,
            "is_weekend": self.current_time.weekday() >= 5,
            "work_intensity": self.hourly_intensity.get(self.current_time.hour, 0.5)
        }

# Example usage and testing
if __name__ == "__main__":
    from datetime import datetime
    from agents_enhanced import EnhancedEmployeeAgent, EnhancedMaliciousAgent
    
    print("🧪 TESTING ENHANCED SIMULATION SCHEDULER")
    print("=" * 50)
    
    # Create test agents
    agents = [
        EnhancedEmployeeAgent(1, "nguyen_van_nam", "SALES", {}),
        EnhancedEmployeeAgent(2, "tran_thi_lan", "HR", {}),
        EnhancedMaliciousAgent(3, "hacker", "ATTACKER", {})
    ]
    
    # Create scheduler
    start_time = datetime(2025, 1, 15, 9, 0, 0)  # Wednesday 9 AM
    scheduler = EnhancedSimulationScheduler(start_time, agents)
    
    # Simulate 2 hours
    print(f"Starting simulation at {start_time}")
    
    for minute in range(120):  # 2 hours
        logs = scheduler.tick(1)
        
        if logs:
            print(f"\nMinute {minute + 1}:")
            for log in logs:
                print(f"  {log['timestamp'][-8:-3]} | {log['user']} ({log['role']}) | {log['action']}")
        
        # Print stats every 30 minutes
        if (minute + 1) % 30 == 0:
            stats = scheduler.get_simulation_stats()
            print(f"\n📊 Stats at minute {minute + 1}:")
            print(f"   Active agents: {stats['active_agents']}/{stats['total_agents']}")
            print(f"   Work intensity: {stats['work_intensity']:.1f}")
    
    print(f"\n✅ Enhanced scheduler simulation completed")