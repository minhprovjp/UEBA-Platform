"""
Vietnamese Business Patterns Library

Provides Vietnamese-specific business workflow patterns, cultural constraints,
regulatory requirements, and realistic parameter generation for dynamic SQL queries.
"""

import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

from .models import (
    WorkflowType, BusinessEvent, BusinessCyclePhase, 
    CulturalConstraints, ComplianceRule, SensitivityLevel
)


@dataclass
class WorkflowPattern:
    """Vietnamese business workflow pattern"""
    workflow_type: WorkflowType
    typical_queries: List[str]
    peak_hours: List[int]
    department_interactions: List[str]
    data_access_patterns: Dict[str, float]
    cultural_considerations: List[str]


@dataclass
class TemporalPattern:
    """Vietnamese temporal business pattern"""
    pattern_name: str
    time_ranges: List[Tuple[int, int]]  # (start_hour, end_hour)
    activity_multiplier: float
    typical_operations: List[str]
    cultural_context: str


class VietnameseBusinessPatterns:
    """
    Vietnamese business patterns library providing cultural context,
    workflow patterns, and realistic parameter generation
    """
    
    def __init__(self):
        self.vietnamese_cities = [
            "Hà Nội", "Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ",
            "Nha Trang", "Vũng Tàu", "Bình Dương", "Đồng Nai", "Long An",
            "Hải Dương", "Nam Định", "Thái Nguyên", "Quảng Ninh", "Bắc Ninh"
        ]
        
        self.vietnamese_companies = [
            "Công ty TNHH Thương mại Việt Nam",
            "Công ty CP Xuất nhập khẩu ABC", 
            "Công ty TNHH Sản xuất XYZ",
            "Công ty CP Đầu tư và Phát triển",
            "Công ty TNHH Dịch vụ Logistics",
            "Công ty CP Công nghệ Thông tin",
            "Công ty TNHH May mặc Đông Nam Á",
            "Công ty CP Thực phẩm Sạch",
            "Công ty TNHH Xây dựng Miền Nam",
            "Công ty CP Du lịch Việt"
        ]
        
        self.vietnamese_business_terms = {
            "departments": {
                "sales": "Phòng Kinh Doanh",
                "marketing": "Phòng Marketing", 
                "hr": "Phòng Nhân Sự",
                "finance": "Phòng Tài Chính",
                "admin": "Phòng Hành Chính",
                "it": "Phòng Công Nghệ Thông Tin",
                "customer_service": "Phòng Chăm Sóc Khách Hàng",
                "logistics": "Phòng Logistics"
            },
            "positions": [
                "Giám Đốc", "Phó Giám Đốc", "Trưởng Phòng", "Phó Trưởng Phòng",
                "Chuyên Viên", "Nhân Viên", "Thực Tập Sinh", "Kế Toán Trưởng",
                "Kế Toán Viên", "Nhân Viên Kinh Doanh", "Nhân Viên Marketing",
                "Lập Trình Viên", "Quản Trị Hệ Thống", "Nhân Viên Hành Chính"
            ],
            "product_categories": [
                "Điện tử", "Nội thất", "Thời trang", "Đồ gia dụng", "Thực phẩm",
                "Mỹ phẩm", "Đồ chơi", "Sách", "Thiết bị văn phòng", "Dụng cụ thể thao",
                "Máy móc", "Vật liệu xây dựng", "Hóa chất", "Dược phẩm"
            ]
        }
        
        # Vietnamese holidays and important dates
        self.vietnamese_holidays = {
            "2025-01-01": "Tết Dương Lịch",
            "2025-01-29": "Tết Nguyên Đán (29/1)",
            "2025-01-30": "Tết Nguyên Đán (30/1)", 
            "2025-01-31": "Tết Nguyên Đán (31/1)",
            "2025-02-01": "Tết Nguyên Đán (1/2)",
            "2025-02-02": "Tết Nguyên Đán (2/2)",
            "2025-04-30": "Ngày Giải Phóng Miền Nam",
            "2025-05-01": "Ngày Quốc Tế Lao Động",
            "2025-09-02": "Quốc Khánh"
        }
        
        # Vietnamese work schedule patterns
        self.work_schedules = {
            "standard": {
                "start_hour": 8,
                "end_hour": 17,
                "lunch_start": 12,
                "lunch_end": 13,
                "peak_hours": [9, 10, 14, 15, 16]
            },
            "customer_service": {
                "start_hour": 7,
                "end_hour": 19,
                "lunch_start": 12,
                "lunch_end": 13,
                "peak_hours": [8, 9, 10, 14, 15, 16, 17]
            },
            "management": {
                "start_hour": 8,
                "end_hour": 19,
                "lunch_start": 12,
                "lunch_end": 13,
                "peak_hours": [9, 10, 11, 14, 15, 16, 17, 18]
            },
            "it_flexible": {
                "start_hour": 9,
                "end_hour": 18,
                "lunch_start": 12,
                "lunch_end": 13,
                "peak_hours": [10, 11, 14, 15, 16, 17]
            }
        }
        
        # Initialize workflow patterns
        self._initialize_workflow_patterns()
        self._initialize_temporal_patterns()
        self._initialize_compliance_rules()

        # Initialize name data
        self.family_names = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng"]
        self.middle_names = ["Văn", "Thị", "Minh", "Hữu", "Đức", "Quang", "Thanh", "Xuân"]
        self.given_names = ["Nam", "Lan", "Duc", "Linh", "Hoa", "Tuấn", "Mai", "Hùng", "Nga", "Sơn", "Phúc", "Hải", "Tâm", "Thảo", "Trang"]
    
    def _initialize_workflow_patterns(self):
        """Initialize Vietnamese business workflow patterns"""
        self.workflow_patterns = {
            WorkflowType.SALES_PROCESS: WorkflowPattern(
                workflow_type=WorkflowType.SALES_PROCESS,
                typical_queries=[
                    "customer_search", "order_creation", "product_inquiry",
                    "price_check", "inventory_check", "customer_update"
                ],
                peak_hours=[9, 10, 14, 15, 16],
                department_interactions=["marketing", "customer_service", "finance"],
                data_access_patterns={
                    "sales_db": 0.7,
                    "marketing_db": 0.2,
                    "inventory_db": 0.1
                },
                cultural_considerations=[
                    "respect_customer_hierarchy",
                    "formal_communication",
                    "relationship_building"
                ]
            ),
            
            WorkflowType.CUSTOMER_SERVICE: WorkflowPattern(
                workflow_type=WorkflowType.CUSTOMER_SERVICE,
                typical_queries=[
                    "ticket_creation", "customer_lookup", "order_status",
                    "complaint_handling", "support_escalation"
                ],
                peak_hours=[8, 9, 10, 14, 15, 16, 17],
                department_interactions=["sales", "technical", "management"],
                data_access_patterns={
                    "support_db": 0.6,
                    "sales_db": 0.3,
                    "customer_db": 0.1
                },
                cultural_considerations=[
                    "customer_respect",
                    "patience_emphasis",
                    "hierarchy_escalation"
                ]
            ),
            
            WorkflowType.FINANCIAL_REPORTING: WorkflowPattern(
                workflow_type=WorkflowType.FINANCIAL_REPORTING,
                typical_queries=[
                    "revenue_analysis", "expense_reporting", "budget_tracking",
                    "invoice_processing", "payment_reconciliation"
                ],
                peak_hours=[9, 10, 11, 14, 15],
                department_interactions=["sales", "hr", "management"],
                data_access_patterns={
                    "finance_db": 0.8,
                    "sales_db": 0.15,
                    "hr_db": 0.05
                },
                cultural_considerations=[
                    "accuracy_emphasis",
                    "senior_approval",
                    "confidentiality"
                ]
            ),
            
            WorkflowType.HR_MANAGEMENT: WorkflowPattern(
                workflow_type=WorkflowType.HR_MANAGEMENT,
                typical_queries=[
                    "employee_lookup", "attendance_tracking", "payroll_processing",
                    "performance_review", "recruitment"
                ],
                peak_hours=[9, 10, 14, 15, 16],
                department_interactions=["finance", "management", "all_departments"],
                data_access_patterns={
                    "hr_db": 0.7,
                    "finance_db": 0.2,
                    "admin_db": 0.1
                },
                cultural_considerations=[
                    "employee_privacy",
                    "hierarchy_respect",
                    "seniority_consideration"
                ]
            )
        }
    
    def _initialize_temporal_patterns(self):
        """Initialize Vietnamese temporal business patterns"""
        self.temporal_patterns = {
            "morning_rush": TemporalPattern(
                pattern_name="morning_rush",
                time_ranges=[(8, 10)],
                activity_multiplier=1.5,
                typical_operations=["login", "email_check", "daily_planning"],
                cultural_context="Vietnamese workers start early, check priorities"
            ),
            
            "lunch_break": TemporalPattern(
                pattern_name="lunch_break", 
                time_ranges=[(12, 13)],
                activity_multiplier=0.3,
                typical_operations=["quick_checks", "urgent_only"],
                cultural_context="Traditional Vietnamese lunch hour, reduced activity"
            ),
            
            "afternoon_peak": TemporalPattern(
                pattern_name="afternoon_peak",
                time_ranges=[(14, 16)],
                activity_multiplier=1.3,
                typical_operations=["meetings", "reports", "customer_calls"],
                cultural_context="Post-lunch productivity peak in Vietnamese culture"
            ),
            
            "end_of_day": TemporalPattern(
                pattern_name="end_of_day",
                time_ranges=[(16, 17)],
                activity_multiplier=1.1,
                typical_operations=["daily_summary", "next_day_prep", "reports"],
                cultural_context="Vietnamese work culture emphasizes day completion"
            ),
            
            "overtime": TemporalPattern(
                pattern_name="overtime",
                time_ranges=[(17, 19)],
                activity_multiplier=0.7,
                typical_operations=["urgent_tasks", "deadlines", "management_requests"],
                cultural_context="Overtime common in Vietnamese business culture"
            ),
            
            "tet_preparation": TemporalPattern(
                pattern_name="tet_preparation",
                time_ranges=[(8, 17)],
                activity_multiplier=0.8,
                typical_operations=["year_end_reports", "bonus_processing", "cleanup"],
                cultural_context="Pre-Tet business activities, cultural preparation"
            )
        }
    
    def _initialize_compliance_rules(self):
        """Initialize Vietnamese compliance and regulatory rules"""
        self.compliance_rules = [
            ComplianceRule(
                rule_id="VN_DATA_PROTECTION_001",
                description="Vietnamese Personal Data Protection Law compliance",
                applies_to_roles=("HR", "FINANCE", "ADMIN"),
                data_types=("personal_info", "salary", "contact_details")
            ),
            
            ComplianceRule(
                rule_id="VN_FINANCIAL_001", 
                description="Vietnamese accounting and financial reporting standards",
                applies_to_roles=("FINANCE", "MANAGEMENT"),
                data_types=("financial_records", "tax_data", "audit_trails")
            ),
            
            ComplianceRule(
                rule_id="VN_LABOR_001",
                description="Vietnamese Labor Code compliance for HR data",
                applies_to_roles=("HR", "MANAGEMENT"),
                data_types=("employee_records", "attendance", "performance")
            ),
            
            ComplianceRule(
                rule_id="VN_BUSINESS_001",
                description="Vietnamese business registration and operational compliance",
                applies_to_roles=("ADMIN", "MANAGEMENT"),
                data_types=("business_license", "operational_data", "regulatory_reports")
            )
        ]
    
    def get_workflow_patterns(self, department: str, time_context: Dict[str, Any]) -> List[WorkflowPattern]:
        """Get workflow patterns for Vietnamese department and time context"""
        current_hour = time_context.get('current_hour', 9)
        is_holiday = time_context.get('is_vietnamese_holiday', False)
        
        # Map department to workflow types
        dept_workflow_map = {
            "Phòng Kinh Doanh": [WorkflowType.SALES_PROCESS],
            "Phòng Marketing": [WorkflowType.SALES_PROCESS],
            "Phòng Chăm Sóc Khách Hàng": [WorkflowType.CUSTOMER_SERVICE],
            "Phòng Tài Chính": [WorkflowType.FINANCIAL_REPORTING],
            "Phòng Nhân Sự": [WorkflowType.HR_MANAGEMENT],
            "Phòng Hành Chính": [WorkflowType.ADMINISTRATIVE],
            "Ban Giám Đốc": [WorkflowType.FINANCIAL_REPORTING, WorkflowType.HR_MANAGEMENT]
        }
        
        workflow_types = dept_workflow_map.get(department, [WorkflowType.ADMINISTRATIVE])
        patterns = []
        
        for workflow_type in workflow_types:
            if workflow_type in self.workflow_patterns:
                pattern = self.workflow_patterns[workflow_type]
                
                # Adjust pattern based on time context
                if is_holiday:
                    # Reduce activity during holidays
                    adjusted_pattern = WorkflowPattern(
                        workflow_type=pattern.workflow_type,
                        typical_queries=pattern.typical_queries[:2],  # Fewer queries
                        peak_hours=[],  # No peak hours during holidays
                        department_interactions=pattern.department_interactions,
                        data_access_patterns=pattern.data_access_patterns,
                        cultural_considerations=pattern.cultural_considerations + ["holiday_reduced_activity"]
                    )
                    patterns.append(adjusted_pattern)
                else:
                    patterns.append(pattern)
        
        return patterns
    
    def get_cultural_constraints(self, action: str, time_context: Dict[str, Any]) -> CulturalConstraints:
        """Get Vietnamese cultural constraints for specific action and time"""
        current_hour = time_context.get('current_hour', 9)
        is_holiday = time_context.get('is_vietnamese_holiday', False)
        is_tet_season = time_context.get('is_tet_season', False)
        
        # Base cultural constraints
        hierarchy_level = 5  # Medium hierarchy respect
        respect_seniority = True
        work_overtime_acceptable = True
        tet_preparation_mode = is_tet_season
        
        # Adjust based on action type
        if action in ["financial_report", "audit", "management_decision"]:
            hierarchy_level = 8  # High hierarchy for important decisions
            respect_seniority = True
        elif action in ["customer_service", "sales_call"]:
            hierarchy_level = 6  # Customer respect important
        elif action in ["routine_data_entry", "basic_lookup"]:
            hierarchy_level = 3  # Lower hierarchy for routine tasks
        
        # Adjust based on time
        if current_hour < 8 or current_hour > 17:
            work_overtime_acceptable = current_hour <= 19  # Acceptable until 7 PM
        
        if is_holiday:
            hierarchy_level = max(hierarchy_level, 7)  # Higher respect during holidays
            work_overtime_acceptable = False
        
        return CulturalConstraints(
            hierarchy_level=hierarchy_level,
            respect_seniority=respect_seniority,
            work_overtime_acceptable=work_overtime_acceptable,
            tet_preparation_mode=tet_preparation_mode
        )
    
    def get_regulatory_requirements(self, database: str, data_type: str) -> List[ComplianceRule]:
        """Get Vietnamese regulatory requirements for database and data type"""
        applicable_rules = []
        
        for rule in self.compliance_rules:
            if data_type in rule.data_types:
                applicable_rules.append(rule)
        
        # Add database-specific rules
        if database == "hr_db":
            applicable_rules.extend([
                rule for rule in self.compliance_rules 
                if rule.rule_id.startswith("VN_LABOR") or rule.rule_id.startswith("VN_DATA_PROTECTION")
            ])
        elif database == "finance_db":
            applicable_rules.extend([
                rule for rule in self.compliance_rules
                if rule.rule_id.startswith("VN_FINANCIAL")
            ])
        
        return list(set(applicable_rules))  # Remove duplicates
    
    def get_vietnamese_business_data(self, data_type: str, count: int = 1) -> List[str]:
        """Generate realistic Vietnamese business data"""
        if data_type == "city":
            return random.choices(self.vietnamese_cities, k=count)
        elif data_type == "company":
            return random.choices(self.vietnamese_companies, k=count)
        elif data_type == "department":
            return random.choices(list(self.vietnamese_business_terms["departments"].values()), k=count)
        elif data_type == "position":
            return random.choices(self.vietnamese_business_terms["positions"], k=count)
        elif data_type == "product_category":
            return random.choices(self.vietnamese_business_terms["product_categories"], k=count)
        else:
            return [f"vietnamese_{data_type}_{i}" for i in range(count)]
    
    def get_temporal_pattern(self, current_hour: int, is_holiday: bool = False) -> Optional[TemporalPattern]:
        """Get current temporal pattern based on Vietnamese business hours"""
        if is_holiday:
            return TemporalPattern(
                pattern_name="holiday",
                time_ranges=[(0, 23)],
                activity_multiplier=0.1,
                typical_operations=["emergency_only"],
                cultural_context="Vietnamese holiday - minimal business activity"
            )
        
        # Find matching temporal pattern
        for pattern in self.temporal_patterns.values():
            for start_hour, end_hour in pattern.time_ranges:
                if start_hour <= current_hour < end_hour:
                    return pattern
        
        # Default pattern for off-hours
        return TemporalPattern(
            pattern_name="off_hours",
            time_ranges=[(current_hour, current_hour + 1)],
            activity_multiplier=0.2,
            typical_operations=["maintenance", "security_check"],
            cultural_context="Outside Vietnamese business hours"
        )
    
    def is_vietnamese_holiday(self, date_str: str) -> bool:
        """Check if date is a Vietnamese holiday"""
        return date_str in self.vietnamese_holidays
    
    def get_holiday_name(self, date_str: str) -> Optional[str]:
        """Get Vietnamese holiday name for date"""
        return self.vietnamese_holidays.get(date_str)
    
    def get_business_cycle_phase(self, current_month: int) -> BusinessCyclePhase:
        """Determine Vietnamese business cycle phase"""
        if current_month in [1, 2]:  # Tet season
            return BusinessCyclePhase.HOLIDAY_SEASON
        elif current_month in [3, 4, 5]:  # Post-Tet recovery
            return BusinessCyclePhase.TRANSITION
        elif current_month in [6, 7, 8, 9]:  # Peak business season
            return BusinessCyclePhase.PEAK_SEASON
        elif current_month in [10, 11]:  # Preparation for year-end
            return BusinessCyclePhase.TRANSITION
        else:  # December - year-end activities
            return BusinessCyclePhase.LOW_SEASON
    
    def get_work_schedule(self, role: str) -> Dict[str, Any]:
        """Get work schedule based on Vietnamese role"""
        role_schedule_map = {
            "CUSTOMER_SERVICE": "customer_service",
            "MANAGEMENT": "management", 
            "DEV": "it_flexible",
            "ADMIN": "it_flexible"
        }
        
        schedule_type = role_schedule_map.get(role, "standard")
        return self.work_schedules[schedule_type].copy()
    
    def generate_realistic_parameters(self, query_type: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Generate realistic Vietnamese business parameters for queries"""
        params = {}
        
        # Common Vietnamese business parameters
        if "customer" in query_type.lower():
            params.update({
                "city": random.choice(self.vietnamese_cities),
                "company_name": random.choice(self.vietnamese_companies),
                "contact_person": self._generate_vietnamese_name()
            })
        
        if "product" in query_type.lower():
            params.update({
                "category": random.choice(self.vietnamese_business_terms["product_categories"]),
                "price": random.randint(50000, 5000000),  # Vietnamese dong
                "quantity": random.randint(1, 100)
            })
        
        if "employee" in query_type.lower():
            params.update({
                "department": random.choice(list(self.vietnamese_business_terms["departments"].values())),
                "position": random.choice(self.vietnamese_business_terms["positions"]),
                "salary": random.randint(8000000, 50000000)  # Vietnamese dong
            })
        
        # Time-based parameters
        current_hour = context.get('current_hour', 9)
        if 8 <= current_hour <= 17:
            params["priority"] = "normal"
        elif current_hour > 17:
            params["priority"] = "urgent"  # Overtime work
        else:
            params["priority"] = "maintenance"
        
        return params
    


    def get_random_given_name(self) -> str:
        """Get a random Vietnamese given name"""
        return random.choice(self.given_names)

    def _generate_vietnamese_name(self) -> str:
        """Generate realistic Vietnamese name"""
        family = random.choice(self.family_names)
        middle = random.choice(self.middle_names)
        given = random.choice(self.given_names)
        
        return f"{family} {middle} {given}"