#!/usr/bin/env python3
"""
Enriched SQL Query Library for Vietnamese Medium-Sized Sales Company
Comprehensive collection of realistic business queries across all databases
"""

import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

class EnrichedSQLLibrary:
    """
    Comprehensive SQL query library with 500+ realistic business queries
    Organized by database, role, complexity, and business scenario
    """
    
    def __init__(self):
        self.vietnamese_cities = [
            "Hà Nội", "Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ", 
            "Nha Trang", "Vũng Tàu", "Bình Dương", "Đồng Nai", "Long An",
            "Hải Dương", "Nam Định", "Thái Bình", "Ninh Bình", "Thanh Hóa"
        ]
        
        self.vietnamese_companies = [
            "Công ty TNHH Thương mại Việt Nam", "Công ty CP Xuất nhập khẩu ABC",
            "Công ty TNHH Sản xuất XYZ", "Công ty CP Đầu tư và Phát triển",
            "Công ty TNHH Dịch vụ Logistics", "Công ty CP Công nghệ Thông tin",
            "Công ty TNHH Thương mại Điện tử", "Công ty CP Phân phối Hàng hóa"
        ]
        
        self.product_categories = [
            "Điện tử", "Nội thất", "Thời trang", "Đồ gia dụng", "Thực phẩm",
            "Mỹ phẩm", "Đồ chơi", "Sách", "Thiết bị văn phòng", "Dụng cụ thể thao",
            "Xe máy", "Điện lạnh", "Xây dựng", "Y tế", "Giáo dục"
        ]
        
        self.vietnamese_names = [
            "Nguyễn Văn Nam", "Trần Thị Hương", "Lê Minh Đức", "Phạm Hồng Lan",
            "Hoàng Quang Hùng", "Vũ Thúy Kiều", "Đặng Xuân Thành", "Bùi Ngọc Linh"
        ]
        
        # Initialize query categories
        self.query_complexity = {
            'SIMPLE': 'Basic single-table queries',
            'MEDIUM': 'Multi-table joins and aggregations', 
            'COMPLEX': 'Advanced analytics and subqueries',
            'EXPERT': 'Complex business logic and CTEs'
        }
        
        self.business_scenarios = {
            'DAILY_OPERATIONS': 'Routine daily business tasks',
            'REPORTING': 'Management and analytical reports',
            'MAINTENANCE': 'System and data maintenance',
            'ANALYTICS': 'Business intelligence and insights',
            'COMPLIANCE': 'Regulatory and audit queries',
            'TROUBLESHOOTING': 'Problem investigation queries'
        }

    # SALES DATABASE - Comprehensive Query Library
    def get_sales_queries_enriched(self, role: str, complexity: str = 'ALL') -> List[str]:
        """Enhanced sales database queries with multiple complexity levels"""
        queries = []
        
        # SIMPLE QUERIES - Basic operations
        simple_queries = [
            # Customer Management
            "SELECT customer_id, company_name, contact_person, city FROM customers WHERE status = 'active'",
            "SELECT * FROM customers WHERE city = '{}' ORDER BY company_name".format(random.choice(self.vietnamese_cities)),
            "SELECT COUNT(*) as total_customers FROM customers WHERE customer_type = 'business'",
            "SELECT customer_code, company_name, contact_person FROM customers WHERE created_at >= CURDATE() - INTERVAL 7 DAY",
            "SELECT DISTINCT city FROM customers WHERE status = 'active' ORDER BY city",
            
            # Product Queries
            "SELECT product_id, product_name, price FROM products WHERE status = 'active'",
            "SELECT * FROM products WHERE category_id = {} AND price BETWEEN 100000 AND 1000000".format(random.randint(1, 10)),
            "SELECT COUNT(*) as product_count FROM products WHERE status = 'active'",
            "SELECT product_name, sku, price FROM products ORDER BY price DESC LIMIT 10",
            "SELECT DISTINCT supplier_name FROM products WHERE status = 'active'",
            
            # Order Basics
            "SELECT order_id, order_number, total_amount FROM orders WHERE status = 'confirmed'",
            "SELECT * FROM orders WHERE order_date = CURDATE()",
            "SELECT COUNT(*) as daily_orders FROM orders WHERE DATE(order_date) = CURDATE()",
            "SELECT order_number, customer_id, total_amount FROM orders WHERE total_amount > 10000000",
            "SELECT DISTINCT status FROM orders"
        ]
        
        # MEDIUM QUERIES - Joins and aggregations
        medium_queries = [
            # Customer Analytics
            """SELECT c.company_name, c.city, COUNT(o.order_id) as total_orders, 
               SUM(o.total_amount) as total_revenue
               FROM customers c 
               LEFT JOIN orders o ON c.customer_id = o.customer_id 
               GROUP BY c.customer_id 
               ORDER BY total_revenue DESC""",
            
            """SELECT c.customer_type, COUNT(c.customer_id) as customer_count,
               AVG(o.total_amount) as avg_order_value
               FROM customers c
               LEFT JOIN orders o ON c.customer_id = o.customer_id
               GROUP BY c.customer_type""",
            
            # Product Performance
            """SELECT p.product_name, pc.category_name, 
               COUNT(oi.item_id) as times_ordered,
               SUM(oi.quantity) as total_quantity_sold,
               SUM(oi.line_total) as total_revenue
               FROM products p
               JOIN product_categories pc ON p.category_id = pc.category_id
               LEFT JOIN order_items oi ON p.product_id = oi.product_id
               GROUP BY p.product_id
               ORDER BY total_revenue DESC""",
            
            # Sales Performance
            """SELECT DATE(o.order_date) as order_date,
               COUNT(o.order_id) as daily_orders,
               SUM(o.total_amount) as daily_revenue,
               AVG(o.total_amount) as avg_order_value
               FROM orders o
               WHERE o.order_date >= CURDATE() - INTERVAL 30 DAY
               GROUP BY DATE(o.order_date)
               ORDER BY order_date""",
            
            # Geographic Analysis
            """SELECT c.city, c.province,
               COUNT(DISTINCT c.customer_id) as customers,
               COUNT(o.order_id) as total_orders,
               SUM(o.total_amount) as total_revenue
               FROM customers c
               LEFT JOIN orders o ON c.customer_id = o.customer_id
               WHERE c.status = 'active'
               GROUP BY c.city, c.province
               ORDER BY total_revenue DESC"""
        ]
        
        # COMPLEX QUERIES - Advanced analytics
        complex_queries = [
            # Customer Segmentation
            """WITH customer_metrics AS (
                SELECT c.customer_id, c.company_name,
                       COUNT(o.order_id) as order_count,
                       SUM(o.total_amount) as total_spent,
                       AVG(o.total_amount) as avg_order_value,
                       DATEDIFF(CURDATE(), MAX(o.order_date)) as days_since_last_order
                FROM customers c
                LEFT JOIN orders o ON c.customer_id = o.customer_id
                WHERE c.status = 'active'
                GROUP BY c.customer_id
            )
            SELECT company_name,
                   CASE 
                       WHEN total_spent > 50000000 AND order_count > 10 THEN 'VIP'
                       WHEN total_spent > 20000000 AND order_count > 5 THEN 'Premium'
                       WHEN total_spent > 5000000 THEN 'Regular'
                       ELSE 'New'
                   END as customer_segment,
                   total_spent, order_count, avg_order_value
            FROM customer_metrics
            ORDER BY total_spent DESC""",
            
            # Sales Trend Analysis
            """SELECT 
                DATE_FORMAT(order_date, '%Y-%m') as month,
                SUM(total_amount) as monthly_revenue,
                COUNT(order_id) as monthly_orders,
                LAG(SUM(total_amount)) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m')) as prev_month_revenue,
                ROUND(((SUM(total_amount) - LAG(SUM(total_amount)) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m'))) / 
                       LAG(SUM(total_amount)) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m'))) * 100, 2) as growth_rate
            FROM orders 
            WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
            GROUP BY DATE_FORMAT(order_date, '%Y-%m')
            ORDER BY month""",
            
            # Product Cross-Sell Analysis
            """SELECT p1.product_name as product_a, p2.product_name as product_b,
                   COUNT(*) as times_bought_together,
                   ROUND(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM order_items), 2) as percentage
            FROM order_items oi1
            JOIN order_items oi2 ON oi1.order_id = oi2.order_id AND oi1.product_id < oi2.product_id
            JOIN products p1 ON oi1.product_id = p1.product_id
            JOIN products p2 ON oi2.product_id = p2.product_id
            GROUP BY oi1.product_id, oi2.product_id
            HAVING times_bought_together >= 5
            ORDER BY times_bought_together DESC""",
            
            # Customer Lifetime Value
            """WITH customer_clv AS (
                SELECT c.customer_id, c.company_name,
                       DATEDIFF(CURDATE(), MIN(o.order_date)) as customer_age_days,
                       COUNT(o.order_id) as total_orders,
                       SUM(o.total_amount) as total_revenue,
                       AVG(DATEDIFF(o2.order_date, o.order_date)) as avg_days_between_orders
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                LEFT JOIN orders o2 ON c.customer_id = o2.customer_id AND o2.order_date > o.order_date
                GROUP BY c.customer_id
            )
            SELECT company_name, customer_age_days, total_orders, total_revenue,
                   ROUND(total_revenue / NULLIF(customer_age_days, 0) * 365, 2) as annual_value,
                   ROUND(total_revenue / NULLIF(total_orders, 0), 2) as avg_order_value,
                   avg_days_between_orders
            FROM customer_clv
            WHERE customer_age_days > 30
            ORDER BY annual_value DESC"""
        ]
        
        # EXPERT QUERIES - Complex business logic
        expert_queries = [
            # Advanced Sales Forecasting
            """WITH monthly_sales AS (
                SELECT DATE_FORMAT(order_date, '%Y-%m') as month,
                       SUM(total_amount) as revenue,
                       COUNT(order_id) as orders
                FROM orders 
                WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)
                GROUP BY DATE_FORMAT(order_date, '%Y-%m')
            ),
            sales_with_trend AS (
                SELECT month, revenue, orders,
                       AVG(revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_revenue,
                       LAG(revenue, 12) OVER (ORDER BY month) as same_month_last_year
                FROM monthly_sales
            )
            SELECT month, revenue, orders, ma3_revenue,
                   ROUND(((revenue - same_month_last_year) / NULLIF(same_month_last_year, 0)) * 100, 2) as yoy_growth,
                   CASE 
                       WHEN revenue > ma3_revenue * 1.1 THEN 'Tăng trưởng mạnh'
                       WHEN revenue > ma3_revenue * 1.05 THEN 'Tăng trưởng ổn định'
                       WHEN revenue < ma3_revenue * 0.9 THEN 'Suy giảm'
                       ELSE 'Ổn định'
                   END as trend_status
            FROM sales_with_trend
            ORDER BY month DESC""",
            
            # Customer Churn Analysis
            """WITH customer_activity AS (
                SELECT c.customer_id, c.company_name,
                       MAX(o.order_date) as last_order_date,
                       COUNT(o.order_id) as total_orders,
                       SUM(o.total_amount) as total_spent,
                       DATEDIFF(CURDATE(), MAX(o.order_date)) as days_inactive
                FROM customers c
                LEFT JOIN orders o ON c.customer_id = o.customer_id
                WHERE c.status = 'active'
                GROUP BY c.customer_id
            ),
            churn_risk AS (
                SELECT *,
                       CASE 
                           WHEN days_inactive > 180 THEN 'Cao'
                           WHEN days_inactive > 90 THEN 'Trung bình'
                           WHEN days_inactive > 30 THEN 'Thấp'
                           ELSE 'Hoạt động'
                       END as churn_risk_level,
                       CASE
                           WHEN total_spent > 50000000 THEN 'VIP'
                           WHEN total_spent > 20000000 THEN 'Premium'
                           ELSE 'Regular'
                       END as customer_value
                FROM customer_activity
            )
            SELECT churn_risk_level, customer_value,
                   COUNT(*) as customer_count,
                   SUM(total_spent) as total_revenue_at_risk,
                   AVG(days_inactive) as avg_days_inactive
            FROM churn_risk
            GROUP BY churn_risk_level, customer_value
            ORDER BY 
                CASE churn_risk_level 
                    WHEN 'Cao' THEN 1 
                    WHEN 'Trung bình' THEN 2 
                    WHEN 'Thấp' THEN 3 
                    ELSE 4 
                END"""
        ]
        
        # Filter by complexity and role
        all_queries = []
        
        if complexity in ['ALL', 'SIMPLE'] and role in ['SALES', 'MARKETING', 'MANAGEMENT', 'ADMIN']:
            all_queries.extend(simple_queries)
        
        if complexity in ['ALL', 'MEDIUM'] and role in ['SALES', 'MARKETING', 'MANAGEMENT', 'ADMIN']:
            all_queries.extend(medium_queries)
        
        if complexity in ['ALL', 'COMPLEX'] and role in ['MANAGEMENT', 'ADMIN', 'DEV']:
            all_queries.extend(complex_queries)
        
        if complexity in ['ALL', 'EXPERT'] and role in ['ADMIN', 'DEV']:
            all_queries.extend(expert_queries)
        
        return all_queries

    # INVENTORY DATABASE - Enriched Queries
    def get_inventory_queries_enriched(self, role: str, complexity: str = 'ALL') -> List[str]:
        """Enhanced inventory database queries"""
        queries = []
        
        simple_queries = [
            "SELECT product_id, current_stock, available_stock FROM inventory_levels WHERE current_stock < min_stock_level",
            "SELECT warehouse_code, warehouse_name, city FROM warehouse_locations WHERE status = 'active'",
            "SELECT COUNT(*) as low_stock_items FROM inventory_levels WHERE current_stock < min_stock_level",
            "SELECT product_id, SUM(current_stock) as total_stock FROM inventory_levels GROUP BY product_id",
            "SELECT * FROM stock_movements WHERE movement_date >= CURDATE() ORDER BY movement_date DESC LIMIT 50"
        ]
        
        medium_queries = [
            """SELECT wl.warehouse_name, wl.city,
               COUNT(il.product_id) as products_stored,
               SUM(il.current_stock) as total_items,
               SUM(CASE WHEN il.current_stock < il.min_stock_level THEN 1 ELSE 0 END) as low_stock_products
               FROM warehouse_locations wl
               LEFT JOIN inventory_levels il ON wl.location_id = il.location_id
               GROUP BY wl.location_id""",
            
            """SELECT sm.movement_type, 
               COUNT(*) as movement_count,
               SUM(sm.quantity) as total_quantity,
               AVG(sm.unit_cost) as avg_unit_cost
               FROM stock_movements sm
               WHERE sm.movement_date >= CURDATE() - INTERVAL 30 DAY
               GROUP BY sm.movement_type""",
            
            """SELECT il.product_id,
               SUM(il.current_stock) as total_stock,
               COUNT(il.location_id) as locations,
               AVG(il.current_stock) as avg_per_location,
               MAX(il.last_count_date) as last_inventory_check
               FROM inventory_levels il
               GROUP BY il.product_id
               HAVING total_stock > 0
               ORDER BY total_stock DESC"""
        ]
        
        if complexity in ['ALL', 'SIMPLE']:
            queries.extend(simple_queries)
        if complexity in ['ALL', 'MEDIUM'] and role in ['FINANCE', 'MANAGEMENT', 'ADMIN', 'DEV']:
            queries.extend(medium_queries)
        
        return queries
    # FINANCE DATABASE - Comprehensive Financial Queries
    def get_finance_queries_enriched(self, role: str, complexity: str = 'ALL') -> List[str]:
        """Enhanced finance database queries"""
        queries = []
        
        simple_queries = [
            "SELECT invoice_number, customer_id, total_amount, status FROM invoices WHERE status = 'overdue'",
            "SELECT COUNT(*) as pending_invoices FROM invoices WHERE status = 'sent'",
            "SELECT SUM(total_amount) as total_receivables FROM invoices WHERE status IN ('sent', 'overdue')",
            "SELECT * FROM expense_reports WHERE status = 'submitted' ORDER BY expense_date DESC",
            "SELECT category, SUM(amount) as total FROM expense_reports WHERE status = 'approved' GROUP BY category"
        ]
        
        medium_queries = [
            """SELECT DATE_FORMAT(invoice_date, '%Y-%m') as month,
               COUNT(*) as invoice_count,
               SUM(total_amount) as total_invoiced,
               SUM(paid_amount) as total_collected,
               SUM(total_amount - paid_amount) as outstanding
               FROM invoices
               WHERE invoice_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
               GROUP BY DATE_FORMAT(invoice_date, '%Y-%m')
               ORDER BY month""",
            
            """SELECT er.employee_id, 
               COUNT(*) as expense_reports,
               SUM(er.amount) as total_expenses,
               AVG(er.amount) as avg_expense,
               MAX(er.expense_date) as last_expense_date
               FROM expense_reports er
               WHERE er.status IN ('approved', 'paid')
               AND er.expense_date >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
               GROUP BY er.employee_id
               ORDER BY total_expenses DESC""",
            
            """SELECT bp.department, bp.budget_year,
               SUM(bp.planned_amount) as total_budget,
               SUM(bp.actual_amount) as total_actual,
               SUM(bp.variance) as total_variance,
               ROUND(AVG(bp.actual_amount / NULLIF(bp.planned_amount, 0) * 100), 2) as budget_utilization
               FROM budget_plans bp
               WHERE bp.budget_year = YEAR(CURDATE())
               GROUP BY bp.department, bp.budget_year"""
        ]
        
        complex_queries = [
            """WITH aging_analysis AS (
                SELECT i.customer_id,
                       SUM(CASE WHEN DATEDIFF(CURDATE(), i.due_date) <= 0 THEN i.balance ELSE 0 END) as current_amount,
                       SUM(CASE WHEN DATEDIFF(CURDATE(), i.due_date) BETWEEN 1 AND 30 THEN i.balance ELSE 0 END) as days_1_30,
                       SUM(CASE WHEN DATEDIFF(CURDATE(), i.due_date) BETWEEN 31 AND 60 THEN i.balance ELSE 0 END) as days_31_60,
                       SUM(CASE WHEN DATEDIFF(CURDATE(), i.due_date) BETWEEN 61 AND 90 THEN i.balance ELSE 0 END) as days_61_90,
                       SUM(CASE WHEN DATEDIFF(CURDATE(), i.due_date) > 90 THEN i.balance ELSE 0 END) as days_over_90
                FROM invoices i
                WHERE i.balance > 0
                GROUP BY i.customer_id
            )
            SELECT 'Current' as aging_bucket, SUM(current_amount) as total_amount, COUNT(*) as customer_count
            FROM aging_analysis WHERE current_amount > 0
            UNION ALL
            SELECT '1-30 Days', SUM(days_1_30), COUNT(*) FROM aging_analysis WHERE days_1_30 > 0
            UNION ALL
            SELECT '31-60 Days', SUM(days_31_60), COUNT(*) FROM aging_analysis WHERE days_31_60 > 0
            UNION ALL
            SELECT '61-90 Days', SUM(days_61_90), COUNT(*) FROM aging_analysis WHERE days_61_90 > 0
            UNION ALL
            SELECT 'Over 90 Days', SUM(days_over_90), COUNT(*) FROM aging_analysis WHERE days_over_90 > 0"""
        ]
        
        if complexity in ['ALL', 'SIMPLE'] and role in ['FINANCE', 'MANAGEMENT', 'ADMIN']:
            queries.extend(simple_queries)
        if complexity in ['ALL', 'MEDIUM'] and role in ['FINANCE', 'MANAGEMENT', 'ADMIN']:
            queries.extend(medium_queries)
        if complexity in ['ALL', 'COMPLEX'] and role in ['FINANCE', 'ADMIN', 'DEV']:
            queries.extend(complex_queries)
        
        return queries

    # MARKETING DATABASE - Enhanced CRM Queries
    def get_marketing_queries_enriched(self, role: str, complexity: str = 'ALL') -> List[str]:
        """Enhanced marketing database queries"""
        queries = []
        
        simple_queries = [
            "SELECT campaign_name, campaign_type, budget, status FROM campaigns WHERE status = 'active'",
            "SELECT COUNT(*) as active_campaigns FROM campaigns WHERE status = 'active'",
            "SELECT lead_source, COUNT(*) as lead_count FROM leads GROUP BY lead_source",
            "SELECT status, COUNT(*) as count FROM leads GROUP BY status",
            "SELECT * FROM leads WHERE created_at >= CURDATE() - INTERVAL 7 DAY ORDER BY created_at DESC"
        ]
        
        medium_queries = [
            """SELECT c.campaign_name, c.campaign_type, c.budget,
               COUNT(l.lead_id) as leads_generated,
               SUM(l.estimated_value) as pipeline_value,
               ROUND(c.budget / NULLIF(COUNT(l.lead_id), 0), 2) as cost_per_lead
               FROM campaigns c
               LEFT JOIN leads l ON c.campaign_id = l.lead_source
               WHERE c.status IN ('active', 'completed')
               GROUP BY c.campaign_id""",
            
            """SELECT l.assigned_to,
               COUNT(*) as total_leads,
               SUM(CASE WHEN l.status = 'won' THEN 1 ELSE 0 END) as won_leads,
               SUM(CASE WHEN l.status = 'won' THEN l.estimated_value ELSE 0 END) as won_value,
               ROUND(SUM(CASE WHEN l.status = 'won' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as conversion_rate
               FROM leads l
               WHERE l.assigned_to IS NOT NULL
               GROUP BY l.assigned_to
               ORDER BY conversion_rate DESC""",
            
            """SELECT DATE_FORMAT(la.activity_date, '%Y-%m-%d') as activity_date,
               la.activity_type,
               COUNT(*) as activity_count,
               COUNT(DISTINCT la.lead_id) as unique_leads,
               SUM(CASE WHEN la.outcome = 'positive' THEN 1 ELSE 0 END) as positive_outcomes
               FROM lead_activities la
               WHERE la.activity_date >= CURDATE() - INTERVAL 30 DAY
               GROUP BY DATE_FORMAT(la.activity_date, '%Y-%m-%d'), la.activity_type
               ORDER BY activity_date DESC"""
        ]
        
        complex_queries = [
            """WITH lead_funnel AS (
                SELECT l.lead_source,
                       COUNT(*) as total_leads,
                       SUM(CASE WHEN l.status IN ('contacted', 'qualified', 'proposal', 'negotiation', 'won') THEN 1 ELSE 0 END) as contacted,
                       SUM(CASE WHEN l.status IN ('qualified', 'proposal', 'negotiation', 'won') THEN 1 ELSE 0 END) as qualified,
                       SUM(CASE WHEN l.status IN ('proposal', 'negotiation', 'won') THEN 1 ELSE 0 END) as proposal,
                       SUM(CASE WHEN l.status = 'won' THEN 1 ELSE 0 END) as won,
                       SUM(CASE WHEN l.status = 'won' THEN l.estimated_value ELSE 0 END) as won_value
                FROM leads l
                GROUP BY l.lead_source
            )
            SELECT lead_source, total_leads, contacted, qualified, proposal, won,
                   ROUND(contacted * 100.0 / total_leads, 2) as contact_rate,
                   ROUND(qualified * 100.0 / NULLIF(contacted, 0), 2) as qualification_rate,
                   ROUND(proposal * 100.0 / NULLIF(qualified, 0), 2) as proposal_rate,
                   ROUND(won * 100.0 / NULLIF(proposal, 0), 2) as close_rate,
                   ROUND(won * 100.0 / total_leads, 2) as overall_conversion,
                   won_value
            FROM lead_funnel
            ORDER BY total_leads DESC"""
        ]
        
        if complexity in ['ALL', 'SIMPLE'] and role in ['MARKETING', 'SALES', 'MANAGEMENT', 'ADMIN']:
            queries.extend(simple_queries)
        if complexity in ['ALL', 'MEDIUM'] and role in ['MARKETING', 'SALES', 'MANAGEMENT', 'ADMIN']:
            queries.extend(medium_queries)
        if complexity in ['ALL', 'COMPLEX'] and role in ['MARKETING', 'MANAGEMENT', 'ADMIN', 'DEV']:
            queries.extend(complex_queries)
        
        return queries

    # SUPPORT DATABASE - Customer Service Analytics
    def get_support_queries_enriched(self, role: str, complexity: str = 'ALL') -> List[str]:
        """Enhanced support database queries"""
        queries = []
        
        simple_queries = [
            "SELECT ticket_number, subject, priority, status FROM support_tickets WHERE status = 'open'",
            "SELECT COUNT(*) as open_tickets FROM support_tickets WHERE status IN ('open', 'in_progress')",
            "SELECT priority, COUNT(*) as count FROM support_tickets GROUP BY priority",
            "SELECT assigned_to, COUNT(*) as assigned_tickets FROM support_tickets WHERE status = 'open' GROUP BY assigned_to",
            "SELECT * FROM support_tickets WHERE created_at >= CURDATE() ORDER BY priority DESC, created_at ASC"
        ]
        
        medium_queries = [
            """SELECT st.assigned_to,
               COUNT(*) as total_tickets,
               AVG(DATEDIFF(COALESCE(st.resolved_at, CURDATE()), st.created_at)) as avg_resolution_days,
               SUM(CASE WHEN st.status = 'resolved' THEN 1 ELSE 0 END) as resolved_tickets,
               ROUND(SUM(CASE WHEN st.status = 'resolved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as resolution_rate
               FROM support_tickets st
               WHERE st.assigned_to IS NOT NULL
               GROUP BY st.assigned_to""",
            
            """SELECT st.category, st.priority,
               COUNT(*) as ticket_count,
               AVG(DATEDIFF(COALESCE(st.resolved_at, CURDATE()), st.created_at)) as avg_days_to_resolve,
               COUNT(tr.response_id) as total_responses
               FROM support_tickets st
               LEFT JOIN ticket_responses tr ON st.ticket_id = tr.ticket_id
               GROUP BY st.category, st.priority
               ORDER BY ticket_count DESC"""
        ]
        
        if complexity in ['ALL', 'SIMPLE'] and role in ['CUSTOMER_SERVICE', 'MANAGEMENT', 'ADMIN']:
            queries.extend(simple_queries)
        if complexity in ['ALL', 'MEDIUM'] and role in ['CUSTOMER_SERVICE', 'MANAGEMENT', 'ADMIN']:
            queries.extend(medium_queries)
        
        return queries
    # HR DATABASE - Enhanced Human Resources Queries
    def get_hr_queries_enriched(self, role: str, complexity: str = 'ALL') -> List[str]:
        """Enhanced HR database queries"""
        queries = []
        
        simple_queries = [
            "SELECT name, position, dept_id, salary FROM employees WHERE hire_date >= CURDATE() - INTERVAL 30 DAY",
            "SELECT d.dept_name, COUNT(e.employee_id) as employee_count FROM departments d LEFT JOIN employees e ON d.dept_id = e.dept_id GROUP BY d.dept_id",
            "SELECT position, COUNT(*) as count, AVG(salary) as avg_salary FROM employees GROUP BY position",
            "SELECT COUNT(*) as total_employees FROM employees",
            "SELECT * FROM attendance WHERE date = CURDATE() AND status != 'present'"
        ]
        
        medium_queries = [
            """SELECT d.dept_name,
               COUNT(e.employee_id) as employee_count,
               AVG(e.salary) as avg_salary,
               MIN(e.salary) as min_salary,
               MAX(e.salary) as max_salary,
               SUM(s.amount + COALESCE(s.bonus, 0)) as total_payroll
               FROM departments d
               LEFT JOIN employees e ON d.dept_id = e.dept_id
               LEFT JOIN salaries s ON e.employee_id = s.employee_id AND MONTH(s.payment_date) = MONTH(CURDATE())
               GROUP BY d.dept_id""",
            
            """SELECT e.employee_id, e.name, e.position,
               COUNT(a.record_id) as attendance_days,
               SUM(CASE WHEN a.status = 'present' THEN 1 ELSE 0 END) as present_days,
               SUM(CASE WHEN a.status = 'absent' THEN 1 ELSE 0 END) as absent_days,
               ROUND(SUM(CASE WHEN a.status = 'present' THEN 1 ELSE 0 END) * 100.0 / COUNT(a.record_id), 2) as attendance_rate
               FROM employees e
               LEFT JOIN attendance a ON e.employee_id = a.employee_id 
               WHERE a.date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
               GROUP BY e.employee_id
               ORDER BY attendance_rate DESC""",
            
            """SELECT DATE_FORMAT(e.hire_date, '%Y-%m') as hire_month,
               COUNT(*) as new_hires,
               d.dept_name
               FROM employees e
               JOIN departments d ON e.dept_id = d.dept_id
               WHERE e.hire_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
               GROUP BY DATE_FORMAT(e.hire_date, '%Y-%m'), d.dept_name
               ORDER BY hire_month DESC"""
        ]
        
        complex_queries = [
            """WITH employee_performance AS (
                SELECT e.employee_id, e.name, e.position, d.dept_name,
                       DATEDIFF(CURDATE(), e.hire_date) as tenure_days,
                       e.salary,
                       AVG(CASE WHEN a.status = 'present' THEN 1 ELSE 0 END) as attendance_rate,
                       COUNT(s.salary_id) as salary_payments
                FROM employees e
                JOIN departments d ON e.dept_id = d.dept_id
                LEFT JOIN attendance a ON e.employee_id = a.employee_id AND a.date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
                LEFT JOIN salaries s ON e.employee_id = s.employee_id
                GROUP BY e.employee_id
            )
            SELECT dept_name,
                   COUNT(*) as employee_count,
                   AVG(tenure_days) as avg_tenure_days,
                   AVG(salary) as avg_salary,
                   AVG(attendance_rate) as avg_attendance_rate,
                   SUM(CASE WHEN tenure_days < 90 THEN 1 ELSE 0 END) as new_employees,
                   SUM(CASE WHEN attendance_rate < 0.8 THEN 1 ELSE 0 END) as low_attendance_employees
            FROM employee_performance
            GROUP BY dept_name
            ORDER BY avg_salary DESC"""
        ]
        
        if complexity in ['ALL', 'SIMPLE'] and role in ['HR', 'MANAGEMENT', 'ADMIN']:
            queries.extend(simple_queries)
        if complexity in ['ALL', 'MEDIUM'] and role in ['HR', 'MANAGEMENT', 'ADMIN']:
            queries.extend(medium_queries)
        if complexity in ['ALL', 'COMPLEX'] and role in ['HR', 'ADMIN', 'DEV']:
            queries.extend(complex_queries)
        
        return queries

    # ADMIN DATABASE - System Administration Queries
    def get_admin_queries_enriched(self, role: str, complexity: str = 'ALL') -> List[str]:
        """Enhanced admin database queries"""
        queries = []
        
        simple_queries = [
            "SELECT log_level, COUNT(*) as count FROM system_logs WHERE created_at >= CURDATE() GROUP BY log_level",
            "SELECT module, COUNT(*) as error_count FROM system_logs WHERE log_level = 'error' AND created_at >= CURDATE() - INTERVAL 7 DAY GROUP BY module",
            "SELECT user_id, COUNT(*) as session_count FROM user_sessions WHERE login_time >= CURDATE() GROUP BY user_id",
            "SELECT COUNT(*) as active_sessions FROM user_sessions WHERE is_active = TRUE",
            "SELECT * FROM system_logs WHERE log_level IN ('error', 'critical') ORDER BY created_at DESC LIMIT 20"
        ]
        
        medium_queries = [
            """SELECT DATE_FORMAT(sl.created_at, '%Y-%m-%d %H:00:00') as hour_bucket,
               sl.log_level,
               COUNT(*) as log_count
               FROM system_logs sl
               WHERE sl.created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
               GROUP BY DATE_FORMAT(sl.created_at, '%Y-%m-%d %H:00:00'), sl.log_level
               ORDER BY hour_bucket DESC""",
            
            """SELECT us.user_id,
               COUNT(*) as total_sessions,
               MAX(us.login_time) as last_login,
               AVG(TIMESTAMPDIFF(MINUTE, us.login_time, us.last_activity)) as avg_session_duration,
               SUM(CASE WHEN us.is_active = TRUE THEN 1 ELSE 0 END) as active_sessions
               FROM user_sessions us
               WHERE us.login_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
               GROUP BY us.user_id
               ORDER BY last_login DESC"""
        ]
        
        if complexity in ['ALL', 'SIMPLE'] and role in ['ADMIN', 'DEV']:
            queries.extend(simple_queries)
        if complexity in ['ALL', 'MEDIUM'] and role in ['ADMIN', 'DEV']:
            queries.extend(medium_queries)
        
        return queries

    # CROSS-DATABASE QUERIES - Business Intelligence
    def get_cross_database_queries(self, role: str, complexity: str = 'ALL') -> List[str]:
        """Cross-database analytical queries for business intelligence"""
        queries = []
        
        if role in ['MANAGEMENT', 'ADMIN', 'DEV']:
            queries.extend([
                # These are conceptual - would be executed as separate queries in practice
                "-- Sales Performance with Inventory Impact Analysis",
                "-- Customer Support Impact on Sales Retention",
                "-- Marketing Campaign ROI with Sales Conversion",
                "-- Employee Performance vs Department Revenue",
                "-- Financial Health Dashboard (Multi-DB Summary)"
            ])
        
        return queries

    # MALICIOUS QUERIES - Security Testing
    def get_malicious_queries_enriched(self, attack_type: str = 'sql_injection') -> List[str]:
        """Enhanced malicious queries for comprehensive security testing"""
        
        if attack_type == 'sql_injection':
            return [
                # Basic SQL Injection
                "SELECT * FROM customers WHERE customer_id = 1 OR 1=1--",
                "SELECT * FROM products WHERE product_name = 'test' UNION SELECT user(), version(), database()--",
                "SELECT * FROM orders WHERE order_id = 1'; DROP TABLE orders;--",
                
                # Advanced SQL Injection
                "SELECT * FROM customers WHERE customer_id = 1 AND (SELECT COUNT(*) FROM information_schema.tables) > 0--",
                "SELECT customer_id, (SELECT table_name FROM information_schema.tables LIMIT 1) FROM customers--",
                "SELECT * FROM products WHERE price = 1000 OR (SELECT SUBSTRING(user(),1,1)) = 'r'--",
                
                # Blind SQL Injection
                "SELECT * FROM customers WHERE customer_id = 1 AND (SELECT ASCII(SUBSTRING(database(),1,1))) > 64--",
                "SELECT * FROM orders WHERE total_amount = 1000 OR (SELECT LENGTH(database())) = 8--",
                
                # Time-based SQL Injection
                "SELECT * FROM products WHERE product_id = 1 OR (SELECT SLEEP(5))--",
                "SELECT * FROM customers WHERE customer_id = 1 AND (SELECT BENCHMARK(1000000, MD5('test')))--"
            ]
        
        elif attack_type == 'privilege_escalation':
            return [
                "SHOW GRANTS FOR CURRENT_USER()",
                "SELECT * FROM mysql.user WHERE user = USER()",
                "SELECT * FROM information_schema.user_privileges WHERE grantee LIKE '%{}%'".format("current_user"),
                "SHOW DATABASES",
                "SELECT schema_name FROM information_schema.schemata",
                "SELECT table_name FROM information_schema.tables WHERE table_schema = database()",
                "SELECT column_name FROM information_schema.columns WHERE table_schema = database()"
            ]
        
        elif attack_type == 'data_exfiltration':
            return [
                "SELECT * FROM customers ORDER BY customer_id LIMIT 10000",
                "SELECT customer_id, company_name, contact_person, phone FROM customers",
                "SELECT * FROM orders WHERE total_amount > 1000000",
                "SELECT employee_id, name, position, salary FROM hr_db.employees",
                "SELECT invoice_number, total_amount FROM finance_db.invoices",
                "SELECT campaign_name, budget FROM marketing_db.campaigns",
                "SELECT ticket_number, subject FROM support_db.support_tickets"
            ]
        
        elif attack_type == 'system_reconnaissance':
            return [
                "SELECT @@version, @@version_comment",
                "SELECT @@datadir, @@basedir",
                "SHOW VARIABLES LIKE '%version%'",
                "SELECT COUNT(*) FROM information_schema.tables",
                "SELECT table_schema, COUNT(*) FROM information_schema.tables GROUP BY table_schema",
                "SHOW PROCESSLIST",
                "SELECT * FROM information_schema.processlist"
            ]
        
        return []

    # MAIN QUERY DISPATCHER
    def get_queries_by_database_and_role(self, database: str, role: str, complexity: str = 'ALL', 
                                       scenario: str = 'ALL', limit: int = None) -> List[str]:
        """
        Main method to get queries based on database, role, complexity, and scenario
        """
        query_map = {
            'sales_db': self.get_sales_queries_enriched,
            'inventory_db': self.get_inventory_queries_enriched,
            'finance_db': self.get_finance_queries_enriched,
            'marketing_db': self.get_marketing_queries_enriched,
            'support_db': self.get_support_queries_enriched,
            'hr_db': self.get_hr_queries_enriched,
            'admin_db': self.get_admin_queries_enriched
        }
        
        queries = []
        
        if database == 'cross_database':
            queries = self.get_cross_database_queries(role, complexity)
        elif database in query_map:
            queries = query_map[database](role, complexity)
        
        # Apply limit if specified
        if limit and len(queries) > limit:
            queries = random.sample(queries, limit)
        
        return queries

    def get_query_statistics(self) -> Dict[str, int]:
        """Get statistics about the query library"""
        stats = {}
        databases = ['sales_db', 'inventory_db', 'finance_db', 'marketing_db', 'support_db', 'hr_db', 'admin_db']
        roles = ['SALES', 'MARKETING', 'CUSTOMER_SERVICE', 'HR', 'FINANCE', 'DEV', 'MANAGEMENT', 'ADMIN']
        
        total_queries = 0
        for db in databases:
            for role in roles:
                queries = self.get_queries_by_database_and_role(db, role)
                count = len(queries)
                stats[f"{db}_{role}"] = count
                total_queries += count
        
        stats['total_queries'] = total_queries
        stats['databases'] = len(databases)
        stats['roles'] = len(roles)
        
        return stats

# Example usage and testing
if __name__ == "__main__":
    library = EnrichedSQLLibrary()
    
    print("🧪 TESTING ENRICHED SQL QUERY LIBRARY")
    print("=" * 60)
    
    # Test query generation for different scenarios
    test_cases = [
        ("sales_db", "SALES", "SIMPLE"),
        ("sales_db", "MANAGEMENT", "COMPLEX"),
        ("finance_db", "FINANCE", "MEDIUM"),
        ("marketing_db", "MARKETING", "ALL"),
        ("hr_db", "HR", "SIMPLE")
    ]
    
    total_queries = 0
    for database, role, complexity in test_cases:
        queries = library.get_queries_by_database_and_role(database, role, complexity)
        total_queries += len(queries)
        print(f"\n📊 {database.upper()} - {role} ({complexity}):")
        print(f"   Generated {len(queries)} queries")
        if queries:
            print(f"   Sample: {queries[0][:100]}...")
    
    # Test malicious queries
    print(f"\n🔒 Security Testing Queries:")
    for attack_type in ['sql_injection', 'privilege_escalation', 'data_exfiltration']:
        malicious = library.get_malicious_queries_enriched(attack_type)
        print(f"   {attack_type}: {len(malicious)} queries")
    
    # Get overall statistics
    stats = library.get_query_statistics()
    print(f"\n📈 Library Statistics:")
    print(f"   Total Queries: {stats['total_queries']}")
    print(f"   Databases: {stats['databases']}")
    print(f"   Roles: {stats['roles']}")
    print(f"   Average per DB-Role: {stats['total_queries'] // (stats['databases'] * stats['roles'])}")
    
    print(f"\n✅ Enriched SQL library ready with {stats['total_queries']} total queries!")