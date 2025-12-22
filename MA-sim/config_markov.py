# simulation_v2/config_markov.py

# Ma trận chuyển đổi trạng thái (Transition Matrix)
# Cấu trúc: { ROLE: { CURRENT_STATE: { NEXT_STATE: PROBABILITY } } }
MARKOV_TRANSITIONS = {
    "SALES": {
        "START": {"LOGIN": 1.0},
        "LOGIN": {"SEARCH_CUSTOMER": 0.3, "SEARCH_ORDER": 0.15, "CHECK_INVENTORY": 0.15, "UPDATE_STATUS": 0.1, "SALES_REPORT": 0.2, "LOGOUT": 0.1},
        
        # Chuỗi hành động với Khách hàng
        "SEARCH_CUSTOMER": {"VIEW_CUSTOMER": 0.8, "SEARCH_CUSTOMER": 0.2},
        "VIEW_CUSTOMER": {"UPDATE_INFO": 0.1, "CREATE_ORDER": 0.3, "SEARCH_CUSTOMER": 0.6},
        "UPDATE_INFO": {"VIEW_CUSTOMER": 1.0}, # Sửa xong thì xem lại
        
        # Chuỗi hành động với Đơn hàng (Logic chặt chẽ)
        "SEARCH_ORDER": {"VIEW_ORDER": 0.8, "SEARCH_ORDER": 0.2},
        "VIEW_ORDER": {"UPDATE_STATUS": 0.15, "UPDATE_ORDER_STATUS": 0.1, "ADD_ITEM": 0.05, "SEARCH_ORDER": 0.7},
        "UPDATE_STATUS": {"VIEW_ORDER": 1.0},
        "UPDATE_ORDER_STATUS": {"VIEW_ORDER": 1.0},
        "ADD_ITEM": {"VIEW_ORDER": 1.0},
        "CREATE_ORDER": {"VIEW_ORDER": 1.0},
        
        # Inventory checks
        "CHECK_INVENTORY": {"VIEW_ORDER": 0.5, "SEARCH_ORDER": 0.3, "LOGOUT": 0.2},
        
        # Sales reports
        "SALES_REPORT": {"VIEW_ORDER": 0.5, "SEARCH_CUSTOMER": 0.4, "LOGOUT": 0.1},

        "LOGOUT": {"START": 1.0} # Kết thúc phiên, chờ phiên sau
    },
    
    "MARKETING": {
        "START": {"LOGIN": 1.0},
        "LOGIN": {"SEARCH_CAMPAIGN": 0.3, "SEARCH_LEAD": 0.2, "VIEW_LEADS": 0.2, "CAMPAIGN_ROI": 0.2, "LOGOUT": 0.1},
        "SEARCH_CAMPAIGN": {"VIEW_CAMPAIGN": 0.8, "SEARCH_CAMPAIGN": 0.2},
        "VIEW_CAMPAIGN": {"UPDATE_CAMPAIGN": 0.2, "CAMPAIGN_ROI": 0.2, "SEARCH_CAMPAIGN": 0.6},
        "UPDATE_CAMPAIGN": {"VIEW_CAMPAIGN": 1.0},
        "SEARCH_LEAD": {"VIEW_LEADS": 0.8, "UPDATE_LEAD_STATUS": 0.2},
        "VIEW_LEADS": {"UPDATE_LEAD_STATUS": 0.3, "UPDATE_LEAD": 0.2, "SEARCH_CAMPAIGN": 0.5},
        "UPDATE_LEAD_STATUS": {"VIEW_LEADS": 0.7, "SEARCH_LEAD": 0.3},
        "UPDATE_LEAD": {"VIEW_LEADS": 1.0},
        "CAMPAIGN_ROI": {"VIEW_CAMPAIGN": 0.6, "LOGOUT": 0.4},
        "LOGOUT": {"START": 1.0}
    },
    
    "CUSTOMER_SERVICE": {
        "START": {"LOGIN": 1.0},
        "LOGIN": {"SEARCH_TICKET": 0.5, "MY_TICKETS": 0.3, "VIEW_CUSTOMER": 0.1, "LOGOUT": 0.1},
        "SEARCH_TICKET": {"VIEW_TICKET": 0.8, "SEARCH_TICKET": 0.2},
        "VIEW_TICKET": {"UPDATE_TICKET": 0.4, "SEARCH_TICKET": 0.6},
        "UPDATE_TICKET": {"VIEW_TICKET": 1.0},
        "MY_TICKETS": {"VIEW_TICKET": 0.7, "SEARCH_TICKET": 0.3},
        "VIEW_CUSTOMER": {"SEARCH_TICKET": 0.8, "LOGOUT": 0.2},
        "LOGOUT": {"START": 1.0}
    },
    
    "HR": {
        "START": {"LOGIN": 1.0},
        "LOGIN": {"SEARCH_EMPLOYEE": 0.7, "VIEW_PAYROLL": 0.2, "LOGOUT": 0.1},
        
        "SEARCH_EMPLOYEE": {"VIEW_PROFILE": 0.9, "SEARCH_EMPLOYEE": 0.1},
        "VIEW_PROFILE": {"UPDATE_SALARY": 0.05, "CHECK_ATTENDANCE": 0.5, "SEARCH_EMPLOYEE": 0.45},
        "UPDATE_SALARY": {"VIEW_PROFILE": 1.0},
        "CHECK_ATTENDANCE": {"VIEW_PROFILE": 1.0},
        
        "VIEW_PAYROLL": {"EXPORT_REPORT": 0.3, "SEARCH_EMPLOYEE": 0.7},
        "EXPORT_REPORT": {"LOGOUT": 1.0},
        
        "LOGOUT": {"START": 1.0}
    },
    
    "FINANCE": {
        "START": {"LOGIN": 1.0},
        "LOGIN": {"VIEW_INVOICE": 0.25, "CHECK_PAYMENT": 0.2, "OVERDUE_CHECK": 0.2, "VIEW_EXPENSE": 0.15, "VIEW_REPORT": 0.1, "LOGOUT": 0.1},
        "VIEW_INVOICE": {"UPDATE_INVOICE": 0.2, "CHECK_PAYMENT": 0.1, "OVERDUE_CHECK": 0.1, "VIEW_INVOICE": 0.6},
        "UPDATE_INVOICE": {"VIEW_INVOICE": 1.0},
        "CHECK_PAYMENT": {"VIEW_INVOICE": 0.6, "OVERDUE_CHECK": 0.2, "VIEW_EXPENSE": 0.2},
        "OVERDUE_CHECK": {"VIEW_INVOICE": 0.7, "VIEW_REPORT": 0.3},
        "VIEW_EXPENSE": {"APPROVE_EXPENSE": 0.3, "VIEW_EXPENSE": 0.7},
        "APPROVE_EXPENSE": {"VIEW_EXPENSE": 1.0},
        "VIEW_REPORT": {"EXPORT_REPORT": 0.4, "VIEW_INVOICE": 0.6},
        "EXPORT_REPORT": {"LOGOUT": 1.0},
        "LOGOUT": {"START": 1.0}
    },
    
    "DEV": {
        "START": {"LOGIN": 1.0},
        "LOGIN": {"DEBUG_QUERY": 0.5, "CHECK_LOGS": 0.4, "LOGOUT": 0.1},
        "DEBUG_QUERY": {"EXPLAIN_QUERY": 0.6, "DEBUG_QUERY": 0.4},
        "EXPLAIN_QUERY": {"DEBUG_QUERY": 1.0},
        "CHECK_LOGS": {"DEBUG_QUERY": 0.5, "LOGOUT": 0.5},
        "LOGOUT": {"START": 1.0}
    },
    
    "MANAGEMENT": {
        "START": {"LOGIN": 1.0},
        "LOGIN": {"VIEW_DASHBOARD": 0.4, "VIEW_REPORT": 0.3, "SEARCH_CUSTOMER": 0.2, "LOGOUT": 0.1},
        "VIEW_DASHBOARD": {"VIEW_REPORT": 0.5, "SEARCH_CUSTOMER": 0.5},
        "VIEW_REPORT": {"EXPORT_REPORT": 0.3, "VIEW_DASHBOARD": 0.7},
        "EXPORT_REPORT": {"LOGOUT": 1.0},
        "SEARCH_CUSTOMER": {"VIEW_CUSTOMER": 0.8, "VIEW_DASHBOARD": 0.2},
        "VIEW_CUSTOMER": {"SEARCH_CUSTOMER": 1.0},
        "LOGOUT": {"START": 1.0}
    },
    
    "ADMIN": {
        "START": {"LOGIN": 1.0},
        "LOGIN": {"CHECK_LOGS": 0.3, "VIEW_SESSIONS": 0.2, "SYSTEM_HEALTH": 0.2, "VIEW_USERS": 0.2, "LOGOUT": 0.1},
        "CHECK_LOGS": {"DEBUG_QUERY": 0.4, "SYSTEM_HEALTH": 0.2, "VIEW_USERS": 0.4},
        "SYSTEM_HEALTH": {"CHECK_LOGS": 0.6, "VIEW_SESSIONS": 0.4},
        "VIEW_SESSIONS": {"VIEW_USERS": 0.5, "CHECK_LOGS": 0.5},
        "VIEW_USERS": {"UPDATE_USER": 0.2, "CHECK_LOGS": 0.8},
        "UPDATE_USER": {"VIEW_USERS": 1.0},
        "DEBUG_QUERY": {"EXPLAIN_QUERY": 0.6, "CHECK_LOGS": 0.4},
        "EXPLAIN_QUERY": {"DEBUG_QUERY": 1.0},
        "LOGOUT": {"START": 1.0}
    },
    
    "INVENTORY": {
        "START": {"LOGIN": 1.0},
        "LOGIN": {"CHECK_STOCK": 0.3, "VIEW_PRODUCT": 0.3, "LOW_STOCK_ALERT": 0.2, "UPDATE_STOCK": 0.15, "LOGOUT": 0.05},
        "CHECK_STOCK": {"VIEW_PRODUCT": 0.6, "UPDATE_STOCK": 0.2, "CHECK_STOCK": 0.2},
        "VIEW_PRODUCT": {"UPDATE_STOCK": 0.2, "CHECK_STOCK": 0.3, "VIEW_PRODUCT": 0.5},
        "UPDATE_STOCK": {"VIEW_PRODUCT": 0.6, "CHECK_STOCK": 0.4},
        "LOW_STOCK_ALERT": {"VIEW_PRODUCT": 0.5, "UPDATE_STOCK": 0.3, "LOGOUT": 0.2},
        "LOGOUT": {"START": 1.0}
    }
}

# Định nghĩa hành động nào cần tham số gì (Context Requirements)
ACTION_REQUIREMENTS = {
    "VIEW_CUSTOMER": ["customer_id"],
    "UPDATE_INFO": ["customer_id"],
    "CREATE_ORDER": ["customer_id"],
    
    "VIEW_ORDER": ["order_id"],
    "UPDATE_ORDER_STATUS": ["order_id"],
    "ADD_ITEM": ["order_id"],
    
    "VIEW_PROFILE": ["employee_id"],
    "UPDATE_SALARY": ["employee_id"],
    "CHECK_ATTENDANCE": ["employee_id"],
    
    "DEBUG_QUERY": ["order_id"]
}