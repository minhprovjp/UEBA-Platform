# simulation_v2/config_markov.py

# Ma trận chuyển đổi trạng thái (Transition Matrix)
# Cấu trúc: { ROLE: { CURRENT_STATE: { NEXT_STATE: PROBABILITY } } }
MARKOV_TRANSITIONS = {
    "SALES": {
        "START": {"LOGIN": 1.0},
        "LOGIN": {"SEARCH_CUSTOMER": 0.6, "SEARCH_ORDER": 0.3, "LOGOUT": 0.1},
        
        # Chuỗi hành động với Khách hàng
        "SEARCH_CUSTOMER": {"VIEW_CUSTOMER": 0.8, "SEARCH_CUSTOMER": 0.2},
        "VIEW_CUSTOMER": {"UPDATE_INFO": 0.1, "CREATE_ORDER": 0.3, "SEARCH_CUSTOMER": 0.6},
        "UPDATE_INFO": {"VIEW_CUSTOMER": 1.0}, # Sửa xong thì xem lại
        
        # Chuỗi hành động với Đơn hàng (Logic chặt chẽ)
        "SEARCH_ORDER": {"VIEW_ORDER": 0.8, "SEARCH_ORDER": 0.2},
        "VIEW_ORDER": {"UPDATE_ORDER_STATUS": 0.2, "ADD_ITEM": 0.1, "SEARCH_ORDER": 0.7},
        "UPDATE_ORDER_STATUS": {"VIEW_ORDER": 1.0},
        "ADD_ITEM": {"VIEW_ORDER": 1.0},
        "CREATE_ORDER": {"VIEW_ORDER": 1.0},

        "LOGOUT": {"START": 1.0} # Kết thúc phiên, chờ phiên sau
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
    
    "DEV": {
        "START": {"LOGIN": 1.0},
        "LOGIN": {"DEBUG_QUERY": 0.5, "CHECK_LOGS": 0.4, "LOGOUT": 0.1},
        "DEBUG_QUERY": {"EXPLAIN_QUERY": 0.6, "DEBUG_QUERY": 0.4},
        "EXPLAIN_QUERY": {"DEBUG_QUERY": 1.0},
        "CHECK_LOGS": {"DEBUG_QUERY": 0.5, "LOGOUT": 0.5},
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