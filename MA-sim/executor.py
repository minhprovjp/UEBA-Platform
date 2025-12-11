# simulation_v2/executor.py
import mysql.connector
import logging
import uuid
import random
import time # [NEW]

# Cấu hình kết nối
DB_CONFIG = {
    "port": 3306,
    "password": "password",
    "connection_timeout": 5
}

class SQLExecutor:
    def __init__(self):
        pass

    def get_connection(self, username, client_profile, database=None):
        """
        Tạo connection.
        [FIX] Đã loại bỏ connection_attributes để tránh lỗi thư viện.
        [FIX] Dùng host='localhost' để kết nối ổn định hơn.
        [FIX] Added optional database parameter to set default database
        """
        # Hackers map về intern_temp hoặc user quyền thấp để kết nối được
        target_user = "intern_temp" if username in ["script_kiddie", "unknown_hacker"] else username
        
        # Nếu user là insider (ví dụ dave.insider) thì dùng chính user đó
        if "insider" in username:
            target_user = username

        connection_params = {
            "host": "127.0.0.1", # [FIX] Dùng localhost thay vì 127.0.0.1
            "user": target_user, 
            "password": "password",
            "autocommit": True, 
            "connection_timeout": 3
        }
        
        # [FIX] Set default database if provided
        if database:
            connection_params["database"] = database

        try:
            return mysql.connector.connect(**connection_params)
        except Exception as e:
            # logging.error(f"Connection failed for {target_user}: {e}")
            return None
    
    def execute_action(self, agent, action, timestamp):
        """
        Execute an action from an agent and return the result record
        """
        if not action:
            return None
        
        try:
            # Get connection for the agent
            conn = self.get_connection(
                action.get("user", agent.username),
                action.get("client_profile", "default"),
                action.get("target_database")
            )
            
            if not conn:
                return {
                    "timestamp": timestamp,
                    "username": agent.username,
                    "role": agent.role,
                    "database": action.get("target_database", "unknown"),
                    "query": "CONNECTION_FAILED",
                    "has_error": 1,
                    "error_message": "Failed to connect to database",
                    "execution_time": 0.0
                }
            
            # Generate a simple query based on the action
            query = self._generate_query_for_action(action)
            
            if not query:
                return None
            
            # Execute the query
            start_time = time.time()
            cursor = conn.cursor()
            
            try:
                cursor.execute(query)
                result = cursor.fetchall()
                execution_time = time.time() - start_time
                
                # Create success record
                record = {
                    "timestamp": timestamp,
                    "username": agent.username,
                    "role": agent.role,
                    "database": action.get("target_database", "unknown"),
                    "query": query,
                    "has_error": 0,
                    "error_message": "",
                    "execution_time": execution_time,
                    "rows_returned": len(result) if result else 0
                }
                
                cursor.close()
                conn.close()
                return record
                
            except Exception as e:
                execution_time = time.time() - start_time
                
                # Create error record
                record = {
                    "timestamp": timestamp,
                    "username": agent.username,
                    "role": agent.role,
                    "database": action.get("target_database", "unknown"),
                    "query": query,
                    "has_error": 1,
                    "error_message": str(e),
                    "execution_time": execution_time
                }
                
                cursor.close()
                conn.close()
                return record
                
        except Exception as e:
            return {
                "timestamp": timestamp,
                "username": agent.username,
                "role": agent.role,
                "database": action.get("target_database", "unknown"),
                "query": "EXECUTION_ERROR",
                "has_error": 1,
                "error_message": str(e),
                "execution_time": 0.0
            }
    
    def _generate_query_for_action(self, action):
        """Generate a SQL query based on the action"""
        action_type = action.get("action", "LOGIN")
        database = action.get("target_database", "sales_db")
        
        # Simple query generation based on action type
        if action_type == "LOGIN":
            return f"SELECT 1"
        elif action_type == "SEARCH_CUSTOMER":
            return f"SELECT customer_id, company_name FROM {database}.customers LIMIT 10"
        elif action_type == "VIEW_CUSTOMER":
            customer_id = action.get("params", {}).get("customer_id", 1)
            return f"SELECT * FROM {database}.customers WHERE customer_id = {customer_id}"
        elif action_type == "SEARCH_ORDER":
            return f"SELECT order_id, customer_id, total_amount FROM {database}.orders LIMIT 10"
        elif action_type == "VIEW_ORDER":
            order_id = action.get("params", {}).get("order_id", 1)
            return f"SELECT * FROM {database}.orders WHERE order_id = {order_id}"
        elif action_type == "SEARCH_EMPLOYEE":
            return f"SELECT employee_id, full_name, department FROM {database}.employees LIMIT 10"
        elif action_type == "VIEW_PROFILE":
            employee_id = action.get("params", {}).get("employee_id", 1)
            return f"SELECT * FROM {database}.employees WHERE employee_id = {employee_id}"
        elif action_type == "SEARCH_CAMPAIGN":
            return f"SELECT campaign_id, campaign_name, status FROM {database}.campaigns LIMIT 10"
        elif action_type == "VIEW_CAMPAIGN":
            campaign_id = action.get("params", {}).get("campaign_id", 1)
            return f"SELECT * FROM {database}.campaigns WHERE campaign_id = {campaign_id}"
        elif action_type == "SEARCH_TICKET":
            return f"SELECT ticket_id, subject, status FROM {database}.support_tickets LIMIT 10"
        elif action_type == "VIEW_TICKET":
            ticket_id = action.get("params", {}).get("ticket_id", 1)
            return f"SELECT * FROM {database}.support_tickets WHERE ticket_id = {ticket_id}"
        else:
            # Default query
            return f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{database}'"

    def execute(self, intent, sql, sim_timestamp=None, client_profile=None):
        user = intent['user']
        is_anomaly = intent['is_anomaly']
        behavior = intent['action']
        db_target = intent.get('database', '') 
        # [NEW] Lấy Port từ Agent
        agent_port = intent.get('client_port', 0)
        
        if not client_profile:
            client_profile = {}

        # Fallback DB target
        if not db_target:
            if "sales_db" in sql: db_target = "sales_db"
            elif "hr_db" in sql: db_target = "hr_db"
            
        # [NEW] Latency Injection
        # Hacker/Remote: 50ms - 200ms
        # Local: 1ms - 5ms
        if is_anomaly:
            latency = random.uniform(0.05, 0.2)
        else:
            latency = random.uniform(0.001, 0.005)
        
        # Sleep để giả lập độ trễ mạng TRƯỚC KHI gửi query
        time.sleep(latency)
        
        # --- TAGGING (Quan trọng nhất) ---
        sim_id = uuid.uuid4().hex[:6]
        
        # Lấy thông tin giả lập từ Profile
        fake_ip = client_profile.get("source_host", "192.168.1.100")
        fake_prog = client_profile.get("program_name", "Unknown")
        fake_os = client_profile.get("client_os", "Windows")
        fake_conn = client_profile.get("connector_name", "mysql-connector")
        fake_host = client_profile.get("source_host", "pc")
        
        ts_tag = f"|TS:{sim_timestamp}" if sim_timestamp else ""
        
        # [UPDATE] Thêm Port vào Tag
        tag = f"/* SIM_META:{user}|{fake_ip}|{agent_port}|ID:{sim_id}|BEH:{behavior}|ANO:{is_anomaly}|PROG:{fake_prog}|OS:{fake_os}|CONN:{fake_conn}|HOST:{fake_host}{ts_tag} */"
        
        tagged_sql = f"{tag} {sql}"

        # [FIX] Determine database from SQL content
        target_database = None
        databases_to_check = ["sales_db", "hr_db", "information_schema", "mysql"]
        for db_name in databases_to_check:
            if f"{db_name}." in sql:
                target_database = db_name
                break  # Use the first database found
        
        # Thực thi
        conn = self.get_connection(user, client_profile, target_database)
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute(tagged_sql)
                
                # Đọc hết kết quả để giải phóng cursor
                if cursor.with_rows: 
                    cursor.fetchall()
                
                cursor.close()
                conn.close()
                return True
            except Exception as e:
                # Lỗi SQL (ví dụ hacker chạy lệnh sai) vẫn là thành công về mặt mô phỏng
                if conn: conn.close()
                return False
        return False