#!/usr/bin/env python3
"""
Validated SQL Executor - Pre-validates queries to prevent "errors"
WARNING: This reduces realism and UBA training value!
"""

import mysql.connector
import re
from executor import SQLExecutor

class ValidatedSQLExecutor(SQLExecutor):
    """
    SQL Executor with pre-validation to prevent "no rows matched" scenarios
    NOTE: This reduces the realism of the simulation!
    """
    
    def __init__(self):
        super().__init__()
        self.validation_enabled = True
    
    def execute_with_validation(self, intent, sql, sim_timestamp=None, client_profile=None):
        """Execute SQL with pre-validation to prevent common 'errors'"""
        
        if not self.validation_enabled:
            return super().execute(intent, sql, sim_timestamp, client_profile)
        
        # Pre-validate UPDATE queries
        if sql.strip().upper().startswith('UPDATE'):
            validated_sql = self._validate_update_query(sql, intent)
            if validated_sql != sql:
                print(f"🔧 VALIDATION: Modified query to ensure success")
                print(f"   Original: {sql}")
                print(f"   Modified: {validated_sql}")
                sql = validated_sql
        
        # Pre-validate SELECT queries with specific IDs
        elif 'WHERE' in sql.upper() and ('_id =' in sql or '_id=' in sql):
            validated_sql = self._validate_select_query(sql, intent)
            if validated_sql != sql:
                print(f"🔧 VALIDATION: Modified query to ensure data exists")
                sql = validated_sql
        
        return super().execute(intent, sql, sim_timestamp, client_profile)
    
    def _validate_update_query(self, sql, intent):
        """Validate UPDATE queries to ensure they will match rows"""
        
        # Extract database and table from UPDATE query
        update_match = re.search(r'UPDATE\s+(\w+)\.(\w+)\s+SET', sql, re.IGNORECASE)
        if not update_match:
            return sql
        
        database, table = update_match.groups()
        
        # Get connection to check data
        conn = self.get_connection(intent['user'], {}, database)
        if not conn:
            return sql
        
        try:
            cursor = conn.cursor()
            
            # For support_tickets updates, ensure ticket exists and has correct status
            if table == 'support_tickets':
                # Extract ticket_id from WHERE clause
                ticket_match = re.search(r'ticket_id\s*<=?\s*(\d+)', sql)
                if ticket_match:
                    ticket_id = int(ticket_match.group(1))
                    
                    # Check if tickets exist with updatable status
                    cursor.execute(f"""
                        SELECT ticket_id FROM {database}.{table} 
                        WHERE ticket_id <= {ticket_id} 
                        AND status IN ('open', 'in_progress') 
                        LIMIT 1
                    """)
                    
                    result = cursor.fetchone()
                    if not result:
                        # Find a valid ticket_id that exists
                        cursor.execute(f"""
                            SELECT MAX(ticket_id) FROM {database}.{table} 
                            WHERE status IN ('open', 'in_progress')
                        """)
                        max_ticket = cursor.fetchone()
                        if max_ticket and max_ticket[0]:
                            # Replace ticket_id in query with valid one
                            sql = re.sub(r'ticket_id\s*<=?\s*\d+', f'ticket_id <= {max_ticket[0]}', sql)
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            if conn:
                conn.close()
            print(f"⚠️ Validation failed: {e}")
        
        return sql
    
    def _validate_select_query(self, sql, intent):
        """Validate SELECT queries to ensure referenced IDs exist"""
        
        # Extract database and table
        from_match = re.search(r'FROM\s+(\w+)\.(\w+)', sql, re.IGNORECASE)
        if not from_match:
            return sql
        
        database, table = from_match.groups()
        
        # Get connection to check data
        conn = self.get_connection(intent['user'], {}, database)
        if not conn:
            return sql
        
        try:
            cursor = conn.cursor()
            
            # Extract ID conditions
            id_matches = re.findall(r'(\w+_id)\s*=\s*(\d+)', sql)
            
            for id_column, id_value in id_matches:
                # Check if this ID exists
                cursor.execute(f"SELECT 1 FROM {database}.{table} WHERE {id_column} = {id_value} LIMIT 1")
                result = cursor.fetchone()
                
                if not result:
                    # Find a valid ID
                    cursor.execute(f"SELECT {id_column} FROM {database}.{table} ORDER BY RAND() LIMIT 1")
                    valid_id = cursor.fetchone()
                    if valid_id:
                        # Replace with valid ID
                        sql = re.sub(f'{id_column}\\s*=\\s*{id_value}', f'{id_column} = {valid_id[0]}', sql)
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            if conn:
                conn.close()
            print(f"⚠️ Validation failed: {e}")
        
        return sql

# Example usage
if __name__ == "__main__":
    print("🧪 TESTING VALIDATED EXECUTOR")
    print("=" * 50)
    
    executor = ValidatedSQLExecutor()
    
    # Test UPDATE validation
    test_intent = {
        'user': 'test_user',
        'is_anomaly': False,
        'action': 'UPDATE_TICKET',
        'database': 'support_db'
    }
    
    test_sql = "UPDATE support_db.support_tickets SET status = 'closed' WHERE ticket_id = 99999 AND status = 'open'"
    
    print(f"Original SQL: {test_sql}")
    validated_sql = executor._validate_update_query(test_sql, test_intent)
    print(f"Validated SQL: {validated_sql}")