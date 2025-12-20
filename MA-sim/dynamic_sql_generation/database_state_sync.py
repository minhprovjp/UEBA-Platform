"""
Database State Synchronization System

Implements real-time database state updates from executor feedback,
entity relationship tracking, constraint violation monitoring, and
performance metrics collection for generation quality assessment.
"""

import logging
import threading
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque

from .models import (
    DatabaseState, Relationship, ConstraintViolation, 
    Modification, PerformanceMetrics
)


@dataclass
class ExecutionFeedback:
    """Feedback from query execution for state synchronization"""
    query: str
    success: bool
    execution_time: float
    error_message: Optional[str]
    rows_affected: int
    rows_returned: int
    database: str
    username: str
    role: str
    timestamp: datetime
    query_type: str  # SELECT, INSERT, UPDATE, DELETE, etc.
    tables_accessed: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'query': self.query,
            'success': self.success,
            'execution_time': self.execution_time,
            'error_message': self.error_message,
            'rows_affected': self.rows_affected,
            'rows_returned': self.rows_returned,
            'database': self.database,
            'username': self.username,
            'role': self.role,
            'timestamp': self.timestamp.isoformat(),
            'query_type': self.query_type,
            'tables_accessed': self.tables_accessed
        }


@dataclass
class EntityRelationshipMap:
    """Entity relationship mapping for database state tracking"""
    database: str
    relationships: Dict[str, List[Relationship]] = field(default_factory=dict)
    entity_counts: Dict[str, int] = field(default_factory=dict)
    last_updated: datetime = field(default_factory=datetime.now)
    
    def add_relationship(self, from_table: str, to_table: str, 
                        relationship_type: str, foreign_key: str):
        """Add a relationship to the mapping"""
        if from_table not in self.relationships:
            self.relationships[from_table] = []
        
        relationship = Relationship(
            from_table=from_table,
            to_table=to_table,
            relationship_type=relationship_type,
            foreign_key=foreign_key
        )
        
        self.relationships[from_table].append(relationship)
        self.last_updated = datetime.now()
    
    def get_related_tables(self, table: str) -> List[str]:
        """Get all tables related to the given table"""
        related = set()
        
        # Direct relationships
        if table in self.relationships:
            for rel in self.relationships[table]:
                related.add(rel.to_table)
        
        # Reverse relationships
        for from_table, rels in self.relationships.items():
            for rel in rels:
                if rel.to_table == table:
                    related.add(from_table)
        
        return list(related)


@dataclass
class PerformanceTracker:
    """Performance metrics tracker for database operations"""
    query_times: deque = field(default_factory=lambda: deque(maxlen=1000))
    slow_queries: List[Dict[str, Any]] = field(default_factory=list)
    error_counts: Dict[str, int] = field(default_factory=dict)
    success_rates: Dict[str, float] = field(default_factory=dict)
    connection_counts: Dict[str, int] = field(default_factory=dict)
    
    def add_query_time(self, execution_time: float, query_type: str, database: str):
        """Add query execution time for tracking"""
        self.query_times.append({
            'time': execution_time,
            'type': query_type,
            'database': database,
            'timestamp': datetime.now()
        })
        
        # Track slow queries (> 2 seconds)
        if execution_time > 2.0:
            self.slow_queries.append({
                'execution_time': execution_time,
                'query_type': query_type,
                'database': database,
                'timestamp': datetime.now()
            })
            
            # Keep only recent slow queries (last 100)
            if len(self.slow_queries) > 100:
                self.slow_queries = self.slow_queries[-100:]
    
    def add_error(self, error_type: str, database: str):
        """Add error count for tracking"""
        key = f"{database}_{error_type}"
        self.error_counts[key] = self.error_counts.get(key, 0) + 1
    
    def update_success_rate(self, database: str, success: bool):
        """Update success rate for database operations"""
        if database not in self.success_rates:
            self.success_rates[database] = 1.0 if success else 0.0
        else:
            # Simple moving average
            current_rate = self.success_rates[database]
            self.success_rates[database] = (current_rate * 0.9) + (1.0 if success else 0.0) * 0.1
    
    def get_avg_query_time(self, database: Optional[str] = None) -> float:
        """Get average query time for database or overall"""
        if not self.query_times:
            return 0.0
        
        if database:
            relevant_times = [q['time'] for q in self.query_times if q['database'] == database]
        else:
            relevant_times = [q['time'] for q in self.query_times]
        
        return sum(relevant_times) / len(relevant_times) if relevant_times else 0.0
    
    def get_slow_query_count(self, database: Optional[str] = None) -> int:
        """Get count of slow queries"""
        if database:
            return len([q for q in self.slow_queries if q['database'] == database])
        return len(self.slow_queries)


class DatabaseStateSynchronizer:
    """
    Database State Synchronization System
    
    Manages real-time database state updates from executor feedback,
    tracks entity relationships and constraint violations, and collects
    performance metrics for generation quality assessment.
    """
    
    def __init__(self, update_interval: float = 1.0):
        """
        Initialize the database state synchronizer
        
        Args:
            update_interval: Interval in seconds for state updates
        """
        self.logger = logging.getLogger(__name__)
        self.update_interval = update_interval
        
        # State tracking
        self.database_states: Dict[str, DatabaseState] = {}
        self.entity_maps: Dict[str, EntityRelationshipMap] = {}
        self.performance_tracker = PerformanceTracker()
        
        # Feedback processing
        self.feedback_queue: deque = deque()
        self.constraint_violations: Dict[str, List[ConstraintViolation]] = defaultdict(list)
        self.recent_modifications: Dict[str, List[Modification]] = defaultdict(list)
        
        # Threading for real-time updates
        self._running = False
        self._update_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
        # Vietnamese business database schema knowledge
        self._initialize_vietnamese_database_schemas()
        
        self.logger.info("DatabaseStateSynchronizer initialized")
    
    def _initialize_vietnamese_database_schemas(self):
        """Initialize Vietnamese business database schemas and relationships"""
        # Define Vietnamese business database schemas
        vietnamese_schemas = {
            'sales_db': {
                'tables': ['customers', 'orders', 'products', 'order_items'],
                'relationships': [
                    ('orders', 'customers', 'many_to_one', 'customer_id'),
                    ('order_items', 'orders', 'many_to_one', 'order_id'),
                    ('order_items', 'products', 'many_to_one', 'product_id')
                ]
            },
            'hr_db': {
                'tables': ['employees', 'departments', 'positions', 'salaries'],
                'relationships': [
                    ('employees', 'departments', 'many_to_one', 'department_id'),
                    ('employees', 'positions', 'many_to_one', 'position_id'),
                    ('salaries', 'employees', 'one_to_one', 'employee_id')
                ]
            },
            'finance_db': {
                'tables': ['invoices', 'payments', 'accounts', 'transactions'],
                'relationships': [
                    ('payments', 'invoices', 'many_to_one', 'invoice_id'),
                    ('transactions', 'accounts', 'many_to_one', 'account_id'),
                    ('invoices', 'accounts', 'many_to_one', 'account_id')
                ]
            },
            'marketing_db': {
                'tables': ['campaigns', 'leads', 'contacts', 'campaign_results'],
                'relationships': [
                    ('leads', 'campaigns', 'many_to_one', 'campaign_id'),
                    ('campaign_results', 'campaigns', 'one_to_one', 'campaign_id'),
                    ('contacts', 'leads', 'one_to_one', 'lead_id')
                ]
            },
            'support_db': {
                'tables': ['tickets', 'ticket_categories', 'responses', 'agents'],
                'relationships': [
                    ('tickets', 'ticket_categories', 'many_to_one', 'category_id'),
                    ('responses', 'tickets', 'many_to_one', 'ticket_id'),
                    ('tickets', 'agents', 'many_to_one', 'assigned_agent_id')
                ]
            },
            'inventory_db': {
                'tables': ['products', 'inventory_levels', 'stock_movements', 'suppliers'],
                'relationships': [
                    ('inventory_levels', 'products', 'one_to_one', 'product_id'),
                    ('stock_movements', 'products', 'many_to_one', 'product_id'),
                    ('products', 'suppliers', 'many_to_one', 'supplier_id')
                ]
            },
            'admin_db': {
                'tables': ['users', 'user_sessions', 'system_logs', 'permissions'],
                'relationships': [
                    ('user_sessions', 'users', 'many_to_one', 'user_id'),
                    ('system_logs', 'users', 'many_to_one', 'user_id'),
                    ('permissions', 'users', 'many_to_many', 'user_id')
                ]
            }
        }
        
        # Initialize entity relationship maps
        for db_name, schema in vietnamese_schemas.items():
            entity_map = EntityRelationshipMap(database=db_name)
            
            # Add relationships
            for from_table, to_table, rel_type, foreign_key in schema['relationships']:
                entity_map.add_relationship(from_table, to_table, rel_type, foreign_key)
            
            # Initialize entity counts (will be updated from feedback)
            for table in schema['tables']:
                entity_map.entity_counts[table] = 0
            
            self.entity_maps[db_name] = entity_map
        
        self.logger.info(f"Initialized {len(vietnamese_schemas)} Vietnamese database schemas")
    
    def start_synchronization(self):
        """Start real-time database state synchronization"""
        if self._running:
            self.logger.warning("Synchronization already running")
            return
        
        self._running = True
        self._update_thread = threading.Thread(target=self._synchronization_loop, daemon=True)
        self._update_thread.start()
        
        self.logger.info("Database state synchronization started")
    
    def stop_synchronization(self):
        """Stop real-time database state synchronization"""
        if not self._running:
            return
        
        self._running = False
        if self._update_thread:
            self._update_thread.join(timeout=5.0)
        
        self.logger.info("Database state synchronization stopped")
    
    def _synchronization_loop(self):
        """Main synchronization loop running in background thread"""
        while self._running:
            try:
                self._process_feedback_queue()
                self._update_database_states()
                self._cleanup_old_data()
                
                time.sleep(self.update_interval)
                
            except Exception as e:
                self.logger.error(f"Error in synchronization loop: {e}")
                time.sleep(self.update_interval)
    
    def add_execution_feedback(self, feedback: ExecutionFeedback):
        """
        Add execution feedback for processing
        
        Args:
            feedback: ExecutionFeedback from query execution
        """
        with self._lock:
            self.feedback_queue.append(feedback)
        
        # Process immediately if queue is getting large
        if len(self.feedback_queue) > 100:
            self._process_feedback_queue()
    
    def _process_feedback_queue(self):
        """Process queued execution feedback"""
        with self._lock:
            if not self.feedback_queue:
                return
            
            # Process all queued feedback
            feedback_batch = list(self.feedback_queue)
            self.feedback_queue.clear()
        
        for feedback in feedback_batch:
            try:
                self._process_single_feedback(feedback)
            except Exception as e:
                self.logger.error(f"Error processing feedback: {e}")
    
    def _process_single_feedback(self, feedback: ExecutionFeedback):
        """Process a single execution feedback"""
        database = feedback.database
        
        # Update performance metrics
        self.performance_tracker.add_query_time(
            feedback.execution_time, feedback.query_type, database
        )
        self.performance_tracker.update_success_rate(database, feedback.success)
        
        # Track errors
        if not feedback.success and feedback.error_message:
            error_type = self._classify_error(feedback.error_message)
            self.performance_tracker.add_error(error_type, database)
            
            # Check for constraint violations
            if self._is_constraint_violation(feedback.error_message):
                self._add_constraint_violation(feedback)
        
        # Track modifications
        if feedback.query_type in ['INSERT', 'UPDATE', 'DELETE'] and feedback.success:
            self._add_modification(feedback)
        
        # Update entity counts
        if feedback.success and feedback.query_type == 'SELECT':
            self._update_entity_counts(feedback)
        
        # Update relationship tracking
        self._update_relationship_tracking(feedback)
    
    def _classify_error(self, error_message: str) -> str:
        """Classify error message into error type"""
        error_message_lower = error_message.lower()
        
        if 'access denied' in error_message_lower or 'permission' in error_message_lower:
            return 'permission_error'
        elif 'syntax error' in error_message_lower or 'sql syntax' in error_message_lower:
            return 'syntax_error'
        elif 'constraint' in error_message_lower or 'foreign key' in error_message_lower:
            return 'constraint_violation'
        elif 'timeout' in error_message_lower or 'connection' in error_message_lower:
            return 'connection_error'
        elif 'table' in error_message_lower and 'exist' in error_message_lower:
            return 'table_not_found'
        else:
            return 'unknown_error'
    
    def _is_constraint_violation(self, error_message: str) -> bool:
        """Check if error message indicates a constraint violation"""
        constraint_keywords = [
            'constraint', 'foreign key', 'primary key', 'unique', 
            'check constraint', 'not null', 'duplicate entry'
        ]
        
        error_lower = error_message.lower()
        return any(keyword in error_lower for keyword in constraint_keywords)
    
    def _add_constraint_violation(self, feedback: ExecutionFeedback):
        """Add constraint violation from feedback"""
        violation = ConstraintViolation(
            constraint_type=self._extract_constraint_type(feedback.error_message),
            table_name=self._extract_table_name(feedback.query),
            column_name=self._extract_column_name(feedback.error_message),
            violation_count=1
        )
        
        self.constraint_violations[feedback.database].append(violation)
        
        # Keep only recent violations (last 100 per database)
        if len(self.constraint_violations[feedback.database]) > 100:
            self.constraint_violations[feedback.database] = self.constraint_violations[feedback.database][-100:]
    
    def _extract_constraint_type(self, error_message: str) -> str:
        """Extract constraint type from error message"""
        error_lower = error_message.lower()
        
        if 'foreign key' in error_lower:
            return 'foreign_key'
        elif 'primary key' in error_lower:
            return 'primary_key'
        elif 'unique' in error_lower:
            return 'unique'
        elif 'not null' in error_lower:
            return 'not_null'
        elif 'check' in error_lower:
            return 'check'
        else:
            return 'unknown'
    
    def _extract_table_name(self, query: str) -> str:
        """Extract table name from SQL query"""
        try:
            query_upper = query.upper()
            
            # Simple extraction for common patterns
            if 'FROM' in query_upper:
                from_index = query_upper.find('FROM')
                after_from = query[from_index + 4:].strip()
                table_name = after_from.split()[0].strip('`"[]')
                return table_name.split('.')[1] if '.' in table_name else table_name
            elif 'INTO' in query_upper:
                into_index = query_upper.find('INTO')
                after_into = query[into_index + 4:].strip()
                table_name = after_into.split()[0].strip('`"[]')
                return table_name.split('.')[1] if '.' in table_name else table_name
            elif 'UPDATE' in query_upper:
                update_index = query_upper.find('UPDATE')
                after_update = query[update_index + 6:].strip()
                table_name = after_update.split()[0].strip('`"[]')
                return table_name.split('.')[1] if '.' in table_name else table_name
            
            return 'unknown'
            
        except Exception:
            return 'unknown'
    
    def _extract_column_name(self, error_message: str) -> str:
        """Extract column name from error message"""
        try:
            # Simple extraction for common error patterns
            if "column '" in error_message.lower():
                start = error_message.lower().find("column '") + 8
                end = error_message.find("'", start)
                return error_message[start:end] if end > start else 'unknown'
            elif 'field ' in error_message.lower():
                start = error_message.lower().find('field ') + 6
                end = error_message.find(' ', start)
                return error_message[start:end] if end > start else 'unknown'
            
            return 'unknown'
            
        except Exception:
            return 'unknown'
    
    def _add_modification(self, feedback: ExecutionFeedback):
        """Add modification record from feedback"""
        modification = Modification(
            table_name=self._extract_table_name(feedback.query),
            operation=feedback.query_type,
            timestamp=feedback.timestamp,
            affected_rows=feedback.rows_affected
        )
        
        self.recent_modifications[feedback.database].append(modification)
        
        # Keep only recent modifications (last 200 per database)
        if len(self.recent_modifications[feedback.database]) > 200:
            self.recent_modifications[feedback.database] = self.recent_modifications[feedback.database][-200:]
    
    def _update_entity_counts(self, feedback: ExecutionFeedback):
        """Update entity counts based on SELECT query results"""
        if feedback.database in self.entity_maps:
            table_name = self._extract_table_name(feedback.query)
            if table_name != 'unknown':
                # Estimate entity count based on returned rows and query pattern
                if 'COUNT(*)' in feedback.query.upper():
                    # Direct count query
                    self.entity_maps[feedback.database].entity_counts[table_name] = feedback.rows_returned
                elif 'LIMIT' not in feedback.query.upper() and feedback.rows_returned > 0:
                    # Full table scan - use returned rows as estimate
                    self.entity_maps[feedback.database].entity_counts[table_name] = feedback.rows_returned
                elif feedback.rows_returned > 0:
                    # Partial results - update if we have more data
                    current_count = self.entity_maps[feedback.database].entity_counts.get(table_name, 0)
                    if feedback.rows_returned > current_count:
                        self.entity_maps[feedback.database].entity_counts[table_name] = feedback.rows_returned
    
    def _update_relationship_tracking(self, feedback: ExecutionFeedback):
        """Update relationship tracking based on query patterns"""
        if feedback.database in self.entity_maps:
            # Analyze JOIN patterns to discover new relationships
            query_upper = feedback.query.upper()
            if 'JOIN' in query_upper and feedback.success:
                # Extract JOIN relationships (simplified)
                self._analyze_join_relationships(feedback.query, feedback.database)
    
    def _analyze_join_relationships(self, query: str, database: str):
        """Analyze JOIN patterns to update relationship knowledge"""
        try:
            # Simple JOIN analysis - could be enhanced with proper SQL parsing
            query_upper = query.upper()
            
            # Look for JOIN patterns
            import re
            join_pattern = r'(\w+)\s+(?:INNER\s+|LEFT\s+|RIGHT\s+)?JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
            matches = re.findall(join_pattern, query_upper)
            
            for match in matches:
                table1, table2, t1_alias, t1_col, t2_alias, t2_col = match
                
                # Update relationship knowledge
                entity_map = self.entity_maps[database]
                
                # Check if this is a new relationship
                existing_rels = entity_map.relationships.get(table1, [])
                is_new = not any(rel.to_table == table2 and rel.foreign_key == t1_col 
                               for rel in existing_rels)
                
                if is_new:
                    entity_map.add_relationship(table1, table2, 'many_to_one', t1_col)
                    self.logger.info(f"Discovered new relationship: {table1} -> {table2} via {t1_col}")
                    
        except Exception as e:
            self.logger.debug(f"Error analyzing JOIN relationships: {e}")
    
    def _update_database_states(self):
        """Update database states from collected information"""
        for database in self.entity_maps.keys():
            # Create performance metrics
            performance_metrics = PerformanceMetrics(
                avg_query_time=self.performance_tracker.get_avg_query_time(database),
                slow_query_count=self.performance_tracker.get_slow_query_count(database),
                connection_count=self.performance_tracker.connection_counts.get(database, 0),
                cache_hit_ratio=self.performance_tracker.success_rates.get(database, 0.8)
            )
            
            # Create database state
            entity_map = self.entity_maps[database]
            database_state = DatabaseState(
                entity_counts=entity_map.entity_counts.copy(),
                relationship_map=entity_map.relationships.copy(),
                constraint_violations=self.constraint_violations[database].copy(),
                recent_modifications=self.recent_modifications[database].copy(),
                performance_metrics=performance_metrics
            )
            
            self.database_states[database] = database_state
    
    def _cleanup_old_data(self):
        """Clean up old data to prevent memory leaks"""
        cutoff_time = datetime.now() - timedelta(hours=24)
        
        # Clean up old modifications
        for database in self.recent_modifications:
            self.recent_modifications[database] = [
                mod for mod in self.recent_modifications[database]
                if mod.timestamp > cutoff_time
            ]
        
        # Clean up old constraint violations (keep recent ones)
        for database in self.constraint_violations:
            if len(self.constraint_violations[database]) > 50:
                self.constraint_violations[database] = self.constraint_violations[database][-50:]
        
        # Clean up old slow queries
        cutoff_timestamp = datetime.now() - timedelta(hours=1)
        self.performance_tracker.slow_queries = [
            q for q in self.performance_tracker.slow_queries
            if q['timestamp'] > cutoff_timestamp
        ]
    
    def get_database_state(self, database: str) -> Optional[DatabaseState]:
        """
        Get current database state for a specific database
        
        Args:
            database: Database name
            
        Returns:
            DatabaseState or None if not found
        """
        return self.database_states.get(database)
    
    def get_all_database_states(self) -> Dict[str, DatabaseState]:
        """Get all current database states"""
        return self.database_states.copy()
    
    def get_entity_relationship_map(self, database: str) -> Optional[EntityRelationshipMap]:
        """
        Get entity relationship map for a database
        
        Args:
            database: Database name
            
        Returns:
            EntityRelationshipMap or None if not found
        """
        return self.entity_maps.get(database)
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get overall performance metrics"""
        return {
            'avg_query_time_overall': self.performance_tracker.get_avg_query_time(),
            'total_slow_queries': self.performance_tracker.get_slow_query_count(),
            'error_counts': dict(self.performance_tracker.error_counts),
            'success_rates': dict(self.performance_tracker.success_rates),
            'databases_tracked': list(self.database_states.keys()),
            'total_feedback_processed': len(self.performance_tracker.query_times),
            'last_update': datetime.now().isoformat()
        }
    
    def reset_state(self):
        """Reset all state tracking"""
        with self._lock:
            self.database_states.clear()
            self.constraint_violations.clear()
            self.recent_modifications.clear()
            self.feedback_queue.clear()
            
            # Reset performance tracker
            self.performance_tracker = PerformanceTracker()
            
            # Reinitialize schemas
            self._initialize_vietnamese_database_schemas()
        
        self.logger.info("Database state synchronization reset")


# Utility functions for integration with executor
def create_execution_feedback_from_executor_result(
    query: str, 
    success: bool, 
    execution_time: float,
    error_message: Optional[str],
    rows_affected: int,
    rows_returned: int,
    database: str,
    username: str,
    role: str
) -> ExecutionFeedback:
    """
    Create ExecutionFeedback from executor result
    
    Args:
        query: SQL query that was executed
        success: Whether execution was successful
        execution_time: Execution time in seconds
        error_message: Error message if failed
        rows_affected: Number of rows affected (for INSERT/UPDATE/DELETE)
        rows_returned: Number of rows returned (for SELECT)
        database: Target database
        username: User who executed the query
        role: User role
        
    Returns:
        ExecutionFeedback object
    """
    # Determine query type
    query_upper = query.strip().upper()
    if query_upper.startswith('SELECT'):
        query_type = 'SELECT'
    elif query_upper.startswith('INSERT'):
        query_type = 'INSERT'
    elif query_upper.startswith('UPDATE'):
        query_type = 'UPDATE'
    elif query_upper.startswith('DELETE'):
        query_type = 'DELETE'
    elif query_upper.startswith('CREATE'):
        query_type = 'CREATE'
    elif query_upper.startswith('DROP'):
        query_type = 'DROP'
    elif query_upper.startswith('ALTER'):
        query_type = 'ALTER'
    else:
        query_type = 'OTHER'
    
    # Extract tables accessed (simplified)
    tables_accessed = []
    try:
        import re
        # Simple table extraction - could be enhanced
        table_pattern = r'(?:FROM|JOIN|INTO|UPDATE)\s+(?:`?(\w+)`?\.)?`?(\w+)`?'
        matches = re.findall(table_pattern, query_upper)
        for match in matches:
            table_name = match[1] if match[1] else match[0]
            if table_name and table_name not in tables_accessed:
                tables_accessed.append(table_name.lower())
    except Exception:
        pass
    
    return ExecutionFeedback(
        query=query,
        success=success,
        execution_time=execution_time,
        error_message=error_message,
        rows_affected=rows_affected,
        rows_returned=rows_returned,
        database=database,
        username=username,
        role=role,
        timestamp=datetime.now(),
        query_type=query_type,
        tables_accessed=tables_accessed
    )


if __name__ == "__main__":
    # Example usage and testing
    print("🔄 TESTING DATABASE STATE SYNCHRONIZATION")
    print("=" * 50)
    
    # Create synchronizer
    sync = DatabaseStateSynchronizer(update_interval=0.5)
    
    # Start synchronization
    sync.start_synchronization()
    
    # Simulate some execution feedback
    test_feedback = [
        ExecutionFeedback(
            query="SELECT * FROM sales_db.customers WHERE city = 'Hồ Chí Minh'",
            success=True,
            execution_time=0.15,
            error_message=None,
            rows_affected=0,
            rows_returned=25,
            database="sales_db",
            username="nguyen_van_nam",
            role="SALES",
            timestamp=datetime.now(),
            query_type="SELECT",
            tables_accessed=["customers"]
        ),
        ExecutionFeedback(
            query="INSERT INTO sales_db.orders (customer_id, total) VALUES (1, 1500000)",
            success=False,
            execution_time=0.05,
            error_message="Foreign key constraint violation: customer_id does not exist",
            rows_affected=0,
            rows_returned=0,
            database="sales_db",
            username="tran_thi_lan",
            role="SALES",
            timestamp=datetime.now(),
            query_type="INSERT",
            tables_accessed=["orders"]
        )
    ]
    
    # Add feedback
    for feedback in test_feedback:
        sync.add_execution_feedback(feedback)
    
    # Wait for processing
    time.sleep(1.0)
    
    # Check results
    sales_state = sync.get_database_state("sales_db")
    if sales_state:
        print(f"📊 Sales DB State:")
        print(f"• Entity counts: {sales_state.entity_counts}")
        print(f"• Constraint violations: {len(sales_state.constraint_violations)}")
        print(f"• Recent modifications: {len(sales_state.recent_modifications)}")
        print(f"• Avg query time: {sales_state.performance_metrics.avg_query_time:.3f}s")
    
    # Check performance metrics
    perf_metrics = sync.get_performance_metrics()
    print(f"\n📈 Performance Metrics:")
    for key, value in perf_metrics.items():
        print(f"• {key}: {value}")
    
    # Check entity relationships
    entity_map = sync.get_entity_relationship_map("sales_db")
    if entity_map:
        print(f"\n🔗 Entity Relationships (sales_db):")
        for table, relationships in entity_map.relationships.items():
            print(f"• {table}: {len(relationships)} relationships")
    
    # Stop synchronization
    sync.stop_synchronization()
    
    print(f"\n✅ Database State Synchronization ready for integration")