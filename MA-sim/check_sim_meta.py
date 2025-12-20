#!/usr/bin/env python3
import mysql.connector
from config import *

try:
    conn = mysql.connector.connect(
        host=MYSQL_LOG_HOST,
        port=MYSQL_LOG_PORT,
        user=MYSQL_LOG_USER,
        password=MYSQL_LOG_PASSWORD,
        database=MYSQL_LOG_DATABASE
    )
    cursor = conn.cursor()
    
    # Check for SIM_META queries
    cursor.execute("SELECT COUNT(*) FROM performance_schema.events_statements_history_long WHERE SQL_TEXT LIKE '%SIM_META%'")
    count = cursor.fetchone()[0]
    print(f'Queries with SIM_META tags: {count}')
    
    # Check total queries
    cursor.execute("SELECT COUNT(*) FROM performance_schema.events_statements_history_long WHERE SQL_TEXT IS NOT NULL")
    total = cursor.fetchone()[0]
    print(f'Total queries in history: {total}')
    
    # Check recent queries
    cursor.execute("SELECT COUNT(*) FROM performance_schema.events_statements_history_long WHERE TIMER_END > UNIX_TIMESTAMP(NOW() - INTERVAL 5 MINUTE) * 1000000000000")
    recent = cursor.fetchone()[0]
    print(f'Recent queries (last 5 min): {recent}')
    
    if count > 0:
        cursor.execute("SELECT SQL_TEXT FROM performance_schema.events_statements_history_long WHERE SQL_TEXT LIKE '%SIM_META%' ORDER BY TIMER_END DESC LIMIT 3")
        samples = cursor.fetchall()
        print('\nSample SIM_META queries:')
        for i, (sql,) in enumerate(samples, 1):
            print(f'{i}. {sql[:150]}...')
    else:
        # Show some recent queries to see what's actually being logged
        cursor.execute("SELECT SQL_TEXT FROM performance_schema.events_statements_history_long WHERE SQL_TEXT IS NOT NULL ORDER BY TIMER_END DESC LIMIT 5")
        samples = cursor.fetchall()
        print('\nSample recent queries (no SIM_META found):')
        for i, (sql,) in enumerate(samples, 1):
            print(f'{i}. {sql[:150]}...')
    
    conn.close()
    
except Exception as e:
    print(f'Error connecting to database: {e}')
    print('Make sure MySQL is running and config is correct')