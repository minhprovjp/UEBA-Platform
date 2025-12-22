"""
Query Pool Loader for Pre-Generated SQL Queries

This module provides functionality to load pre-generated SQL queries from JSON files
and integrate them into the main execution flow, replacing dynamic generation for
faster and more reliable query retrieval.
"""

import json
import os
import random
import threading
from typing import Dict, List, Optional, Any
from datetime import datetime


class QueryPoolLoader:
    """
    Thread-safe loader for pre-generated SQL query pools.
    
    This class loads queries from JSON files and provides efficient query selection
    based on database and intent criteria, with fallback to dynamic generation when
    queries are unavailable.
    """
    
    def __init__(self):
        self.pool = {}
        self.metadata = {}
        self.index = {}  # Fast lookup: "{database}_{intent}" -> [queries]
        self.usage_stats = {}  # Track query usage for analytics
        self.lock = threading.RLock()  # Thread-safe access
        self.loaded = False
    
    def load_pool(self, filepath: str) -> bool:
        """
        Load query pool from JSON file.
        
        Args:
            filepath: Path to the query pool JSON file
            
        Returns:
            True if loading succeeded, False otherwise
        """
        try:
            if not os.path.exists(filepath):
                print(f"⚠️  Query pool file not found: {filepath}")
                return False
            
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check if data has new metadata structure or old flat structure
            if "metadata" in data and "queries" in data:
                # New format with metadata
                self.metadata = data.get("metadata", {})
                self.pool = data.get("queries", {})
            else:
                # Old format - direct database->intent mapping
                self.pool = data
                self.metadata = {
                    "generated_at": "unknown",
                    "total_queries": sum(
                        len(queries) 
                        for db in self.pool.values() 
                        for queries in db.values()
                    )
                }
            
            # Build index for fast lookup
            self._build_index()
            
            self.loaded = True
            
            # Print loading summary
            total_queries = sum(len(queries) for queries in self.index.values())
            print(f"✅ Query Pool Loaded Successfully!")
            print(f"   📁 File: {filepath}")
            if self.metadata.get("generated_at"):
                print(f"   📅 Generated: {self.metadata['generated_at']}")
            print(f"   📊 Total Queries: {total_queries}")
            print(f"   🗂️  Database/Intent Combinations: {len(self.index)}")
            if self.metadata.get("duplicates_removed"):
                print(f"   🔄 Duplicates Removed: {self.metadata['duplicates_removed']}")
            
            return True
            
        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse query pool JSON: {e}")
            return False
        except Exception as e:
            print(f"❌ Failed to load query pool: {e}")
            return False
    
    def _build_index(self):
        """Build index for O(1) query lookup by database and intent."""
        self.index.clear()
        
        for database, intents in self.pool.items():
            for intent, queries in intents.items():
                key = f"{database}_{intent}"
                self.index[key] = queries if isinstance(queries, list) else []
        
        # Initialize usage stats
        for key in self.index.keys():
            self.usage_stats[key] = {
                'total_selections': 0,
                'query_distribution': {}
            }
    
    def get_query(self, database: str, intent: str) -> Optional[str]:
        """
        Get a random query for the specified database and intent.
        
        Args:
            database: Target database name (e.g., 'sales_db')
            intent: Query intent (e.g., 'SEARCH_CUSTOMER')
            
        Returns:
            SQL query string, or None if no matching query found
        """
        if not self.loaded:
            return None
        
        key = f"{database}_{intent}"
        
        with self.lock:
            if key not in self.index or not self.index[key]:
                return None
            
            # Randomly select from available queries
            query = random.choice(self.index[key])
            
            # Track usage statistics
            self.usage_stats[key]['total_selections'] += 1
            if query not in self.usage_stats[key]['query_distribution']:
                self.usage_stats[key]['query_distribution'][query] = 0
            self.usage_stats[key]['query_distribution'][query] += 1
            
            return query
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the query pool.
        
        Returns:
            Dictionary containing pool statistics
        """
        with self.lock:
            total_queries = sum(len(queries) for queries in self.index.values())
            
            # Calculate coverage by database
            coverage_by_database = {}
            for database in self.pool.keys():
                db_queries = sum(
                    len(queries) for intent, queries in self.pool[database].items()
                )
                coverage_by_database[database] = {
                    'intents': len(self.pool[database]),
                    'queries': db_queries
                }
            
            # Calculate most/least used queries
            total_selections = sum(
                stats['total_selections'] for stats in self.usage_stats.values()
            )
            
            # Fallback statistics
            fallbacks = sum(
                1 for key in self.index.keys()
                if not self.index[key]
            )
            
            return {
                'loaded': self.loaded,
                'total_queries': total_queries,
                'total_combinations': len(self.index),
                'databases_covered': len(self.pool),
                'coverage_by_database': coverage_by_database,
                'total_selections': total_selections,
                'missing_combinations': fallbacks,
                'coverage_percentage': (
                    ((len(self.index) - fallbacks) / len(self.index) * 100)
                    if len(self.index) > 0 else 0
                ),
                'metadata': self.metadata
            }
    
    def get_most_used_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get the most frequently used queries.
        
        Args:
            limit: Maximum number of queries to return
            
        Returns:
            List of query usage statistics
        """
        with self.lock:
            all_usages = []
            
            for key, stats in self.usage_stats.items():
                for query, count in stats['query_distribution'].items():
                    database, intent = key.split('_', 1)
                    all_usages.append({
                        'database': database,
                        'intent': intent,
                        'query': query,
                        'usage_count': count
                    })
            
            # Sort by usage count
            all_usages.sort(key=lambda x: x['usage_count'], reverse=True)
            
            return all_usages[:limit]
    
    def get_coverage_gaps(self) -> List[Dict[str, str]]:
        """
        Identify database/intent combinations with no queries.
        
        Returns:
            List of missing combinations
        """
        with self.lock:
            gaps = []
            
            for key, queries in self.index.items():
                if not queries:
                    database, intent = key.split('_', 1)
                    gaps.append({
                        'database': database,
                        'intent': intent
                    })
            
            return gaps
    
    def export_usage_report(self, filepath: str = "query_pool_usage_report.json"):
        """
        Export comprehensive usage statistics to JSON file.
        
        Args:
            filepath: Output file path
        """
        with self.lock:
            report = {
                'export_time': datetime.now().isoformat(),
                'pool_stats': self.get_pool_stats(),
                'most_used_queries': self.get_most_used_queries(20),
                'coverage_gaps': self.get_coverage_gaps(),
                'detailed_usage': self.usage_stats
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            print(f"📊 Usage report exported to: {filepath}")


# Example usage
if __name__ == "__main__":
    # Test the query pool loader
    loader = QueryPoolLoader()
    
    # Try to load the query pool
    if loader.load_pool("dynamic_sql_generation/ai_query_pool.json"):
        # Get some queries
        query1 = loader.get_query("sales_db", "SEARCH_CUSTOMER")
        query2 = loader.get_query("hr_db", "VIEW_PROFILE")
        
        if query1:
            print(f"\n📝 Sample query from sales_db.SEARCH_CUSTOMER:")
            print(f"   {query1[:100]}...")
        
        if query2:
            print(f"\n📝 Sample query from hr_db.VIEW_PROFILE:")
            print(f"   {query2[:100]}...")
        
        # Print statistics
        stats = loader.get_pool_stats()
        print(f"\n📊 Pool Statistics:")
        print(f"   Coverage: {stats['coverage_percentage']:.1f}%")
        print(f"   Missing Combinations: {stats['missing_combinations']}")
        
        # Show coverage gaps
        gaps = loader.get_coverage_gaps()
        if gaps:
            print(f"\n⚠️  Coverage Gaps:")
            for gap in gaps[:5]:
                print(f"   • {gap['database']}.{gap['intent']}")
    else:
        print("❌ Failed to load query pool")
