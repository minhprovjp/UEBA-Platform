"""
Comprehensive Logging and Monitoring for Dynamic SQL Generation System

Provides detailed logging of generation decisions, context factors, pattern selection,
metrics collection for performance and success rates, and monitoring capabilities.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict, field
from pathlib import Path
from collections import defaultdict, deque
import threading
from contextlib import contextmanager


@dataclass
class GenerationMetrics:
    """Metrics for SQL generation performance and quality"""
    # Performance metrics
    generation_time_ms: float = 0.0
    context_analysis_time_ms: float = 0.0
    pattern_selection_time_ms: float = 0.0
    query_construction_time_ms: float = 0.0
    
    # Quality metrics
    query_complexity_score: int = 0
    vietnamese_pattern_usage: int = 0
    cultural_constraints_applied: int = 0
    business_logic_adherence: float = 0.0
    
    # Success metrics
    generation_successful: bool = True
    execution_successful: Optional[bool] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    
    # Context metrics
    context_completeness: float = 0.0
    database_state_freshness: float = 0.0
    user_expertise_level: int = 0
    business_context_richness: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class GenerationDecision:
    """Record of a generation decision and its context"""
    timestamp: str
    query_id: str
    user_id: str
    database: str
    intent_type: str
    
    # Context factors
    context_factors: Dict[str, Any] = field(default_factory=dict)
    pattern_selection_reason: str = ""
    complexity_decision: str = ""
    vietnamese_patterns_used: List[str] = field(default_factory=list)
    
    # Generation details
    generated_query: str = ""
    fallback_used: bool = False
    generation_strategy: str = ""
    
    # Metrics
    metrics: GenerationMetrics = field(default_factory=GenerationMetrics)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        data = asdict(self)
        data['metrics'] = self.metrics.to_dict()
        return data


@dataclass
class SystemMetrics:
    """System-wide metrics for monitoring"""
    # Generation statistics
    total_generations: int = 0
    successful_generations: int = 0
    failed_generations: int = 0
    fallback_generations: int = 0
    
    # Performance statistics
    avg_generation_time_ms: float = 0.0
    min_generation_time_ms: float = float('inf')
    max_generation_time_ms: float = 0.0
    
    # Pattern usage statistics
    vietnamese_pattern_usage_rate: float = 0.0
    cultural_constraint_application_rate: float = 0.0
    business_logic_adherence_rate: float = 0.0
    
    # Error statistics
    error_counts: Dict[str, int] = field(default_factory=dict)
    
    # Database usage statistics
    database_usage_counts: Dict[str, int] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


class GenerationLogger:
    """
    Comprehensive logger for SQL generation decisions and context
    """
    
    def __init__(self, log_dir: str = "logs", log_level: str = "INFO"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Set up logging
        self.logger = logging.getLogger("dynamic_sql_generation")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # File handlers
        self._setup_file_handlers(detailed_formatter)
        
        # Console handler - REMOVED to reduce noise
        # console_handler = logging.StreamHandler()
        # console_handler.setLevel(logging.INFO)
        # console_handler.setFormatter(detailed_formatter)
        # self.logger.addHandler(console_handler)
        
        # Decision log file
        self.decision_log_file = self.log_dir / "generation_decisions.jsonl"
        
        self.logger.info("Generation logger initialized")
    
    def _setup_file_handlers(self, formatter):
        """Set up file handlers for different log types"""
        # Main log file
        main_handler = logging.FileHandler(
            self.log_dir / "dynamic_sql_generation.log",
            encoding='utf-8'
        )
        main_handler.setLevel(logging.DEBUG)
        main_handler.setFormatter(formatter)
        self.logger.addHandler(main_handler)
        
        # Error log file
        error_handler = logging.FileHandler(
            self.log_dir / "generation_errors.log",
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        self.logger.addHandler(error_handler)
        
        # Performance log file
        perf_handler = logging.FileHandler(
            self.log_dir / "generation_performance.log",
            encoding='utf-8'
        )
        perf_handler.setLevel(logging.INFO)
        perf_handler.setFormatter(formatter)
        self.logger.addHandler(perf_handler)
    
    def log_generation_start(self, query_id: str, user_id: str, intent: Dict[str, Any]):
        """Log the start of query generation"""
        self.logger.debug(
            f"Starting generation - Query ID: {query_id}, User: {user_id}, "
            f"Intent: {intent.get('type', 'unknown')}"
        )
    
    def log_context_analysis(self, query_id: str, context_factors: Dict[str, Any], 
                           analysis_time_ms: float):
        """Log context analysis results"""
        self.logger.debug(
            f"Context analysis completed - Query ID: {query_id}, "
            f"Time: {analysis_time_ms:.2f}ms, Factors: {len(context_factors)}"
        )
        
        # Log detailed context factors
        for factor, value in context_factors.items():
            self.logger.debug(f"  {factor}: {value}")
    
    def log_pattern_selection(self, query_id: str, selected_patterns: List[str], 
                            selection_reason: str, selection_time_ms: float):
        """Log pattern selection decision"""
        self.logger.debug(
            f"Pattern selection - Query ID: {query_id}, "
            f"Patterns: {selected_patterns}, Reason: {selection_reason}, "
            f"Time: {selection_time_ms:.2f}ms"
        )
    
    def log_complexity_decision(self, query_id: str, complexity_level: int, 
                              decision_factors: Dict[str, Any]):
        """Log query complexity decision"""
        self.logger.debug(
            f"Complexity decision - Query ID: {query_id}, "
            f"Level: {complexity_level}, Factors: {decision_factors}"
        )
    
    def log_generation_success(self, query_id: str, generated_query: str, 
                             generation_time_ms: float, metrics: GenerationMetrics):
        """Log successful query generation"""
        self.logger.debug(
            f"Generation successful - Query ID: {query_id}, "
            f"Time: {generation_time_ms:.2f}ms, "
            f"Complexity: {metrics.query_complexity_score}"
        )
        
        # Log performance metrics
        self.logger.debug(
            f"Performance metrics - Query ID: {query_id}, "
            f"Context: {metrics.context_analysis_time_ms:.2f}ms, "
            f"Pattern: {metrics.pattern_selection_time_ms:.2f}ms, "
            f"Construction: {metrics.query_construction_time_ms:.2f}ms"
        )
    
    def log_generation_failure(self, query_id: str, error_type: str, 
                             error_message: str, fallback_used: bool):
        """Log failed query generation"""
        self.logger.error(
            f"Generation failed - Query ID: {query_id}, "
            f"Error: {error_type}, Message: {error_message}, "
            f"Fallback: {fallback_used}"
        )
    
    def log_vietnamese_pattern_usage(self, query_id: str, patterns_used: List[str], 
                                   cultural_constraints: List[str]):
        """Log Vietnamese pattern and cultural constraint usage"""
        self.logger.debug(
            f"Vietnamese patterns - Query ID: {query_id}, "
            f"Patterns: {patterns_used}, Constraints: {cultural_constraints}"
        )
    
    def log_decision(self, decision: GenerationDecision):
        """Log a complete generation decision"""
        # Log to structured decision log
        with open(self.decision_log_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(decision.to_dict(), ensure_ascii=False) + '\n')
        
        # Log summary to main log
        self.logger.debug(
            f"Decision logged - Query ID: {decision.query_id}, "
            f"Success: {decision.metrics.generation_successful}, "
            f"Time: {decision.metrics.generation_time_ms:.2f}ms"
        )


class MetricsCollector:
    """
    Collects and aggregates metrics for generation quality and performance
    """
    
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.metrics_history = deque(maxlen=window_size)
        self.system_metrics = SystemMetrics()
        self.lock = threading.Lock()
        
        # Real-time metrics tracking
        self.generation_times = deque(maxlen=window_size)
        self.success_rates = deque(maxlen=100)  # Track success rate over last 100 generations
        
        self.logger = logging.getLogger("metrics_collector")
    
    def record_generation(self, decision: GenerationDecision):
        """Record a generation decision and update metrics"""
        with self.lock:
            self.metrics_history.append(decision)
            self._update_system_metrics(decision)
            
            # Update real-time tracking
            self.generation_times.append(decision.metrics.generation_time_ms)
            self.success_rates.append(1.0 if decision.metrics.generation_successful else 0.0)
    
    def _update_system_metrics(self, decision: GenerationDecision):
        """Update system-wide metrics"""
        metrics = decision.metrics
        
        # Update generation counts
        self.system_metrics.total_generations += 1
        if metrics.generation_successful:
            self.system_metrics.successful_generations += 1
        else:
            self.system_metrics.failed_generations += 1
        
        if decision.fallback_used:
            self.system_metrics.fallback_generations += 1
        
        # Update performance metrics
        gen_time = metrics.generation_time_ms
        if gen_time > 0:
            self.system_metrics.avg_generation_time_ms = (
                (self.system_metrics.avg_generation_time_ms * 
                 (self.system_metrics.total_generations - 1) + gen_time) /
                self.system_metrics.total_generations
            )
            self.system_metrics.min_generation_time_ms = min(
                self.system_metrics.min_generation_time_ms, gen_time
            )
            self.system_metrics.max_generation_time_ms = max(
                self.system_metrics.max_generation_time_ms, gen_time
            )
        
        # Update pattern usage
        if metrics.vietnamese_pattern_usage > 0:
            total_with_patterns = sum(1 for d in self.metrics_history 
                                    if d.metrics.vietnamese_pattern_usage > 0)
            self.system_metrics.vietnamese_pattern_usage_rate = (
                total_with_patterns / len(self.metrics_history)
            )
        
        # Update error counts
        if metrics.error_type:
            self.system_metrics.error_counts[metrics.error_type] = (
                self.system_metrics.error_counts.get(metrics.error_type, 0) + 1
            )
        
        # Update database usage
        self.system_metrics.database_usage_counts[decision.database] = (
            self.system_metrics.database_usage_counts.get(decision.database, 0) + 1
        )
    
    def get_current_metrics(self) -> SystemMetrics:
        """Get current system metrics"""
        with self.lock:
            return self.system_metrics
    
    def get_recent_performance(self) -> Dict[str, float]:
        """Get recent performance metrics"""
        with self.lock:
            if not self.generation_times:
                return {}
            
            times = list(self.generation_times)
            success_rates = list(self.success_rates)
            
            return {
                'avg_generation_time_ms': sum(times) / len(times),
                'min_generation_time_ms': min(times),
                'max_generation_time_ms': max(times),
                'success_rate': sum(success_rates) / len(success_rates) if success_rates else 0.0,
                'total_recent_generations': len(times)
            }
    
    def get_pattern_usage_stats(self) -> Dict[str, Any]:
        """Get Vietnamese pattern usage statistics"""
        with self.lock:
            if not self.metrics_history:
                return {}
            
            pattern_counts = defaultdict(int)
            constraint_counts = defaultdict(int)
            
            for decision in self.metrics_history:
                for pattern in decision.vietnamese_patterns_used:
                    pattern_counts[pattern] += 1
                
                if decision.metrics.cultural_constraints_applied > 0:
                    constraint_counts['cultural_constraints'] += 1
            
            return {
                'pattern_usage': dict(pattern_counts),
                'constraint_usage': dict(constraint_counts),
                'total_decisions': len(self.metrics_history)
            }
    
    def export_metrics(self, export_path: str):
        """Export metrics to file"""
        with self.lock:
            export_data = {
                'system_metrics': self.system_metrics.to_dict(),
                'recent_performance': self.get_recent_performance(),
                'pattern_usage_stats': self.get_pattern_usage_stats(),
                'export_timestamp': str(datetime.now()),
                'window_size': self.window_size,
                'total_decisions_recorded': len(self.metrics_history)
            }
            
            with open(export_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)


class MonitoringDashboard:
    """
    Simple monitoring dashboard for generation quality and Vietnamese pattern usage
    """
    
    def __init__(self, metrics_collector: MetricsCollector, 
                 generation_logger: GenerationLogger):
        self.metrics_collector = metrics_collector
        self.generation_logger = generation_logger
        self.logger = logging.getLogger("monitoring_dashboard")
    
    def print_system_status(self):
        """Print current system status"""
        metrics = self.metrics_collector.get_current_metrics()
        recent_perf = self.metrics_collector.get_recent_performance()
        pattern_stats = self.metrics_collector.get_pattern_usage_stats()
        
        print("\n" + "="*60)
        print("🔍 DYNAMIC SQL GENERATION MONITORING DASHBOARD")
        print("="*60)
        
        # System overview
        print(f"\n📊 SYSTEM OVERVIEW:")
        print(f"  Total Generations: {metrics.total_generations}")
        print(f"  Successful: {metrics.successful_generations}")
        print(f"  Failed: {metrics.failed_generations}")
        print(f"  Fallback Used: {metrics.fallback_generations}")
        
        if metrics.total_generations > 0:
            success_rate = (metrics.successful_generations / metrics.total_generations) * 100
            print(f"  Success Rate: {success_rate:.1f}%")
        
        # Performance metrics
        if recent_perf:
            print(f"\n⚡ RECENT PERFORMANCE:")
            print(f"  Avg Generation Time: {recent_perf['avg_generation_time_ms']:.2f}ms")
            print(f"  Min/Max Time: {recent_perf['min_generation_time_ms']:.2f}ms / {recent_perf['max_generation_time_ms']:.2f}ms")
            print(f"  Recent Success Rate: {recent_perf['success_rate']*100:.1f}%")
        
        # Vietnamese pattern usage
        if pattern_stats:
            print(f"\n🇻🇳 VIETNAMESE PATTERN USAGE:")
            pattern_usage = pattern_stats.get('pattern_usage', {})
            if pattern_usage:
                for pattern, count in sorted(pattern_usage.items(), key=lambda x: x[1], reverse=True)[:5]:
                    print(f"  {pattern}: {count} times")
            else:
                print("  No Vietnamese patterns used yet")
        
        # Database usage
        if metrics.database_usage_counts:
            print(f"\n💾 DATABASE USAGE:")
            for db, count in sorted(metrics.database_usage_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"  {db}: {count} queries")
        
        # Error summary
        if metrics.error_counts:
            print(f"\n❌ ERROR SUMMARY:")
            for error_type, count in sorted(metrics.error_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"  {error_type}: {count} occurrences")
        
        print("="*60)
    
    def generate_report(self, report_path: str):
        """Generate comprehensive monitoring report"""
        metrics = self.metrics_collector.get_current_metrics()
        recent_perf = self.metrics_collector.get_recent_performance()
        pattern_stats = self.metrics_collector.get_pattern_usage_stats()
        
        report_data = {
            'report_timestamp': str(datetime.now()),
            'system_metrics': metrics.to_dict(),
            'recent_performance': recent_perf,
            'pattern_usage_statistics': pattern_stats,
            'summary': {
                'total_generations': metrics.total_generations,
                'success_rate': (metrics.successful_generations / metrics.total_generations * 100) 
                               if metrics.total_generations > 0 else 0,
                'avg_generation_time_ms': metrics.avg_generation_time_ms,
                'vietnamese_pattern_usage_rate': metrics.vietnamese_pattern_usage_rate * 100,
                'most_used_database': max(metrics.database_usage_counts.items(), 
                                        key=lambda x: x[1])[0] if metrics.database_usage_counts else None
            }
        }
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Monitoring report generated: {report_path}")


# Global instances
_generation_logger = None
_metrics_collector = None
_monitoring_dashboard = None

def get_generation_logger() -> GenerationLogger:
    """Get global generation logger instance"""
    global _generation_logger
    if _generation_logger is None:
        _generation_logger = GenerationLogger()
    return _generation_logger

def get_metrics_collector() -> MetricsCollector:
    """Get global metrics collector instance"""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector

def get_monitoring_dashboard() -> MonitoringDashboard:
    """Get global monitoring dashboard instance"""
    global _monitoring_dashboard
    if _monitoring_dashboard is None:
        _monitoring_dashboard = MonitoringDashboard(
            get_metrics_collector(),
            get_generation_logger()
        )
    return _monitoring_dashboard

@contextmanager
def generation_timing(operation_name: str):
    """Context manager for timing generation operations"""
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        duration_ms = (end_time - start_time) * 1000
        logger = get_generation_logger()
        logger.logger.debug(f"{operation_name} completed in {duration_ms:.2f}ms")


if __name__ == "__main__":
    # Example usage and testing
    print("📊 TESTING MONITORING SYSTEM")
    print("=" * 50)
    
    # Create monitoring components
    logger = get_generation_logger()
    collector = get_metrics_collector()
    dashboard = get_monitoring_dashboard()
    
    # Simulate some generation decisions
    for i in range(5):
        decision = GenerationDecision(
            timestamp=str(datetime.now()),
            query_id=f"test_query_{i}",
            user_id=f"user_{i % 3}",
            database=f"sales_db" if i % 2 == 0 else "hr_db",
            intent_type="select",
            context_factors={"work_hours": True, "vietnamese_context": True},
            pattern_selection_reason="Vietnamese business patterns",
            vietnamese_patterns_used=["work_hours", "hierarchy"],
            generated_query=f"SELECT * FROM table_{i}",
            metrics=GenerationMetrics(
                generation_time_ms=50.0 + i * 10,
                query_complexity_score=i + 1,
                vietnamese_pattern_usage=2,
                generation_successful=True
            )
        )
        
        logger.log_decision(decision)
        collector.record_generation(decision)
    
    # Show dashboard
    dashboard.print_system_status()
    
    print(f"\n✅ Monitoring system ready for dynamic SQL generation")