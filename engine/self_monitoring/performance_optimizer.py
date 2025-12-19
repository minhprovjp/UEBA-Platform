"""
Performance Optimization Module for UBA Self-Monitoring System

This module implements efficient data structures for real-time processing,
caching strategies for threat patterns, and parallel processing capabilities
for high throughput monitoring.
"""

import threading
import time
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple, Callable
from dataclasses import dataclass, field
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import queue
import logging
import weakref
import pickle
import os
from pathlib import Path

try:
    from .interfaces import InfrastructureEvent, ThreatDetection, ComponentType
except ImportError:
    from interfaces import InfrastructureEvent, ThreatDetection, ComponentType


@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    data: Any
    timestamp: datetime
    access_count: int = 0
    last_accessed: datetime = field(default_factory=datetime.now)
    ttl_seconds: int = 3600  # 1 hour default TTL


@dataclass
class ProcessingMetrics:
    """Performance metrics for processing operations"""
    total_events_processed: int = 0
    average_processing_time_ms: float = 0.0
    peak_processing_time_ms: float = 0.0
    cache_hit_rate: float = 0.0
    parallel_tasks_completed: int = 0
    queue_utilization: float = 0.0


class LRUCache:
    """
    High-performance LRU cache with TTL support for threat patterns
    and detection results.
    """
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 3600):
        """
        Initialize LRU cache.
        
        Args:
            max_size: Maximum number of entries
            default_ttl: Default time-to-live in seconds
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: Dict[str, CacheEntry] = {}
        self._access_order = deque()
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        
        # Start cleanup thread
        self._cleanup_thread = threading.Thread(target=self._cleanup_expired, daemon=True)
        self._cleanup_thread.start()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None
            
            entry = self._cache[key]
            
            # Check TTL
            if self._is_expired(entry):
                del self._cache[key]
                self._access_order.remove(key)
                self._misses += 1
                return None
            
            # Update access metadata
            entry.access_count += 1
            entry.last_accessed = datetime.now()
            
            # Move to end (most recently used)
            self._access_order.remove(key)
            self._access_order.append(key)
            
            self._hits += 1
            return entry.data
    
    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Put value in cache"""
        with self._lock:
            ttl = ttl or self.default_ttl
            
            # Remove if already exists
            if key in self._cache:
                self._access_order.remove(key)
            
            # Evict if at capacity
            elif len(self._cache) >= self.max_size:
                self._evict_lru()
            
            # Add new entry
            entry = CacheEntry(
                data=value,
                timestamp=datetime.now(),
                ttl_seconds=ttl
            )
            
            self._cache[key] = entry
            self._access_order.append(key)
    
    def _evict_lru(self):
        """Evict least recently used entry"""
        if self._access_order:
            lru_key = self._access_order.popleft()
            del self._cache[lru_key]
    
    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if cache entry is expired"""
        age = (datetime.now() - entry.timestamp).total_seconds()
        return age > entry.ttl_seconds
    
    def _cleanup_expired(self):
        """Background thread to clean up expired entries"""
        while True:
            try:
                with self._lock:
                    expired_keys = [
                        key for key, entry in self._cache.items()
                        if self._is_expired(entry)
                    ]
                    
                    for key in expired_keys:
                        del self._cache[key]
                        if key in self._access_order:
                            self._access_order.remove(key)
                
                time.sleep(60)  # Cleanup every minute
                
            except Exception as e:
                logging.error(f"Error in cache cleanup: {e}")
                time.sleep(60)
    
    def get_hit_rate(self) -> float:
        """Get cache hit rate"""
        total = self._hits + self._misses
        return self._hits / total if total > 0 else 0.0
    
    def clear(self):
        """Clear all cache entries"""
        with self._lock:
            self._cache.clear()
            self._access_order.clear()
            self._hits = 0
            self._misses = 0


class CircularBuffer:
    """
    High-performance circular buffer for storing recent events
    with O(1) insertion and efficient range queries.
    """
    
    def __init__(self, capacity: int):
        """
        Initialize circular buffer.
        
        Args:
            capacity: Maximum number of elements
        """
        self.capacity = capacity
        self._buffer = [None] * capacity
        self._head = 0
        self._size = 0
        self._lock = threading.RLock()
    
    def append(self, item: Any) -> None:
        """Add item to buffer"""
        with self._lock:
            self._buffer[self._head] = item
            self._head = (self._head + 1) % self.capacity
            
            if self._size < self.capacity:
                self._size += 1
    
    def get_recent(self, count: int) -> List[Any]:
        """Get most recent items"""
        with self._lock:
            if count >= self._size:
                # Return all items in order
                if self._size < self.capacity:
                    return [item for item in self._buffer[:self._size] if item is not None]
                else:
                    # Buffer is full, need to handle wraparound
                    return (self._buffer[self._head:] + self._buffer[:self._head])
            
            # Get last 'count' items
            result = []
            pos = (self._head - 1) % self.capacity
            
            for _ in range(min(count, self._size)):
                if self._buffer[pos] is not None:
                    result.append(self._buffer[pos])
                pos = (pos - 1) % self.capacity
            
            return list(reversed(result))
    
    def get_range(self, start_time: datetime, end_time: datetime) -> List[Any]:
        """Get items within time range (assumes items have timestamp attribute)"""
        with self._lock:
            result = []
            
            for i in range(self._size):
                pos = (self._head - 1 - i) % self.capacity
                item = self._buffer[pos]
                
                if item is not None and hasattr(item, 'timestamp'):
                    if start_time <= item.timestamp <= end_time:
                        result.append(item)
                    elif item.timestamp < start_time:
                        break  # Items are in reverse chronological order
            
            return list(reversed(result))
    
    def size(self) -> int:
        """Get current size"""
        return self._size
    
    def is_full(self) -> bool:
        """Check if buffer is full"""
        return self._size == self.capacity


class ParallelProcessor:
    """
    Parallel processing engine for high-throughput event analysis
    and threat detection.
    """
    
    def __init__(self, max_workers: int = None, use_processes: bool = False):
        """
        Initialize parallel processor.
        
        Args:
            max_workers: Maximum number of worker threads/processes
            use_processes: Use processes instead of threads for CPU-bound tasks
        """
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) + 4)
        self.use_processes = use_processes
        self.logger = logging.getLogger(__name__)
        
        # Create executor
        if use_processes:
            self.executor = ProcessPoolExecutor(max_workers=self.max_workers)
        else:
            self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # Processing metrics
        self.metrics = ProcessingMetrics()
        self._processing_times = deque(maxlen=1000)
        self._lock = threading.Lock()
    
    def process_batch_parallel(self, items: List[Any], 
                             processor_func: Callable[[Any], Any],
                             batch_size: int = 10) -> List[Any]:
        """
        Process items in parallel batches.
        
        Args:
            items: Items to process
            processor_func: Function to process each item
            batch_size: Size of each batch
            
        Returns:
            List of processed results
        """
        if not items:
            return []
        
        start_time = time.time()
        results = []
        
        try:
            # Split items into batches
            batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
            
            # Submit batch processing tasks
            future_to_batch = {
                self.executor.submit(self._process_batch, batch, processor_func): batch
                for batch in batches
            }
            
            # Collect results
            for future in as_completed(future_to_batch):
                try:
                    batch_results = future.result(timeout=30)
                    results.extend(batch_results)
                except Exception as e:
                    self.logger.error(f"Error processing batch: {e}")
                    # Continue with other batches
            
            # Update metrics
            processing_time = (time.time() - start_time) * 1000
            self._update_metrics(len(items), processing_time)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in parallel processing: {e}")
            return []
    
    def _process_batch(self, batch: List[Any], processor_func: Callable[[Any], Any]) -> List[Any]:
        """Process a single batch of items"""
        results = []
        
        for item in batch:
            try:
                result = processor_func(item)
                if result is not None:
                    results.append(result)
            except Exception as e:
                self.logger.error(f"Error processing item: {e}")
                # Continue with other items
        
        return results
    
    def process_with_timeout(self, item: Any, processor_func: Callable[[Any], Any],
                           timeout_seconds: float = 5.0) -> Optional[Any]:
        """
        Process single item with timeout.
        
        Args:
            item: Item to process
            processor_func: Processing function
            timeout_seconds: Timeout in seconds
            
        Returns:
            Processed result or None if timeout/error
        """
        try:
            future = self.executor.submit(processor_func, item)
            return future.result(timeout=timeout_seconds)
        except Exception as e:
            self.logger.error(f"Error processing item with timeout: {e}")
            return None
    
    def _update_metrics(self, items_processed: int, processing_time_ms: float):
        """Update processing metrics"""
        with self._lock:
            self.metrics.total_events_processed += items_processed
            self.metrics.parallel_tasks_completed += 1
            
            # Update processing times
            self._processing_times.append(processing_time_ms)
            
            # Calculate averages
            if self._processing_times:
                self.metrics.average_processing_time_ms = sum(self._processing_times) / len(self._processing_times)
                self.metrics.peak_processing_time_ms = max(self._processing_times)
    
    def get_metrics(self) -> ProcessingMetrics:
        """Get current processing metrics"""
        return self.metrics
    
    def shutdown(self):
        """Shutdown the executor"""
        self.executor.shutdown(wait=True)


class ThreatPatternCache:
    """
    Specialized cache for threat detection patterns with intelligent
    preloading and pattern matching optimization.
    """
    
    def __init__(self, cache_size: int = 5000):
        """Initialize threat pattern cache"""
        self.cache = LRUCache(cache_size, default_ttl=7200)  # 2 hours TTL
        self.pattern_index: Dict[str, Set[str]] = defaultdict(set)
        self.compiled_patterns: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
    
    def add_pattern(self, pattern_id: str, pattern_data: Dict[str, Any]):
        """Add threat pattern to cache"""
        with self._lock:
            # Cache the pattern
            self.cache.put(pattern_id, pattern_data)
            
            # Index by keywords for fast lookup
            keywords = pattern_data.get('keywords', [])
            for keyword in keywords:
                self.pattern_index[keyword.lower()].add(pattern_id)
            
            # Pre-compile regex patterns if present
            if 'regex' in pattern_data:
                try:
                    import re
                    self.compiled_patterns[pattern_id] = re.compile(
                        pattern_data['regex'], 
                        re.IGNORECASE
                    )
                except Exception as e:
                    self.logger.error(f"Error compiling pattern {pattern_id}: {e}")
    
    def find_matching_patterns(self, text: str) -> List[Tuple[str, Dict[str, Any]]]:
        """Find patterns that match the given text"""
        matches = []
        text_lower = text.lower()
        
        with self._lock:
            # Find patterns by keyword matching
            candidate_patterns = set()
            
            for keyword, pattern_ids in self.pattern_index.items():
                if keyword in text_lower:
                    candidate_patterns.update(pattern_ids)
            
            # Check each candidate pattern
            for pattern_id in candidate_patterns:
                pattern_data = self.cache.get(pattern_id)
                if pattern_data is None:
                    continue
                
                # Check compiled regex if available
                if pattern_id in self.compiled_patterns:
                    if self.compiled_patterns[pattern_id].search(text):
                        matches.append((pattern_id, pattern_data))
                else:
                    # Fallback to simple keyword matching
                    keywords = pattern_data.get('keywords', [])
                    if any(keyword.lower() in text_lower for keyword in keywords):
                        matches.append((pattern_id, pattern_data))
        
        return matches
    
    def preload_common_patterns(self, pattern_file: str):
        """Preload common threat patterns from file"""
        try:
            if os.path.exists(pattern_file):
                with open(pattern_file, 'r') as f:
                    import json
                    patterns = json.load(f)
                    
                    for pattern_id, pattern_data in patterns.items():
                        self.add_pattern(pattern_id, pattern_data)
                
                self.logger.info(f"Preloaded {len(patterns)} threat patterns")
        except Exception as e:
            self.logger.error(f"Error preloading patterns: {e}")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'hit_rate': self.cache.get_hit_rate(),
            'pattern_count': len(self.pattern_index),
            'compiled_patterns': len(self.compiled_patterns)
        }


class PerformanceOptimizer:
    """
    Main performance optimization coordinator that manages all
    optimization strategies and provides unified performance monitoring.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize performance optimizer.
        
        Args:
            config: Performance optimization configuration
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize optimization components
        cache_config = config.get('cache', {})
        self.event_cache = LRUCache(
            max_size=cache_config.get('event_cache_size', 10000),
            default_ttl=cache_config.get('event_ttl', 1800)
        )
        
        self.threat_pattern_cache = ThreatPatternCache(
            cache_size=cache_config.get('pattern_cache_size', 5000)
        )
        
        # Event buffer for recent events
        buffer_config = config.get('buffer', {})
        self.event_buffer = CircularBuffer(
            capacity=buffer_config.get('event_buffer_size', 50000)
        )
        
        # Parallel processor
        parallel_config = config.get('parallel', {})
        self.parallel_processor = ParallelProcessor(
            max_workers=parallel_config.get('max_workers'),
            use_processes=parallel_config.get('use_processes', False)
        )
        
        # Performance metrics
        self.metrics = ProcessingMetrics()
        self._start_time = datetime.now()
        
        # Preload patterns if configured
        pattern_file = config.get('threat_patterns_file')
        if pattern_file:
            self.threat_pattern_cache.preload_common_patterns(pattern_file)
    
    def optimize_event_processing(self, events: List[InfrastructureEvent]) -> List[InfrastructureEvent]:
        """
        Optimize event processing using caching and deduplication.
        
        Args:
            events: Events to process
            
        Returns:
            Optimized list of events
        """
        if not events:
            return []
        
        optimized_events = []
        
        for event in events:
            # Create cache key
            cache_key = self._create_event_cache_key(event)
            
            # Check if we've seen this event recently
            cached_event = self.event_cache.get(cache_key)
            if cached_event is not None:
                # Skip duplicate event
                continue
            
            # Cache the event
            self.event_cache.put(cache_key, event)
            
            # Add to buffer for recent access
            self.event_buffer.append(event)
            
            optimized_events.append(event)
        
        return optimized_events
    
    def optimize_threat_detection(self, events: List[InfrastructureEvent],
                                detection_func: Callable[[List[InfrastructureEvent]], List[ThreatDetection]]) -> List[ThreatDetection]:
        """
        Optimize threat detection using parallel processing and caching.
        
        Args:
            events: Events to analyze
            detection_func: Threat detection function
            
        Returns:
            Detected threats
        """
        if not events:
            return []
        
        # Use parallel processing for large batches
        if len(events) > self.config.get('parallel_threshold', 100):
            return self.parallel_processor.process_batch_parallel(
                events, 
                lambda batch: detection_func([batch]) if isinstance(batch, list) else detection_func([batch]),
                batch_size=self.config.get('detection_batch_size', 20)
            )
        else:
            # Process normally for small batches
            return detection_func(events)
    
    def get_recent_events(self, count: int = 1000) -> List[InfrastructureEvent]:
        """Get recent events from buffer"""
        return self.event_buffer.get_recent(count)
    
    def get_events_in_range(self, start_time: datetime, end_time: datetime) -> List[InfrastructureEvent]:
        """Get events within time range from buffer"""
        return self.event_buffer.get_range(start_time, end_time)
    
    def find_threat_patterns(self, text: str) -> List[Tuple[str, Dict[str, Any]]]:
        """Find matching threat patterns in text"""
        return self.threat_pattern_cache.find_matching_patterns(text)
    
    def _create_event_cache_key(self, event: InfrastructureEvent) -> str:
        """Create cache key for event"""
        # Use event content hash for deduplication
        content = f"{event.event_type}:{event.source_ip}:{event.user_account}:{event.target_component.value}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get comprehensive performance metrics"""
        uptime = (datetime.now() - self._start_time).total_seconds()
        
        return {
            'uptime_seconds': uptime,
            'event_cache_hit_rate': self.event_cache.get_hit_rate(),
            'threat_pattern_cache_stats': self.threat_pattern_cache.get_cache_stats(),
            'event_buffer_utilization': self.event_buffer.size() / self.event_buffer.capacity,
            'parallel_processing_metrics': self.parallel_processor.get_metrics().__dict__,
            'events_per_second': self.metrics.total_events_processed / uptime if uptime > 0 else 0
        }
    
    def optimize_memory_usage(self):
        """Optimize memory usage by clearing old caches"""
        try:
            # Manually clean up expired entries without sleeping
            with self.event_cache._lock:
                expired_keys = [
                    key for key, entry in self.event_cache._cache.items()
                    if self.event_cache._is_expired(entry)
                ]
                
                for key in expired_keys:
                    del self.event_cache._cache[key]
                    if key in self.event_cache._access_order:
                        self.event_cache._access_order.remove(key)
            
            # Force garbage collection if available
            try:
                import gc
                gc.collect()
            except ImportError:
                pass
            
            self.logger.debug("Memory optimization completed")
            
        except Exception as e:
            self.logger.error(f"Error optimizing memory usage: {e}")
    
    def shutdown(self):
        """Shutdown performance optimizer"""
        try:
            self.parallel_processor.shutdown()
            self.event_cache.clear()
            self.logger.info("Performance optimizer shutdown completed")
        except Exception as e:
            self.logger.error(f"Error shutting down performance optimizer: {e}")


# Factory function for creating optimized components
def create_performance_optimizer(config: Dict[str, Any]) -> PerformanceOptimizer:
    """
    Factory function to create performance optimizer with default configuration.
    
    Args:
        config: Performance configuration
        
    Returns:
        Configured PerformanceOptimizer instance
    """
    default_config = {
        'cache': {
            'event_cache_size': 10000,
            'event_ttl': 1800,
            'pattern_cache_size': 5000
        },
        'buffer': {
            'event_buffer_size': 50000
        },
        'parallel': {
            'max_workers': min(32, (os.cpu_count() or 1) + 4),
            'use_processes': False
        },
        'parallel_threshold': 100,
        'detection_batch_size': 20
    }
    
    # Merge with provided config
    merged_config = {**default_config, **config}
    
    return PerformanceOptimizer(merged_config)