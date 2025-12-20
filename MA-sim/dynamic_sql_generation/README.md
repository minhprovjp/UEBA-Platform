# Dynamic SQL Generation System

## Overview

The Dynamic SQL Generation System is a sophisticated, context-aware SQL query generation engine designed specifically for Vietnamese business environments. It replaces static SQL templates with intelligent, adaptive query generation that considers user expertise, business context, temporal patterns, and Vietnamese cultural factors.

## Features

### Core Capabilities
- **Context-Aware Generation**: Analyzes runtime context including database state, user behavior, and business workflows
- **Vietnamese Business Integration**: Incorporates Vietnamese work hours, holidays, business practices, and cultural hierarchies
- **Adaptive Complexity**: Adjusts query sophistication based on user expertise and business requirements
- **Attack Pattern Simulation**: Generates sophisticated attack queries for security testing and UBA training
- **Pattern Learning**: Learns from successful query executions to improve future generation

### Key Components

#### 1. Dynamic SQL Generator (`generator.py`)
- Central orchestrator for query generation
- Coordinates context analysis and generation strategies
- Manages pattern learning and feedback integration
- Provides fallback mechanisms for generation failures

#### 2. Query Context Engine (`context_engine.py`)
- Analyzes current database state and entity relationships
- Determines business workflow context and constraints
- Assesses temporal patterns and Vietnamese business hours
- Evaluates user expertise and query complexity requirements

#### 3. Query Complexity Engine (`complexity_engine.py`)
- Determines appropriate query sophistication based on user expertise
- Generates complex multi-table queries for experienced users
- Creates realistic analytical queries with aggregations and window functions
- Adapts complexity based on success rates and user behavior

#### 4. Vietnamese Business Patterns (`vietnamese_patterns.py`)
- Provides Vietnamese-specific business workflow patterns
- Applies cultural constraints (hierarchy, work hours, holidays)
- Ensures compliance with Vietnamese data protection standards
- Generates realistic parameter values using Vietnamese business data

#### 5. Database State Synchronization (`database_state_sync.py`)
- Maintains real-time database state updates
- Tracks entity relationships and constraint violations
- Collects performance metrics for generation quality assessment
- Provides feedback to improve generation accuracy

## Installation and Setup

### Prerequisites
- Python 3.8+
- Required packages: `hypothesis`, `pytest`, `dataclasses`, `enum34`

### Installation
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests to verify installation
python -m pytest dynamic_sql_generation/ -v
```

## Usage

### Basic Usage

```python
from dynamic_sql_generation import DynamicSQLGenerator
from dynamic_sql_generation.models import UserContext, BusinessContext

# Initialize generator
generator = DynamicSQLGenerator(seed=42)

# Create user context
user_context = UserContext(
    username='nguyen_van_a',
    role='SALES',
    department='Phòng Kinh Doanh',
    expertise_level=ExpertiseLevel.INTERMEDIATE,
    session_history=[],
    work_intensity=1.2,
    stress_level=0.3
)

# Create business context
business_context = BusinessContext(
    current_workflow=WorkflowType.SALES_PROCESS,
    business_event=BusinessEvent.MONTH_END_CLOSING,
    department_interactions=['finance', 'marketing'],
    compliance_requirements=[],
    data_sensitivity_level=SensitivityLevel.INTERNAL
)

# Generate query
intent = {
    'action': 'customer_analysis',
    'username': 'nguyen_van_a',
    'role': 'SALES',
    'target_database': 'sales_db'
}

result = generator.generate_query(intent, user_context, business_context)
print(f"Generated Query: {result.query}")
print(f"Complexity Level: {result.complexity_level}")
print(f"Generation Time: {result.generation_time_ms}ms")
```

### Advanced Usage

#### Attack Pattern Generation
```python
# Generate sophisticated attack patterns
attack_intent = {
    'action': 'data_extraction',
    'username': 'attacker',
    'role': 'HR',
    'target_database': 'hr_db',
    'attack_mode': True,
    'attack_type': 'insider_threat'
}

attack_result = generator.generate_query(attack_intent, user_context, business_context)
```

#### Pattern Learning and Feedback
```python
# Analyze query success and learn patterns
generator.analyze_query_success(
    query="SELECT * FROM customers WHERE city = 'Ho Chi Minh'",
    success=True,
    execution_time=45.2
)

# Get generation statistics
stats = generator.get_generation_stats()
print(f"Total generations: {stats['total_generations']}")
print(f"Success rate: {stats['success_rate']:.2%}")
```

## Configuration

### Vietnamese Business Configuration
The system uses configuration files in `config/vietnamese_business.json`:

```json
{
  "cities": ["Ho Chi Minh", "Ha Noi", "Da Nang", "Can Tho"],
  "companies": ["Vietcombank", "Vingroup", "FPT", "Techcombank"],
  "work_hours": {
    "start": 8,
    "end": 17,
    "lunch_start": 12,
    "lunch_end": 13
  },
  "holidays": ["2025-01-29", "2025-01-30", "2025-04-30"]
}
```

### Generation Configuration
Configure generation parameters in `config/generation.json`:

```json
{
  "max_generation_time_ms": 1000,
  "fallback_to_templates": true,
  "enable_pattern_learning": true,
  "max_history_size": 1000,
  "complexity_adaptation_threshold": 0.7
}
```

## Testing

The system includes comprehensive testing with both unit tests and property-based tests:

### Running Tests
```bash
# Run all tests
python -m pytest -v

# Run specific test categories
python -m pytest test_unit_comprehensive.py -v
python -m pytest test_integration_comprehensive.py -v
python -m pytest test_performance_quality_validation.py -v

# Run property-based tests only
python -m pytest -k "test_.*_adaptation" -v
```

### Property-Based Testing
The system uses Hypothesis for property-based testing to verify correctness properties:

- **Context-aware query generation**: Ensures queries incorporate database state and Vietnamese business patterns
- **Query complexity adaptation**: Verifies appropriate sophistication matching user capabilities
- **Temporal business pattern adaptation**: Tests adaptation to Vietnamese business hours and events
- **Generation consistency**: Ensures reproducible patterns with controlled variation
- **Error handling**: Validates graceful degradation under failure conditions
- **Attack pattern sophistication**: Tests generation of sophisticated attack queries

## Architecture

### System Integration
The dynamic SQL generation system integrates with existing components:

- **Enhanced SQL Translator**: Receives enhanced intents with contextual metadata
- **Agent Systems**: Provide user context, role information, and behavioral patterns
- **Database Executor**: Executes generated queries and provides success/failure feedback
- **Scenario Manager**: Provides attack context and rule-bypassing requirements

### Data Flow
1. **Intent Processing**: User intents are enhanced with contextual information
2. **Context Analysis**: Current state and business context are analyzed
3. **Complexity Assessment**: Appropriate query sophistication is determined
4. **Query Generation**: SQL queries are generated using selected strategies
5. **Feedback Integration**: Execution results are analyzed for pattern learning

## Performance

### Benchmarks
- **Generation Speed**: ~1-5ms per query (depending on complexity)
- **Memory Usage**: <50MB for typical workloads
- **Concurrent Performance**: Supports 100+ concurrent generations
- **Pattern Learning**: Adapts within 10-20 successful executions

### Optimization Features
- **Caching**: Frequently used patterns are cached for faster generation
- **Fallback Mechanisms**: Graceful degradation to simpler generation methods
- **Resource Management**: Automatic cleanup of generation history and learned patterns
- **Performance Monitoring**: Built-in metrics collection and analysis

## Monitoring and Logging

### Generation Monitoring
The system provides comprehensive monitoring through `monitoring.py`:

```python
# Enable detailed logging
import logging
logging.getLogger('dynamic_sql_generation').setLevel(logging.INFO)

# Monitor generation decisions
generator.enable_decision_logging()

# Access performance metrics
metrics = generator.get_performance_metrics()
```

### Log Files
- `logs/dynamic_sql_generation.log`: General system logs
- `logs/generation_decisions.jsonl`: Detailed generation decision logs
- `logs/generation_performance.log`: Performance metrics and timing data

## Troubleshooting

### Common Issues

#### Generation Failures
- **Symptom**: Queries fail to generate or return empty results
- **Solution**: Check database state synchronization and context validity
- **Debug**: Enable detailed logging and review generation decision logs

#### Performance Issues
- **Symptom**: Slow query generation or high memory usage
- **Solution**: Adjust complexity thresholds and enable pattern caching
- **Debug**: Monitor performance metrics and generation timing

#### Vietnamese Pattern Issues
- **Symptom**: Generated queries don't reflect Vietnamese business patterns
- **Solution**: Verify Vietnamese business configuration and cultural constraints
- **Debug**: Check cultural context application in generation logs

### Debug Mode
```python
# Enable debug mode for detailed logging
generator = DynamicSQLGenerator(debug=True, seed=42)

# Access internal state for debugging
debug_info = generator.get_debug_info()
print(f"Current patterns: {debug_info['learned_patterns']}")
print(f"Generation history: {debug_info['recent_history']}")
```

## Contributing

### Development Setup
1. Clone the repository
2. Install development dependencies: `pip install -r requirements-dev.txt`
3. Run tests: `python -m pytest -v`
4. Follow code style guidelines (PEP 8)

### Adding New Features
1. Create feature branch
2. Implement feature with comprehensive tests
3. Update documentation
4. Submit pull request with test coverage report

### Testing Guidelines
- Write both unit tests and property-based tests
- Ensure >90% code coverage
- Test Vietnamese business pattern integration
- Validate performance under load

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions, issues, or contributions:
- Create an issue in the project repository
- Review existing documentation and tests
- Check troubleshooting section for common problems

---

**Note**: This system is designed specifically for Vietnamese business environments and includes cultural and regulatory considerations specific to Vietnam. Adaptation for other regions may require configuration updates and cultural pattern modifications.