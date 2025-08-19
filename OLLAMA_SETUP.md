# 🚀 Ollama Integration Setup Guide for UBA Platform

## Overview
The UBA Platform has been upgraded to work seamlessly with Ollama, allowing you to run local LLM models for anomaly analysis without external API dependencies.

## 📋 Prerequisites

### 1. Install Ollama
```bash
# macOS/Linux
curl -fsSL https://ollama.ai/install.sh | sh

# Windows
# Download from https://ollama.ai/download
```

### 2. Install Python Dependencies
```bash
pip install ollama pandas python-dotenv
```

## 🔧 Initial Setup

### 1. Start Ollama Service
```bash
ollama serve
```

### 2. Pull a Model
```bash
# Recommended models for analysis:
ollama pull llama2:latest      # Good balance of speed and quality
ollama pull llama2:7b          # Faster, lighter version
ollama pull codellama:latest   # Specialized for code analysis
ollama pull mistral:latest     # High quality, reasonable speed
```

### 3. Verify Installation
```bash
ollama list
```

## ⚙️ Configuration

### 1. Engine Configuration
The platform uses `engine_config.json` for Ollama settings:

```json
{
  "llm_config": {
    "ollama_host": "http://localhost:11434",
    "ollama_model": "llama2:latest",
    "ollama_timeout": 3600,
    "max_retries": 3,
    "retry_delay": 2,
    "enable_connection_testing": true,
    "fallback_model": "llama2:7b",
    "temperature": 0.7,
    "max_tokens": 4096
  }
}
```

### 2. Configuration via UI
- Navigate to **Configuration** page in the web interface
- Update Ollama settings as needed
- Click **Save Configuration**

## 🧪 Testing the Integration

### 1. Run Test Script
```bash
python test_ollama.py
```

### 2. Expected Output
```
✅ Successfully imported LLM analyzer modules
🔧 Testing Ollama client connection...
✅ Ollama client created successfully
✅ Connection test successful
🤖 Testing LLM analysis...
✅ LLM analysis completed successfully!
🎉 All tests completed successfully!
```

## 🔍 Troubleshooting

### Common Issues

#### 1. Connection Refused
```
❌ Failed to create Ollama client: Connection refused
```
**Solution:**
- Ensure Ollama service is running: `ollama serve`
- Check if port 11434 is accessible
- Verify firewall settings

#### 2. Model Not Found
```
❌ Model llama2:latest not found
```
**Solution:**
- Pull the model: `ollama pull llama2:latest`
- Check available models: `ollama list`
- Update configuration to use available model

#### 3. Timeout Issues
```
❌ LLM analysis failed: timeout
```
**Solution:**
- Increase timeout in configuration
- Use smaller/faster model
- Check system resources

#### 4. JSON Parsing Errors
```
❌ AI response is not a valid JSON
```
**Solution:**
- Use more capable model (llama2:latest, mistral:latest)
- Check model's JSON generation capabilities
- Verify prompt formatting

### Performance Optimization

#### 1. Model Selection
- **Fast Analysis**: `llama2:7b`, `mistral:7b`
- **Quality Analysis**: `llama2:latest`, `mistral:latest`
- **Code Analysis**: `codellama:latest`

#### 2. Resource Management
- Monitor GPU/CPU usage
- Adjust batch sizes if needed
- Use appropriate timeout values

## 📊 Usage Examples

### 1. Basic Query Analysis
```python
from engine.llm_analyzer import analyze_query_with_llm

result = analyze_query_with_llm(
    anomaly_row=anomaly_data,
    anomaly_type_from_system="suspicious",
    llm_config=llm_config,
    rules_config=rules_config
)
```

### 2. Session Analysis
```python
from engine.llm_analyzer import analyze_session_with_llm

result = analyze_session_with_llm(
    session_anomaly_row=session_data,
    anomaly_type_from_system="multi_table",
    llm_config=llm_config,
    rules_config=rules_config
)
```

## 🔐 Security Considerations

### 1. Network Security
- Ollama runs on localhost by default
- For remote access, use proper authentication
- Consider VPN for distributed deployments

### 2. Model Security
- Use trusted models from Ollama library
- Verify model checksums
- Keep Ollama updated

### 3. Data Privacy
- All analysis happens locally
- No data sent to external services
- Logs contain analysis results (ensure proper access controls)

## 📈 Monitoring and Logs

### 1. Log Files
- LLM analysis logs: `logs/llm_analysis.log`
- Engine logs: `logs/engine.log`
- Application logs: `logs/app.log`

### 2. Key Metrics
- Response times
- Success/failure rates
- Model usage patterns
- Resource utilization

### 3. Health Checks
```bash
# Check Ollama status
curl http://localhost:11434/api/tags

# Check model availability
ollama list

# Test specific model
ollama run llama2:latest "Hello, world!"
```

## 🚀 Advanced Configuration

### 1. Custom Models
```bash
# Create custom model with specific parameters
ollama create my-analyzer -f Modelfile
```

### 2. Load Balancing
- Run multiple Ollama instances
- Use reverse proxy for distribution
- Implement failover mechanisms

### 3. Caching
- Enable response caching
- Cache model outputs for similar queries
- Implement TTL-based invalidation

## 📚 Additional Resources

- [Ollama Documentation](https://ollama.ai/docs)
- [Model Library](https://ollama.ai/library)
- [Performance Tips](https://ollama.ai/docs/performance)
- [Community Support](https://github.com/ollama/ollama/discussions)

## 🆘 Support

If you encounter issues:

1. Check the troubleshooting section above
2. Review logs for detailed error messages
3. Test with simple Ollama commands
4. Verify system requirements
5. Check Ollama GitHub issues

---

**Happy Analyzing! 🎉**
