# 🚀 Dual LLM Provider System Setup Guide

## Overview
The UBA Platform now supports **both Ollama (local LLM) and Third-party AI APIs** with intelligent fallback and provider selection. This gives you the flexibility to use local models for privacy/cost and external APIs for enhanced capabilities.

## 🔧 **System Architecture**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Ollama        │    │   OpenAI        │    │   Anthropic     │
│   (Local)       │    │   (Cloud)       │    │   (Cloud)       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────────────┐
                    │    LLM Manager          │
                    │  (Provider Selection)   │
                    │  (Fallback Logic)       │
                    └─────────────────────────┘
                                 │
                    ┌─────────────────────────┐
                    │   Analysis Engine       │
                    │  (Query & Session)      │
                    └─────────────────────────┘
```

## 📋 **Prerequisites**

### 1. **Ollama Setup**
```bash
# Install Ollama
curl -fsSL https://ollama.ai/install.sh | sh

# Start service
ollama serve

# Pull models
ollama pull seneca:latest
ollama pull llama2:latest
```

### 2. **Third-Party API Setup**
```bash
# Install dependencies
pip install requests openai anthropic

# Set environment variables (optional)
export OPENAI_API_KEY="sk-..."
export ANTHROPIC_API_KEY="sk-ant-..."
```

### 3. **Python Dependencies**
```bash
pip install ollama pandas python-dotenv requests
```

## ⚙️ **Configuration**

### 1. **Engine Configuration (`engine_config.json`)**
```json
{
  "llm_config": {
    "enable_ollama": true,
    "ollama_host": "http://localhost:11434",
    "ollama_model": "seneca:latest",
    "ollama_timeout": 3600,
    
    "enable_openai": false,
    "openai_api_key": "sk-...",
    "openai_model": "gpt-3.5-turbo",
    "openai_base_url": "https://api.openai.com/v1",
    
    "enable_anthropic": false,
    "anthropic_api_key": "sk-ant-...",
    "anthropic_model": "claude-3-sonnet-20240229",
    
    "provider_priority": ["ollama", "openai", "anthropic"],
    "enable_fallback": true,
    "max_fallback_attempts": 3
  }
}
```

### 2. **Configuration via Web UI**
- Navigate to **Configuration** page
- Configure each provider section
- Set provider priority and fallback options
- Save configuration

## 🎯 **Provider Configuration Options**

### **Ollama Provider**
- ✅ **Enable/Disable**: Toggle Ollama usage
- 🌐 **Host**: Ollama server address (default: localhost:11434)
- 🤖 **Model**: Local model name (e.g., seneca:latest)
- ⏱️ **Timeout**: Request timeout in seconds
- 🔄 **Max Retries**: Number of retry attempts
- 🌡️ **Temperature**: Model creativity (0.0-2.0)
- 📝 **Max Tokens**: Maximum response length

### **OpenAI Provider**
- ✅ **Enable/Disable**: Toggle OpenAI usage
- 🔑 **API Key**: Your OpenAI API key
- 🤖 **Model**: OpenAI model (e.g., gpt-3.5-turbo, gpt-4)
- 🌐 **Base URL**: API endpoint (supports custom endpoints)

### **Anthropic Provider**
- ✅ **Enable/Disable**: Toggle Anthropic usage
- 🔑 **API Key**: Your Anthropic API key
- 🤖 **Model**: Claude model (e.g., claude-3-sonnet-20240229)

### **Fallback & Priority**
- 🔄 **Enable Fallback**: Automatic provider switching
- 📊 **Max Fallback Attempts**: Maximum fallback tries
- 🎯 **Provider Priority**: Order of provider preference

## 🚀 **Usage Examples**

### **1. Basic Query Analysis**
```python
from engine.llm_analyzer_dual import analyze_query_with_llm

result = analyze_query_with_llm(
    anomaly_row=anomaly_data,
    anomaly_type_from_system="suspicious",
    llm_config=llm_config,
    rules_config=rules_config
)

# Result includes provider information
print(f"Providers used: {result['providers_used']}")
print(f"Analysis timestamp: {result['analysis_timestamp']}")
```

### **2. Session Analysis**
```python
from engine.llm_analyzer_dual import analyze_session_with_llm

result = analyze_session_with_llm(
    session_anomaly_row=session_data,
    anomaly_type_from_system="multi_table",
    llm_config=llm_config,
    rules_config=rules_config
)
```

### **3. Provider Management**
```python
from engine.llm_analyzer_dual import LLMManager

# Initialize manager
config = {"llm_config": llm_config}
manager = LLMManager(config)

# Check available providers
available = manager.get_available_providers()
print(f"Available: {[p.get_name() for p in available]}")

# Analyze with fallback
result = manager.analyze_with_fallback(prompt, model="seneca:latest")
```

## 🔄 **Fallback Logic**

### **Automatic Fallback**
1. **Primary Provider**: Attempts analysis with first priority provider
2. **Fallback Chain**: If primary fails, tries next provider in priority list
3. **Error Handling**: Logs failures and continues to next provider
4. **Result Return**: Returns first successful analysis

### **Fallback Scenarios**
```
Ollama (Primary) → Fails → OpenAI (Fallback 1) → Success ✅
Ollama (Primary) → Fails → OpenAI (Fallback 1) → Fails → Anthropic (Fallback 2) → Success ✅
All Providers → Fail → RuntimeError ❌
```

### **Provider Priority Examples**
- **Privacy First**: `["ollama", "anthropic", "openai"]`
- **Performance First**: `["openai", "anthropic", "ollama"]`
- **Cost First**: `["ollama", "openai", "anthropic"]`
- **Hybrid**: `["openai", "ollama", "anthropic"]`

## 📊 **Performance & Cost Optimization**

### **Provider Selection Strategy**
- **🔒 Privacy Critical**: Use Ollama only
- **⚡ Speed Critical**: Use OpenAI/Anthropic first
- **💰 Cost Critical**: Use Ollama with API fallback
- **🔄 Reliability**: Use multiple providers with fallback

### **Model Selection Tips**
- **Ollama**: `seneca:latest` (fast), `llama2:latest` (quality)
- **OpenAI**: `gpt-3.5-turbo` (cost), `gpt-4` (quality)
- **Anthropic**: `claude-3-haiku` (fast), `claude-3-sonnet` (balanced)

### **Timeout Configuration**
- **Ollama**: 3600s (local processing)
- **OpenAI**: 60s (API calls)
- **Anthropic**: 60s (API calls)

## 🔐 **Security Considerations**

### **API Key Management**
- Store API keys in environment variables
- Use secure configuration files
- Rotate keys regularly
- Monitor API usage

### **Network Security**
- Ollama: Localhost only (default)
- External APIs: Use HTTPS
- Consider VPN for distributed deployments
- Monitor API access logs

### **Data Privacy**
- Ollama: 100% local, no data leaves your system
- External APIs: Data sent to third-party servers
- Review privacy policies
- Use local models for sensitive data

## 🧪 **Testing & Validation**

### **1. Test Individual Providers**
```bash
# Test Ollama
python -c "from engine.llm_analyzer_dual import OllamaProvider; p = OllamaProvider(); print(p.is_available())"

# Test OpenAI
python -c "from engine.llm_analyzer_dual import OpenAIProvider; p = OpenAIProvider('your-key'); print(p.is_available())"
```

### **2. Test Fallback System**
```bash
# Test with failing primary provider
python test_fallback.py
```

### **3. Performance Testing**
```bash
# Benchmark response times
python benchmark_providers.py
```

## 🔍 **Troubleshooting**

### **Common Issues**

#### **1. Ollama Connection Failed**
```
❌ Failed to connect to Ollama server
```
**Solutions:**
- Check if `ollama serve` is running
- Verify host/port configuration
- Check firewall settings
- Test with `curl http://localhost:11434/api/tags`

#### **2. API Key Invalid**
```
❌ OpenAI API error: 401 - Invalid API key
```
**Solutions:**
- Verify API key is correct
- Check API key permissions
- Ensure account has credits
- Test with simple API call

#### **3. Model Not Available**
```
❌ Model llama2:latest not found
```
**Solutions:**
- Pull the model: `ollama pull llama2:latest`
- Check available models: `ollama list`
- Update configuration to use available model

#### **4. Fallback Not Working**
```
❌ All LLM providers failed
```
**Solutions:**
- Check provider availability
- Verify configuration settings
- Review error logs
- Test individual providers

### **Debug Mode**
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# This will show detailed provider initialization and fallback attempts
```

## 📈 **Monitoring & Metrics**

### **Key Metrics to Track**
- Provider success/failure rates
- Response times per provider
- Fallback frequency
- Cost per analysis
- Error types and frequencies

### **Log Analysis**
```bash
# Monitor LLM analysis logs
tail -f logs/llm_analysis.log

# Search for provider usage
grep "Provider used" logs/llm_analysis.log

# Check fallback events
grep "Fallback" logs/llm_analysis.log
```

## 🚀 **Advanced Configuration**

### **Custom Provider Priority**
```json
{
  "provider_priority": ["ollama", "openai", "anthropic"],
  "custom_fallback_rules": {
    "security_level": "high",
    "use_local_only": true,
    "api_fallback_threshold": 0.8
  }
}
```

### **Load Balancing**
- Run multiple Ollama instances
- Use reverse proxy for distribution
- Implement health checks
- Monitor resource usage

### **Caching Strategy**
- Cache analysis results
- Implement TTL-based invalidation
- Use similarity-based caching
- Monitor cache hit rates

## 📚 **Additional Resources**

- [Ollama Documentation](https://ollama.ai/docs)
- [OpenAI API Reference](https://platform.openai.com/docs/api-reference)
- [Anthropic API Docs](https://docs.anthropic.com/)
- [UBA Platform Documentation](./README.md)

## 🆘 **Support & Community**

### **Getting Help**
1. Check this troubleshooting guide
2. Review error logs
3. Test with minimal configuration
4. Check provider status pages
5. Community forums and discussions

### **Contributing**
- Report bugs and issues
- Suggest improvements
- Share configurations
- Contribute to documentation

---

**🎉 Happy Analyzing with Dual LLM Providers! 🎉**

Your UBA Platform now has the flexibility to use the best of both worlds: local privacy with Ollama and cloud capabilities with external APIs!
