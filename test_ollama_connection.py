#!/usr/bin/env python3
"""
Simple test script to verify Ollama connection and troubleshoot issues
"""

import requests
import json
import sys
import os

# Add the engine directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'engine'))

def test_ollama_direct():
    """Test Ollama connection directly using HTTP requests"""
    print("🔍 Testing Ollama connection directly...")
    
    # Test the endpoint you mentioned
    host = "http://192.168.2.239:11434"
    
    try:
        # Test basic endpoint
        print(f"📡 Testing endpoint: {host}")
        response = requests.get(host, timeout=10)
        print(f"✅ Basic endpoint response: {response.status_code}")
        if response.text:
            print(f"📝 Response text: {response.text[:200]}...")
    except Exception as e:
        print(f"❌ Basic endpoint failed: {e}")
    
    try:
        # Test tags endpoint
        print(f"📡 Testing tags endpoint: {host}/api/tags")
        response = requests.get(f"{host}/api/tags", timeout=10)
        print(f"✅ Tags endpoint response: {response.status_code}")
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"📊 Tags response: {json.dumps(data, indent=2)}")
            except:
                print(f"📝 Raw response: {response.text[:200]}...")
        else:
            print(f"📝 Error response: {response.text[:200]}...")
    except Exception as e:
        print(f"❌ Tags endpoint failed: {e}")
    
    try:
        # Test models endpoint
        print(f"📡 Testing models endpoint: {host}/api/models")
        response = requests.get(f"{host}/api/models", timeout=10)
        print(f"✅ Models endpoint response: {response.status_code}")
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"📊 Models response: {json.dumps(data, indent=2)}")
            except:
                print(f"📝 Raw response: {response.text[:200]}...")
        else:
            print(f"📝 Error response: {response.text[:200]}...")
    except Exception as e:
        print(f"❌ Models endpoint failed: {e}")

def test_ollama_python_client():
    """Test Ollama using the Python client"""
    print("\n🐍 Testing Ollama Python client...")
    
    try:
        import ollama
        
        # Create client
        client = ollama.Client(host="http://192.168.2.239:11434", timeout=30)
        print("✅ Ollama client created successfully")
        
        # Test list models
        try:
            print("📡 Testing list() method...")
            models = client.list()
            print(f"✅ List method successful")
            print(f"📊 Response type: {type(models)}")
            print(f"📊 Response: {models}")
            
            # Try to extract model names safely
            if isinstance(models, dict) and 'models' in models:
                model_list = models['models']
                print(f"📊 Models list type: {type(model_list)}")
                print(f"📊 Models list: {model_list}")
                
                if isinstance(model_list, list):
                    names = []
                    for m in model_list:
                        if isinstance(m, dict) and 'name' in m:
                            names.append(m['name'])
                        elif hasattr(m, 'name'):
                            names.append(m.name)
                        else:
                            names.append(str(m))
                    print(f"🤖 Model names: {names}")
                else:
                    print(f"⚠️ Models is not a list: {type(model_list)}")
            else:
                print(f"⚠️ Unexpected response structure: {type(models)}")
                
        except Exception as e:
            print(f"❌ List method failed: {e}")
            print(f"📊 Error type: {type(e)}")
            
    except ImportError:
        print("❌ Ollama Python package not installed")
        print("💡 Install with: pip install ollama")
    except Exception as e:
        print(f"❌ Python client test failed: {e}")

def test_engine_import():
    """Test importing the engine modules"""
    print("\n⚙️ Testing engine module imports...")
    
    try:
        from engine.llm_analyzer import OllamaProvider
        print("✅ OllamaProvider imported successfully")
        
        # Test creating provider
        try:
            provider = OllamaProvider(host="http://192.168.2.239:11434")
            print("✅ OllamaProvider created successfully")
            
            # Test availability
            try:
                is_available = provider.is_available()
                print(f"✅ Availability check: {is_available}")
                
                if is_available:
                    print("🎉 Ollama is working correctly!")
                else:
                    print("⚠️ Ollama availability check failed")
                    
            except Exception as e:
                print(f"❌ Availability check failed: {e}")
                
        except Exception as e:
            print(f"❌ Provider creation failed: {e}")
            
    except Exception as e:
        print(f"❌ Engine import failed: {e}")

if __name__ == "__main__":
    print("🚀 Ollama Connection Test Script")
    print("=" * 50)
    
    test_ollama_direct()
    test_ollama_python_client()
    test_engine_import()
    
    print("\n" + "=" * 50)
    print("🏁 Test completed!")
    print("\n💡 If you see errors, check:")
    print("   1. Ollama service is running: ollama serve")
    print("   2. Network connectivity to 192.168.2.239:11434")
    print("   3. Firewall settings")
    print("   4. Ollama version compatibility")
