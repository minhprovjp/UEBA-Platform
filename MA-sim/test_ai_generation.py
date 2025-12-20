
import logging
from dynamic_sql_generation.generator import DynamicSQLGenerator
from dynamic_sql_generation.models import QueryContext, UserContext, BusinessContext, TemporalContext, ExpertiseLevel, WorkflowType
from dynamic_sql_generation.complexity_engine import ComplexityLevel

# Setup logging
logging.basicConfig(level=logging.INFO)

def test_ai_generation():
    print("🚀 TESTING AI QUERY GENERATION")
    print("=" * 50)
    
    generator = DynamicSQLGenerator(seed=42)
    # Mock fallback to prevent context crashes
    generator._generate_context_aware_query = lambda i, c, r: "SELECT * FROM fallback_table;"
    
    # Mock Intent
    intent = {
        'action': 'SEARCH_CUSTOMER',
        'target_database': 'sales_db',
        'type': 'analytics'
    }
    
    # Mock Context
    user_context = UserContext(
        username="test_user",
        role="MARKETING",
        department="Marketing",
        expertise_level=ExpertiseLevel.INTERMEDIATE,
        session_history=[],
        work_intensity=1.0,
        stress_level=0.5
    )
    
    business_context = WorkflowType.MARKETING_CAMPAIGN
    
    # Minimal mock for QueryContext since we only need basic fields
    # We can't easily instantiate the full QueryContext without the engine, 
    # but let's try to pass None where possible or simple mocks
    
    # Actually, simpler to just access the method if we can avoid complex context validation
    # looking at _generate_ai_query implementation, it checks context.business_context.current_workflow
    
    # Let's create a minimal class to mock context if the real one is too hard to instantiate
    class MockContext:
        class MockBusinessContext:
            class MockWorkflow:
                value = 'marketing_campaign'
            current_workflow = MockWorkflow()
        business_context = MockBusinessContext()
    
    context = MockContext()
    reasoning = []
    
    print("\nAttempting to generate query via Ollama...")
    try:
        # Direct call to the new method
        query = generator._generate_ai_query(intent, context, reasoning)
        
        print("\n📊 Result:")
        print(f"Query: {query}")
        print("\n📋 Reasoning Log:")
        for r in reasoning:
             print(f"• {r}")
             
        if "AI generation successful" in reasoning:
            print("\n✅ AI Generation WORKS! (Ollama responded)")
        elif "AI API error" in str(reasoning):
            print("\n❌ AI Generation FAILED: API Error (Is Ollama running? Is model 'seneca' created?)")
        else:
             print("\n⚠️ AI Generation FAILED: Fallback used (See reasoning)")
             
    except Exception as e:
        print(f"\n❌ Error running test: {e}")

if __name__ == "__main__":
    test_ai_generation()
