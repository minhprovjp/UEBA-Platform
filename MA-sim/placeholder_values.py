# ALL DUMMY_VALUES placeholders from pre_generate_ai_queries.py
import random
from faker import Faker

# Initialize Vietnamese Faker for realistic data
fake_vn = Faker('vi_VN')

PLACEHOLDER_VALUES = {
    "{customer_id}": lambda: str(random.randint(1, 500)),
    "{product_id}": lambda: str(random.randint(1, 200)),
    "{order_id}": lambda: str(random.randint(1000, 9999)),
    "{lead_id}": lambda: str(random.randint(1, 1000)),
    "{ticket_id}": lambda: str(random.randint(1, 500)),
    "{employee_id}": lambda: str(random.randint(1, 100)),
    "{start_date}": lambda: "2025-01-01",
    "{end_date}": lambda: "2025-12-31",
    "{status}": lambda: random.choice(["active", "pending", "completed", "cancelled"]),
    "{order_status}": lambda: random.choice(["pending", "shipped", "delivered", "cancelled"]),
    "{min_stock_level}": lambda: "10",
    "{min_stock_threshold}": lambda: "5",
    "{search_term}": lambda: "test",
    "{lead_status}": lambda: random.choice(["new", "qualified", "lost", "won"]),
    "{ticket_status}": lambda: random.choice(["open", "in_progress", "resolved", "closed"]),
    "{customer_code}": lambda: f"C{random.randint(100, 999)}",
    "{email}": lambda: fake_vn.email(),  # Realistic Vietnamese email
    "{phone}": lambda: fake_vn.phone_number()[:15],  # Realistic Vietnamese phone
    "{is_active}": lambda: str(random.choice([0, 1])),
    "{report_type}": lambda: random.choice(["daily", "weekly", "monthly"]),
    "{user_id}": lambda: str(random.randint(1, 100)),
    "{frequency}": lambda: random.choice(["daily", "weekly", "monthly"]),
    "{quantity}": lambda: str(random.randint(1, 100)),
    "{amount}": lambda: str(random.randint(10, 10000)),
    "{number}": lambda: str(random.randint(1, 100)),
    "{bonus}": lambda: str(random.randint(0, 10000)),
    "{rating}": lambda: str(random.randint(1, 5)),
    "{stock_quantity}": lambda: str(random.randint(1, 1000)),
    "{limit}": lambda: str(random.randint(10, 100)),
    "{campaign_name}": lambda: f"Campaign {fake_vn.word().title()}",  # Vietnamese campaign names
    "{invoice_id}": lambda: str(random.randint(10000, 99999)),
    "{location_id}": lambda: str(random.randint(1, 20)),
    "{campaign_id}": lambda: str(random.randint(1, 100)),
    "{username}": lambda: fake_vn.user_name(),  # Realistic Vietnamese username
    "{dept_id}": lambda: str(random.randint(1, 10)),
}


def fill_query_template(query_template: str) -> str:
    """
    Replace all placeholders in a query template with random realistic values.
    
    Args:
        query_template: SQL query template with {placeholder} markers
        
    Returns:
        Hydrated SQL query with actual values
    """
    result = query_template
    
    # Replace all known placeholders
    for placeholder, value_func in PLACEHOLDER_VALUES.items():
        if placeholder in result:
            result = result.replace(placeholder, value_func())
    
    # Handle any remaining unknown placeholders with generic values
    import re
    remaining_placeholders = re.findall(r'\{([^}]+)\}', result)
    for placeholder_name in remaining_placeholders:
        placeholder = f"{{{placeholder_name}}}"
        # Generate a sensible default based on placeholder name
        if 'id' in placeholder_name.lower():
            value = str(random.randint(1, 1000))
        elif 'date' in placeholder_name.lower():
            value = "2025-01-01"
        elif 'email' in placeholder_name.lower():
            value = fake_vn.email()  # Use Faker for emails
        elif 'phone' in placeholder_name.lower():
            value = fake_vn.phone_number()[:15]  # Use Faker for phones
        elif 'name' in placeholder_name.lower():
            value = fake_vn.name()  # Use Faker for names
        else:
            value = "default_value"
        
        result = result.replace(placeholder, value)
    
    return result
