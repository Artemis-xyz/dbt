import os
from datetime import datetime, timedelta

def generate_unique_chain_date_test():
    """Generate a unique test for chain and date combination as YAML string"""
    return """      - unique:
          column_name: [chain, date]
          description: Ensures that each chain and date combination is unique
          config:
            severity: warn"""

def generate_recency_test(days_threshold=2):
    """Generate a recency test for date columns as YAML string"""
    return f"""      - dbt_utils.expression_is_true:
          expression: exists (select 1 from {{{{ this }}}} where date >= dateadd(day, -{days_threshold}, current_date()) limit 1)
          description: Ensures that the date column contains at least one record from the last {days_threshold} days"""

def generate_not_null_chain_test():
    """Generate a not null test for chain column as YAML string"""
    return """      - not_null:
          column_name: chain
          description: Ensures that the chain column is not null"""

def generate_not_null_date_test():
    """Generate a not null test for date column as YAML string"""
    return """      - not_null:
          column_name: date
          description: Ensures that the date column is not null"""

def generate_all_tests():
    """Generate all tests as a YAML string block"""
    tests = [
        generate_unique_chain_date_test(),
        generate_recency_test(),
        generate_not_null_chain_test(),
        generate_not_null_date_test()
    ]
    return "    tests:\n" + "\n".join(tests)

def generate_tests_for_schema(schema_path, project_name):
    """Legacy function for backward compatibility - now returns test string"""
    return generate_all_tests()

if __name__ == "__main__":
    # Test the test generation
    print("Generated tests:")
    print(generate_all_tests())
