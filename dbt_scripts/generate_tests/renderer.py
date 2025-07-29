from pathlib import Path
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import DoubleQuotedScalarString

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)

# Prevent ruamel from wrapping quoted strings onto multiple lines
yaml.width = 4096  # effectively disables automatic line splitting

# Configure YAML to not quote certain fields
yaml.default_flow_style = False

# Helper to recursively double-quote only 'description' values
def ensure_quoted_descriptions(obj):
    """
    Recursively traverse *obj* and convert every string value found under a
    'description' key into a DoubleQuotedScalarString so ruamel.yaml writes it
    with explicit quotes. Other fields like 'field', 'severity', etc. remain unquoted.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "description" and isinstance(v, str):
                obj[k] = DoubleQuotedScalarString(v)
            else:
                ensure_quoted_descriptions(v)
    elif isinstance(obj, list):
        for item in obj:
            ensure_quoted_descriptions(item)

def normalize_test_definition(test_def):
    """
    Normalize a test definition for comparison by sorting keys and handling nested structures.
    This helps in detecting duplicate tests even if they have different key orders.
    """
    if isinstance(test_def, dict):
        # Sort the keys to ensure consistent comparison
        normalized = {}
        for key in sorted(test_def.keys()):
            value = test_def[key]
            if isinstance(value, dict):
                normalized[key] = normalize_test_definition(value)
            else:
                normalized[key] = value
        return normalized
    return test_def

def tests_are_equivalent(test1, test2):
    """
    Compare two test definitions to determine if they are equivalent.
    Ignores severity and description differences for comparison.
    """
    def clean_for_comparison(test_def):
        """Remove severity and description for comparison purposes"""
        if isinstance(test_def, dict):
            cleaned = {}
            for key, value in test_def.items():
                if key not in ['severity', 'description']:
                    if isinstance(value, dict):
                        cleaned[key] = clean_for_comparison(value)
                    else:
                        cleaned[key] = value
            return cleaned
        return test_def
    
    # Normalize both tests
    norm1 = normalize_test_definition(clean_for_comparison(test1))
    norm2 = normalize_test_definition(clean_for_comparison(test2))
    
    return norm1 == norm2

class YamlRenderer:
    def write_tests(self, model, tests):
        if not tests:
            return

        # Convert compiled path back to source path
        compiled_path = model["path"]
        # Extract project name from path like: target/compiled/artemis_dbt/models/projects/aave/core/model.sql
        parts = compiled_path.parts
        project_idx = parts.index("projects") + 1
        project_name = parts[project_idx]
        
        # Find dbt project root by going up from compiled path
        dbt_root = compiled_path
        while dbt_root.name != "target" and len(dbt_root.parts) > 1:
            dbt_root = dbt_root.parent
        dbt_root = dbt_root.parent  # Go up one more to get to dbt project root
        
        # Build schema file path: {dbt_root}/models/projects/{project}/core/__{project}__schema.yml
        schema_path = dbt_root / "models" / "projects" / project_name / "core" / f"__{project_name}__schema.yml"
        
        # Load existing schema file
        if schema_path.exists():
            with open(schema_path, 'r') as f:
                existing_schema = yaml.load(f)
        else:
            existing_schema = {"version": 2, "models": []}
        
        # Find the model in the schema file
        model_found = False
        for existing_model in existing_schema["models"]:
            if existing_model["name"] == model["name"]:
                # Add tests to existing model
                if "tests" not in existing_model:
                    existing_model["tests"] = []
                
                # Get existing test definitions for comparison
                existing_tests = existing_model["tests"]
                
                # Build test list with proper dbt YAML structure, avoiding duplicates
                new_tests_added = 0
                for t in tests:
                    test_def = t["definition"].copy()
                    # Add severity to the test definition
                    for test_name, test_config in test_def.items():
                        if isinstance(test_config, dict):
                            test_config["severity"] = t["severity"]
                            # Ensure only description is quoted, other fields remain unquoted
                            if "description" in test_config:
                                test_config["description"] = DoubleQuotedScalarString(test_config["description"])
                        else:
                            # If test_config is not a dict, make it one
                            test_def[test_name] = {"severity": t["severity"]}
                    
                    # Check if this test already exists
                    is_duplicate = False
                    for existing_test in existing_tests:
                        if tests_are_equivalent(test_def, existing_test):
                            is_duplicate = True
                            break
                    
                    if not is_duplicate:
                        existing_model["tests"].append(test_def)
                        new_tests_added += 1
                
                if new_tests_added > 0:
                    print(f"📝 added {new_tests_added} new tests to {model['name']}")
                else:
                    print(f"✅ no new tests needed for {model['name']} (all tests already exist)")
                
                model_found = True
                break
        
        # If model not found, add it
        if not model_found:
            # Build test list with proper dbt YAML structure
            test_list = []
            for t in tests:
                test_def = t["definition"].copy()
                # Add severity to the test definition
                for test_name, test_config in test_def.items():
                    if isinstance(test_config, dict):
                        test_config["severity"] = t["severity"]
                        # Ensure only description is quoted, other fields remain unquoted
                        if "description" in test_config:
                            test_config["description"] = DoubleQuotedScalarString(test_config["description"])
                    else:
                        # If test_config is not a dict, make it one
                        test_def[test_name] = {"severity": t["severity"]}
                test_list.append(test_def)
            
            existing_schema["models"].append({
                "name": model["name"],
                "tests": test_list,
            })
            print(f"📝 created new model {model['name']} with {len(test_list)} tests")
        
        # Ensure every description string is double‑quoted
        ensure_quoted_descriptions(existing_schema)

        # Write back to schema file
        with open(schema_path, 'w') as f:
            yaml.dump(existing_schema, f)
        print(f"📝 updated {schema_path}")