from pathlib import Path
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import DoubleQuotedScalarString

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)

# Prevent ruamel from wrapping quoted strings onto multiple lines
yaml.width = 4096  # effectively disables automatic line splitting

# Helper to recursively double-quote all 'description' values
def ensure_quoted_descriptions(obj):
    """
    Recursively traverse *obj* and convert every string value found under a
    'description' key into a DoubleQuotedScalarString so ruamel.yaml writes it
    with explicit quotes.
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
                
                # Build test list with proper dbt YAML structure
                for t in tests:
                    test_def = t["definition"].copy()
                    # Add severity to the test definition
                    for test_name, test_config in test_def.items():
                        if isinstance(test_config, dict):
                            test_config["severity"] = t["severity"]
                            # Ensure description is quoted if present
                            if "description" in test_config:
                                test_config["description"] = DoubleQuotedScalarString(test_config["description"])
                        else:
                            # If test_config is not a dict, make it one
                            test_def[test_name] = {"severity": t["severity"]}
                    existing_model["tests"].append(test_def)
                
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
                        # Ensure description is quoted if present
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
        
        # Ensure every description string is double‑quoted
        ensure_quoted_descriptions(existing_schema)

        # Write back to schema file
        with open(schema_path, 'w') as f:
            yaml.dump(existing_schema, f)
        print(f"📝 updated {schema_path}")