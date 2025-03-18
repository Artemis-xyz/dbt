import yaml
import re
import os

def load_global_schema(global_schema_path):
    """ Load and flatten the global schema into a set of metric names. """
    with open(global_schema_path, "r") as file:
        schema = yaml.safe_load(file)
    
    metric_set = set()
    
    # Iterate through schema categories
    for category, metrics in schema["column_definitions"].items():
        for metric in metrics:
            metric_set.add(metric["name"])  # Extract metric names

    return metric_set

def extract_sql_columns(sql_file_path):
    """ Extract column names from a dbt SQL file by parsing the last SELECT clause. """
    with open(sql_file_path, "r") as file:
        sql = file.read()
    
    # Find all SELECT statements
    select_matches = re.findall(r"select\s+(.*?)\s+from", sql, re.DOTALL | re.IGNORECASE)
    if not select_matches:
        raise ValueError("No SELECT statement found in SQL file.")
    
    # Take the last SELECT statement
    select_clause = select_matches[-1]

    print("Processing SELECT clause:", select_clause)

    # Extract column names (handling various alias patterns)
    column_names = set()
    for line in select_clause.split("\n"):
        line = line.strip().rstrip(",")  # Remove trailing commas
        if not line or line.startswith("--"):  # Skip empty lines and comments
            continue
            
        # Handle different alias patterns
        # Pattern 1: column as alias
        match = re.search(r"(?:.*\s)?as\s+(\w+)(?:\s*,)?$", line, re.IGNORECASE)
        if match:
            column_names.add(match.group(1))
            continue
            
        # Pattern 2: simple column name
        if re.match(r"^\w+$", line):
            column_names.add(line)
            continue
            
        # Pattern 3: table.column as alias
        match = re.search(r"[\w.]+\.(\w+)(?:\s*,)?$", line)
        if match:
            column_names.add(match.group(1))
            continue
            
        # Pattern 4: complex expression with final alias
        match = re.search(r"\w+$", line)
        if match:
            column_names.add(match.group(0))

    return sorted(column_names)

def compare_columns(sql_columns, schema_columns):
    """ Compare extracted SQL columns against the global schema. """
    matching_columns = set(sql_columns) & set(schema_columns)
    missing_columns = set(sql_columns) - set(schema_columns)

    print("✅ Matching columns:", matching_columns)
    print("⚠️ Missing columns from schema:", missing_columns)

    return matching_columns, missing_columns

def load_existing_overrides(project_schema_path):
    """Load any existing column overrides from the project schema."""
    if not os.path.exists(project_schema_path):
        return {}
    
    with open(project_schema_path, 'r') as f:
        existing_schema = yaml.safe_load(f)
    
    overrides = {}
    # Extract existing column definitions that differ from global
    for column in existing_schema.get('models', [{}])[0].get('columns', []):
        if 'override' in column.get('tags', []):  # Check if column is marked as override
            overrides[column['name']] = column
    
    return overrides

def get_project_sql_files(project_name):
    """Get all SQL files in the project's core directory"""
    dbt_root = get_dbt_root()
    project_core_dir = os.path.join(dbt_root, 'models', 'projects', project_name, 'core')
    
    sql_files = []
    for file in os.listdir(project_core_dir):
        if file.endswith('.sql'):
            sql_files.append(os.path.join(project_core_dir, file))
    
    return sql_files

def generate_project_schema(project_name, global_schema_path, sql_files):
    """Generate project schema file using matched columns from all SQL files"""
    dbt_root = get_dbt_root()
    output_dir = os.path.join(dbt_root, 'models', 'projects', project_name, 'core')
    output_path = os.path.join(output_dir, f"__{project_name}__schema.yml")

    # Load any existing overrides
    existing_overrides = load_existing_overrides(output_path)

    # Read global schema to get column definitions
    with open(global_schema_path, 'r') as f:
        global_schema = yaml.safe_load(f)
    
    # Extract all column definitions from global schema
    column_defs = {}
    for metric_group in global_schema['column_definitions'].values():
        for column in metric_group:
            if isinstance(column, dict) and 'name' in column:
                column_defs[column['name']] = column

    # Create new schema structure
    project_schema = {
        'version': 2,
        'models': []
    }

    # Process each SQL file
    for sql_file in sql_files:
        model_name = os.path.basename(sql_file).replace('.sql', '')
        
        # Get columns from SQL file
        sql_columns = extract_sql_columns(sql_file)
        schema_columns = load_global_schema(global_schema_path)
        matching_columns, missing_columns = compare_columns(sql_columns, schema_columns)

        if matching_columns:  # Only add model if it has matching columns
            model_def = {
                'name': model_name,
                'description': f'This table stores metrics for the {project_name.upper()} protocol',
                'columns': []
            }

            # Add matching columns with their definitions
            for column_name in matching_columns:
                if column_name in existing_overrides:
                    # Use the override definition
                    model_def['columns'].append(existing_overrides[column_name])
                elif column_name in column_defs:
                    # Use the global definition
                    column_def = column_defs[column_name].copy()
                    model_def['columns'].append(column_def)

            project_schema['models'].append(model_def)

    # Write the new schema file
    with open(output_path, 'w') as f:
        yaml.dump(project_schema, f, sort_keys=False, default_flow_style=False)

    print(f"Generated schema file: {output_path}")
    if existing_overrides:
        print(f"Preserved {len(existing_overrides)} column overrides")

def get_dbt_root():
    """Find the dbt project root directory by looking for dbt_project.yml"""
    current_dir = os.getcwd()
    while current_dir != '/':
        if os.path.exists(os.path.join(current_dir, 'dbt_project.yml')):
            return current_dir
        current_dir = os.path.dirname(current_dir)
    raise Exception("Could not find dbt project root (no dbt_project.yml found)")

def get_project_paths(project_name):
    """Generate standard paths relative to dbt project root"""
    dbt_root = get_dbt_root()
    
    paths = {
        'global_schema': os.path.join(dbt_root, 'models', '__global__schema.yml'),
    }
    return paths

if __name__ == "__main__":
    project_name = input("Enter project name: ")
    
    # Get paths
    paths = get_project_paths(project_name)
    global_schema_path = paths['global_schema']

    # Get all SQL files in project
    sql_files = get_project_sql_files(project_name)
    if not sql_files:
        raise FileNotFoundError(f"No SQL files found in project {project_name}")

    # Generate combined schema file for all models
    generate_project_schema(project_name, global_schema_path, sql_files)

    
