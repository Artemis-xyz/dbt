import yaml
import re
import os
import subprocess
import requests
from generate_tests import generate_tests_for_schema

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

    def strip_sql_comments(sql):
        # Remove single-line comments (--) and multi-line comments (/* */)
        sql = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        return sql

    with open(sql_file_path, "r") as file:
        sql = file.read()

    cleaned_sql = strip_sql_comments(sql)

    # Find all SELECT statements
    select_matches = re.findall(r"select\s+(.*?)\s+from", cleaned_sql, re.DOTALL | re.IGNORECASE)
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
        match = re.search(r"(?:.*\s)?as\s+(\w+)(?:\s*,)?(?:\s*--.*)?$", line, re.IGNORECASE)
        if match:
            column_names.add(match.group(1))
            continue

        # Pattern 2: simple column name
        match = re.match(r"^\s*,?\s*(\w+)(?:\s*--.*)?$", line)
        if match:
            column_names.add(match.group(1))  # Extract the captured column name
            continue

        # Pattern 3: table.column as alias
        match = re.search(r"[\w.]+\.(\w+)(?:\s*,)?(?:\s*--.*)?$", line)
        if match:
            column_names.add(match.group(1))
            continue

        # Pattern 4: complex expression with final alias
        match = re.search(r"\w+(?:\s*--.*)?$", line)
        if match:
            column_names.add(match.group(0))

    return sorted(column_names)

def compare_columns(sql_columns, schema_columns, existing_overrides):
    """ Compare extracted SQL columns against the global schema. """
    matching_columns = set(sql_columns) & (set(schema_columns) | set(existing_overrides))
    missing_columns = set(sql_columns) - (set(schema_columns) | set(existing_overrides))

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
    # Extract existing column definitions that are marked as overrides
    column_defs = existing_schema.get('column_definitions', {})
    for col_name, col_def in column_defs.items():
        if 'tags' in col_def and 'override' in col_def['tags']:
            overrides[col_name] = col_def

    return overrides

def get_project_sql_files(project_name):
    """Get all SQL files in the project's core directory"""
    dbt_root = get_dbt_root()

    project_core_dir = os.path.join(dbt_root, 'target', 'compiled', 'artemis_dbt', 'models', 'projects', project_name, 'core')
    # project_core_dir = os.path.join(dbt_root, 'models', 'projects', project_name, 'core')
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

    # First pass: collect all needed columns
    needed_columns = set()
    for sql_file in sql_files:
        sql_columns = extract_sql_columns(sql_file)
        schema_columns = load_global_schema(global_schema_path)
        matching_columns, _ = compare_columns(sql_columns, schema_columns, existing_overrides.keys())
        needed_columns.update(matching_columns)

    if len(needed_columns) == 0:
        print(f"No matching columns found for project: {project_name}")
        return

    # Write the new schema file with auto-generated comment and anchors
    with open(output_path, 'w') as f:
        # Write header
        f.write("# This file is auto-generated from the global schema definitions.\n")
        f.write("# To override a column definition, add the 'override' tag to that column.\n\n")

        # Write version
        f.write("version: 2\n\n")

        # Write column definitions with anchors
        f.write("column_definitions:\n")
        for col_name in sorted(needed_columns):
            if col_name in existing_overrides:
                col_def = existing_overrides[col_name]
            elif col_name in column_defs:
                col_def = column_defs[col_name]
            else:
                continue

            f.write(f"  {col_name}: &{col_name}\n")
            f.write(f"    name: {col_def['name']}\n")
            f.write(f"    description: \"{col_def['description']}\"\n")
            if 'tests' in col_def:
                f.write("    tests:\n")
                for test in col_def['tests']:
                    f.write(f"      - {test}\n")
            if 'tags' in col_def:
                f.write("    tags:\n")
                for tag in col_def['tags']:
                    f.write(f"      - {tag}\n")
            f.write("\n")

        # Write models section
        f.write("models:\n")
        for sql_file in sql_files:
            model_name = os.path.basename(sql_file).replace('.sql', '')
            sql_columns = extract_sql_columns(sql_file)
            schema_columns = load_global_schema(global_schema_path)
            matching_columns, _ = compare_columns(sql_columns, schema_columns, existing_overrides.keys())

            if matching_columns:  # Only add model if it has matching columns
                f.write(f"  - name: {model_name}\n")
                f.write(f"    description: \"This table stores metrics for the {project_name.upper()} protocol\"\n")
                f.write("    columns:\n")
                for col_name in sorted(matching_columns):
                    f.write(f"      - *{col_name}\n")
                # Add tests block using abstracted test generation
                from generate_tests import generate_all_tests
                f.write(generate_all_tests(table_name=model_name))
                f.write("\n")

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

def get_project_path():
    """Generate standard paths relative to dbt project root"""
    dbt_root = get_dbt_root()

    paths = {
        'global_schema': os.path.join(dbt_root, 'models', '__global__schema.yml'),
    }
    return paths

def exec_main_script(project_name):
    try:
        # Get dbt root directory
        dbt_root = get_dbt_root()

        # Change to dbt root directory before running compile
        print(f"Changing to dbt root directory: {dbt_root}")
        original_dir = os.getcwd()
        os.chdir(dbt_root)

        print(f"Compiling models for project: {project_name}...")
        dbt_compile_command = ['dbt', 'compile', '-s', f'models/projects/{project_name}', '--target', 'prod']

        result = subprocess.run(dbt_compile_command, capture_output=True, text=True)

        # Change back to original directory
        os.chdir(original_dir)

        if result.returncode != 0:
            print("❌ dbt compile failed:")
            print(result.stdout)
            exit(1)
        print("✅ dbt compile successful")

    except FileNotFoundError:
        print("❌ dbt command not found. Make sure dbt is installed and in your PATH")
        exit(1)

    # Continue with rest of script...

    # Get paths
    paths = get_project_path()
    global_schema_path = paths['global_schema']

    # Get all SQL files in project
    sql_files = get_project_sql_files(project_name)
    if not sql_files:
        raise FileNotFoundError(f"No SQL files found in project {project_name}")

    # Generate combined schema file for all models
    generate_project_schema(project_name, global_schema_path, sql_files)



if __name__ == "__main__":
    # Get comma-separated input and split into list
    project_names = input("Enter project names, separated by commas: ").split(',')

    # Trim whitespace and run script
    for name in [n.strip() for n in project_names if n.strip()]:
        exec_main_script(name) 
