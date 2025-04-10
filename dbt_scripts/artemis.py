import snowflake.connector
import os
import argparse
import duckdb
import pandas as pd
import subprocess
import warnings

warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable.*')



def get_snowflake_connection():    
    conn = snowflake.connector.connect(
        account="iytnltj-mwb17343",
        user=os.environ.get("SYSTEM_SNOWFLAKE_USER"),
        password="",
        authenticator="externalbrowser",
        client_store_temporary_credential=True,
        consent_cache_id_token=True,
    )
    return conn

def execute_sql(sql, print_results=True, extract=None):
    conn = get_snowflake_connection()    
    try:
        query = sql
        df = pd.read_sql(query, conn)

        if extract:
            df = df[extract.upper()]
            for value in df:
                print(value)

        if print_results:
            print(duckdb.query("SELECT * FROM df").show())
    except Exception as e:
        print(f"Error executing SQL: {e}")
    finally:
        conn.close()

def get_dbt_root():

    current_dir = os.getcwd()
    while current_dir != '/':
        if os.path.exists(os.path.join(current_dir, 'dbt_project.yml')):
            return current_dir
        current_dir = os.path.dirname(current_dir)
    raise Exception("Could not find dbt project root (no dbt_project.yml found)")

def main():
    parser = argparse.ArgumentParser(description='Run SQL in Snowflake')
    parser.add_argument('--sql', help='SQL query to execute')
    parser.add_argument('--file', help='SQL file to execute')
    parser.add_argument('--no-print', action='store_true', help='Do not print results')
    parser.add_argument('--compile', help='Run dbt command')
    parser.add_argument('--extract', help='A column to store to print')
    parser.add_argument('--build', help='Build and run model')
    args = parser.parse_args()
    
    if args.sql:
        sql = args.sql
    elif args.file:
        with open(args.file, 'r') as f:
            sql = f.read()
    elif args.compile:
        dbt_root = get_dbt_root()
        original_dir = os.getcwd()
        os.chdir(dbt_root)
        print(f"Compiling {args.compile.split('/')[-1]}...")
        dbt_compile_command = ['dbt', 'compile', '-s', args.compile, '--quiet']
        result = subprocess.run(dbt_compile_command, capture_output=True, text=True)
        os.chdir(original_dir)
            
        if result.returncode != 0:
            print("❌ dbt compile failed:")
            print(result.stderr)
            exit(1)
        print("✅ dbt compile successful")

        sql = result.stdout

    elif args.build:
        dbt_root = get_dbt_root()
        original_dir = os.getcwd()
        os.chdir(dbt_root)
        print(f"Building {args.build.split('/')[-1]}...")
        dbt_build_command = ['dbt', 'build', '-s', args.build]
        result = subprocess.run(dbt_build_command, capture_output=True, text=True)
        os.chdir(original_dir)

        if result.returncode != 0:
            print("❌ dbt build failed:")
            print(result.stderr)
            exit(1)
        print(result.stdout)
        sql = 'select * from aave.prod_core.ez_metrics'
    else:
        print("Please provide either --sql, --dbt, or --file argument")
        exit(1)    
    execute_sql(sql, not args.no_print, args.extract)

if __name__ == "__main__":
    main()