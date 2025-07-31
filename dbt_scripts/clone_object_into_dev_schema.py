import os
import sys
import snowflake.connector
from dbt.cli.main import dbtRunner

def clone_object_into_dev_schema(dbt_model_names: list[str]):
    print(f"Cloning objects into dev schema for models: {dbt_model_names}")

    dbt_model_names = ' '.join(dbt_model_names)
    dbt = dbtRunner()

    # Run dbt compile to get the name of the relations for each model using the prod target
    cli_args = ["compile", "--select", dbt_model_names, "--target", "prod", "--threads", "4", "--no-print", "--quiet", "--no-debug"]

    dbt_invocation_result = dbt.invoke(cli_args)
    source_relation_names = []
    for r in dbt_invocation_result.result:
        # Get the relation from the run results 
        source_relation_names.append(r.to_dict()['node']['relation_name'].upper())

    target_database_name = "DEV"
    target_schema_name = f"DEV_{os.getenv('SYSTEM_SNOWFLAKE_USER').split('@')[0].replace('.', '_').upper()}"

    query = f"CREATE SCHEMA IF NOT EXISTS {target_database_name}.{target_schema_name};"
    dbt_model_names = dbt_model_names.split(' ')

    # Generate the query to clone the objects into the dev schemaq
    for i, source_relation_name in enumerate(source_relation_names):
        target_table_name = dbt_model_names[i].upper()
        query += f"""
CREATE OR REPLACE TRANSIENT TABLE {target_database_name}.{target_schema_name}.{target_table_name} CLONE {source_relation_name};
        """

    print(f"Cloning objects into dev schema {target_database_name}.{target_schema_name}...")
    with snowflake.connector.connect(
       account="iytnltj-mwb17343",
       user=os.environ.get("SYSTEM_SNOWFLAKE_USER"),
       password="",
       authenticator="externalbrowser",
       client_store_temporary_credential=True,
       consent_cache_id_token=True,
    ) as ctx: 
       try:
           with ctx.cursor().execute(query, num_statements=query.count(';')) as cs:
               cs.fetchall()
       except snowflake.connector.errors.ProgrammingError as e:
           print(e)
           print(e.errno)
       except Exception as e:
           print(e)
    print("Successfully cloned objects into dev schema!")

if __name__ == "__main__":
    dbt_model_names = sys.argv[1:]
    assert len(dbt_model_names) > 0, "No dbt model names provided!"

    clone_object_into_dev_schema(dbt_model_names)