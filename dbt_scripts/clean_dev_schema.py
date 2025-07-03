import os
import snowflake.connector

with snowflake.connector.connect(
    account="iytnltj-mwb17343",
    user=os.environ.get("SYSTEM_SNOWFLAKE_USER"),
    password="",
    authenticator="externalbrowser",
    client_store_temporary_credential=True,
    consent_cache_id_token=True,
) as ctx: 
    # Drop user's dev schema, add prompt to make sure user wants to do this
    user_input = input("Are you sure you want to drop your dev schema? All data will be lost! (y/n): ")
    if user_input != "y":
        print("Exiting...")
        exit()

    dev_schema_name = f"DEV_{os.environ.get('SYSTEM_SNOWFLAKE_USER').split('@')[0].replace('.', '_').upper()}"
    print(f"Dropping dev schema {dev_schema_name}...")

    query = f"""
        USE DATABASE DEV;
        DROP SCHEMA IF EXISTS {dev_schema_name};
    """
    try:
        with ctx.cursor().execute(query, num_statements=query.count(';')) as cs:
            result = cs.fetchall()
            print(f"Successfully dropped dev schema {dev_schema_name}!")
    except snowflake.connector.errors.ProgrammingError as e:
        print(e)
        print(e.errno)
    except Exception as e:
        print(e)

