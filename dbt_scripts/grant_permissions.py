import pandas as pd

ROLE_NAME = "READ_AND_WRITE_NEW_SCHEMA_ROLE"


def grant_usage_to_db(db_name, role_name):
    return f"""\nGRANT usage on database {db_name} to role {role_name};\n
    GRANT usage on all schemas in database {db_name} to role {role_name};
    GRANT select on all views in database {db_name} to role {role_name};
    GRANT select on all tables in database {db_name} to role {role_name};\n
    GRANT usage on future schemas in database {db_name} to role {role_name};
    GRANT select on future views in database {db_name} to role {role_name};
    GRANT select on future tables in database {db_name} to role {role_name};
    \n
    """


if __name__ == "__main__":
    asset_list = pd.read_csv("databases.csv")
    script = "use role accountadmin;\n"
    print("Generating script to grant permissions")

    for db in asset_list["database"].unique():
        script += grant_usage_to_db(db, ROLE_NAME)
    # write the script to a file
    with open("dbt_scripts/grant_permissions.sql", "w") as f:
        f.write(script)
    print("Script generated successfully")
