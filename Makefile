install:
	source syncenv && uv pip install -r requirements.txt && dbt deps

# As of 4/2/2024, compile and test will be run via Github cloud agent.
# Running locally without creds is not possible in Snowflake, but we are actively working on a solution.
dbt-compile:
	dbt compile

dbt-test:
	dbt test

dbt-perms:
	python dbt_scripts/grant_permissions.py