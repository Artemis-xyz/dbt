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

pipcompile:
	uv pip compile --annotation-style=line requirements.in -o requirements.txt --no-strip-extras

generate_schema:
	python3 dbt_scripts/generate_schema.py

clean-dev-schema:
	python3 dbt_scripts/clean_dev_schema.py

build-models:
	@echo "Running dbt build for models: $(filter-out $@,$(MAKECMDGOALS))"
	@dbt build -s $(foreach model,$(filter-out $@,$(MAKECMDGOALS)),$(model)+) --exclude "*iceberg*"

# Make target to automatically compare changes in your feature branch with the current state of prod
compare_dev_schema_target:
	# Make manifests/ directory if it doesn't already exist
	mkdir -p manifests/

	# Download the prod manifest from github and move to manifests/
	wget https://artemis-xyz.github.io/dbt/manifest.json && mv manifest.json manifests/manifest.json

	# Compare prod manifest on feature branch with prod manifest on main
	dbt ls --select state:modified.body --state ./manifests/ --target prod

	# Clean up
	rm -rf manifests/
%:
	@:

