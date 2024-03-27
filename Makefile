install:
	pip3 install -r requirements.txt && dbt deps

dbt-compile:
	dbt compile

dbt-test:
	dbt test

dbt-perms:
	python dbt_scripts/grant_permissions.py