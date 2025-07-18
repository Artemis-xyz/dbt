{{ config(materialized="incremental", unique_key="date", snowflake_warehouse='OPTIMISM_MD') }}

{{get_contract_deployments("optimism")}}
