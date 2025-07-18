{{ config(materialized="incremental", unique_key="date", snowflake_warehouse='ETHEREUM') }}

{{get_contract_deployments("ethereum")}}
