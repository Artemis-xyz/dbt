{{ config(materialized="incremental", unique_key="date", snowflake_warehouse='SEI') }}

{{get_contract_deployments("sei")}}