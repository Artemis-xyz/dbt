{{ config(materialized="incremental", unique_key="date", snowflake_warehouse='BASE_MD') }}

{{get_contract_deployments("base")}}
