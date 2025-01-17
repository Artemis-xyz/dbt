{{ config(materialized="incremental", unique_key="date", snowflake_warehouse='ARBITRUM_MD') }}

{{get_contract_deployments("arbitrum")}}
