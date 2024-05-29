-- depends_on {{ ref("ez_starknet_transactions") }}
{{ config(materialized="table", snowflake_warehouse="STARKNET") }}
{{ get_fundamental_data_for_chain("starknet") }}
