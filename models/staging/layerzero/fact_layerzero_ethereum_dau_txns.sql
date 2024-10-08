{{ config(
    materialized= "incremental",
    snowflake_warehouse="LAYERZERO"
) }}   

{{ get_layerzero_dau_txns_for_chain('ethereum') }}