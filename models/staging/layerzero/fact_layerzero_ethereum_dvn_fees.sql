{{ config(
    materialized= "incremental",
    snowflake_warehouse="LAYERZERO"
) }}   

{{ get_layerzero_dvn_fees_for_chain('ethereum') }}