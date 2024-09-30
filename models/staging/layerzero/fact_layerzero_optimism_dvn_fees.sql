{{ config(
    materialized= "table",
    snowflake_warehouse="LAYERZERO"
) }}

{{ get_layerzero_dvn_fees_for_chain('optimism') }}