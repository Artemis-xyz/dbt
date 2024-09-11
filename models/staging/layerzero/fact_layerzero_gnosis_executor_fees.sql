{{ config(
    materialized= "table",
    snowflake_warehouse="LAYERZERO"
) }}

{{ get_layerzero_executor_fees_for_chain('gnosis') }}
