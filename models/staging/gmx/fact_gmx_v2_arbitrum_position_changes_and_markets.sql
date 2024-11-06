{{
    config(
        materialized = 'incremental',
        snowflake_warehouse = 'GMX'
    )
}}

{{ get_gmx_v2_trade_events_for_chain('arbitrum') }}