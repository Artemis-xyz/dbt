{{
    config(
        materialized = 'incremental',
        snowflake_warehouse = 'GMX',
        unique_key =  ['tx_hash', 'event_index']
    )
}}

{{ get_gmx_v2_trade_events_for_chain('avalanche') }}