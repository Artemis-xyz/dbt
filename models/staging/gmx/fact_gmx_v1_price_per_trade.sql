{{
    config(
        materialized = 'incremental',
        unique_key = ['tx_hash', 'event_index']
    )
}}
with arbitrum_v1 as (
    {{ get_gmx_v1_price_per_trade_for_chain('arbitrum') }}
),
avalanche_v1 as (
    {{ get_gmx_v1_price_per_trade_for_chain('avalanche') }}
)
select * from arbitrum_v1
union all
select * from avalanche_v1  
