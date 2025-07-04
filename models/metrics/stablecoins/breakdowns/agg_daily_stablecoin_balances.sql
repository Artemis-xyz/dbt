{{ config(materialized="table", snowflake_warehouse="STABLECOIN_V2_LG") }}

{% set chain_list = ['arbitrum', 'avalanche', 'base', 'bsc', 'celo', 'ethereum', 'mantle', 'optimism', 'polygon', 'solana', 'sui', 'ton', 'tron', 'sonic', 'kaia', 'aptos', 'ripple'] %}

with
    stablecoin_balances as (
        {% for chain in chain_list %}
        select 
            date
            , address
            , contract_address
            , symbol
            , stablecoin_supply_native
            , stablecoin_supply
            , chain
            , unique_id
        from {{ ref("fact_" ~ chain ~ "_stablecoin_balances") }}
        {% if is_incremental() %}
            where date >= (select DATEADD('day', -3, max(date)) from {{ this }})
        {% endif %}
        {% if not loop.last %} union all {% endif %}
        {% endfor %}
    )
select
    date
    , address
    , contract_address
    , symbol
    , stablecoin_supply_native
    , stablecoin_supply
    , chain
    , unique_id
from stablecoin_balances