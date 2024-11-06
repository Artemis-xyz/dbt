{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GMX'
    )
}}

with market_creation_txs as (
    select 
        created_tx_hash,
        e.market
    from {{ source('ARBITRUM_FLIPSIDE', 'dim_contracts') }} c
    join {{ref('fact_gmx_v2_arbitrum_position_changes_and_markets')}} e on e.market = c.address
)

, index_token_addresses as (
    select
        m.market,
        {{ hex_string_to_evm_address('substr(t.input, 35, 40)') }} as index_token_address
    from 
    {{ source('ARBITRUM_FLIPSIDE', 'fact_traces') }} t
    join market_creation_txs m on m.created_tx_hash = t.tx_hash
    and t.input like '0xa50ff3a6%'
    {% if is_incremental() %}
        and t.block_timestamp >= dateadd('day', -1, (select max(last_updated) from {{ this }}))
    {% endif %}
)

select
    sysdate() as last_updated,
    market,
    index_token_address
from index_token_addresses