{{ config(materialized="table") }}

with main as (
    select
        tx_hash,
        block_timestamp,
        origin_from_address as swapper,
        symbol_in,
        symbol_out,
        token_in,
        token_out,
        greatest(symbol_in, symbol_out) || ' - ' || least(symbol_in, symbol_out) as token_pair,
        nvl(amount_in_usd, amount_out_usd) as amount_usd,
        'ethereum' as chain
    from {{ source('ETHEREUM_FLIPSIDE_DEFI', 'ez_dex_swaps') }}
    where
        amount_usd < 1e7
        and platform ilike 'dodo%'

    union all

    select
        tx_hash,
        block_timestamp,
        origin_from_address as swapper,
        symbol_in,
        symbol_out,
        token_in,
        token_out,
        greatest(symbol_in, symbol_out) || ' - ' || least(symbol_in, symbol_out) as token_pair,
        nvl(amount_in_usd, amount_out_usd) as amount_usd,
        'polygon' as chain
    from {{ source('POLYGON_FLIPSIDE_DEFI', 'ez_dex_swaps') }}
    where
        amount_usd < 1e7
        and platform ilike 'dodo%'

    union all

    select
        tx_hash,
        block_timestamp,
        origin_from_address as swapper,
        symbol_in,
        symbol_out,
        token_in,
        token_out,
        greatest(symbol_in, symbol_out) || ' - ' || least(symbol_in, symbol_out) as token_pair,
        nvl(amount_in_usd, amount_out_usd) as amount_usd,
        'arbitrum' as chain
    from {{ source('ARBITRUM_FLIPSIDE_DEFI', 'ez_dex_swaps') }}
    where
        amount_usd < 1e7
        and platform ilike 'dodo%'

    union all

    select
        tx_hash,
        block_timestamp,
        origin_from_address as swapper,
        symbol_in,
        symbol_out,
        token_in,
        token_out,
        greatest(symbol_in, symbol_out) || ' - ' || least(symbol_in, symbol_out) as token_pair,
        nvl(amount_in_usd, amount_out_usd) as amount_usd,
        'bsc' as chain
    from {{ source('BSC_FLIPSIDE_DEFI', 'ez_dex_swaps') }}
    where
        amount_usd < 1e7
        and platform ilike 'dodo%'
)
select * from main

