{{
    config(
        materialized="incremental",
        unique_key=["date", "token_address"],
        snowflake_warehouse="RAYDIUM",
    )
}}


with 
swap_pricing_data as (
    -- DEX Swaps where TO Swap Token is either SOL, USDC, or USDT
    select 
        date_trunc('day', block_timestamp) as date
        , swap_from_mint as token_address
        , swap_to_mint as priced_asset_address
        , swap_from_amount as token_amount
        , swap_to_amount as priced_asset_amount
        , swapper
        from solana_flipside.defi.fact_swaps
    where swap_to_mint in ('So11111111111111111111111111111111111111112', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB')
    and succeeded
    {% if is_incremental() %}
        AND block_timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
    {% else %}
        AND block_timestamp::date >= date('2022-04-22')
    {% endif %}
    union all 
    -- DEX Swaps where FROM Swap Token is either SOL, USDC, or USDT
    select 
        date_trunc('day', block_timestamp) as date
        , swap_to_mint as token_address
        , swap_from_mint as priced_asset_address
        , swap_to_amount as token_amount
        , swap_from_amount as priced_asset_amount
        , swapper
    from solana_flipside.defi.fact_swaps
    where swap_from_mint in ('So11111111111111111111111111111111111111112', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB')
    and succeeded
    {% if is_incremental() %}
        AND block_timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
    {% else %}
        AND block_timestamp::date >= date('2022-04-22')
    {% endif %}
),
priced_ratio as (
    SELECT 
        swap_pricing_data.date
        , token_address
        , swapper
        , (priced_asset_amount*price)/token_amount as token_price_usd
    FROM swap_pricing_data
    left join {{ ref("fact_raydium_token_prices") }} as token_prices
    on swap_pricing_data.date = token_prices.date and swap_pricing_data.priced_asset_address = token_prices.solana_mint_address
    where token_amount > 0
),
solana_dex_token_prices as (
    select
        date
        , token_address
        , avg(token_price_usd) as price
        , count(*) as number_of_swaps
        , count(distinct swapper) as unique_traders
    from priced_ratio
    group by 1, 2
    HAVING count(*) > 1000 and count(distinct swapper) > 50
)
select 
    solana_dex_token_prices.date
    , token_address
    , LEAST_IGNORE_NULLS(solana_dex_token_prices.price, token_prices.price) as price
    , number_of_swaps
    , unique_traders
from solana_dex_token_prices
left join {{ ref("fact_raydium_token_prices") }} as token_prices
on solana_dex_token_prices.date = token_prices.date and solana_dex_token_prices.token_address = token_prices.solana_mint_address
