{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="RAYDIUM",
    )
}}


with 
solana_tokens as (
    select 
    coingecko_id, solana_mint_address
    FROM (
        VALUES
            ('solana', 'So11111111111111111111111111111111111111112'), 
            ('usd-coin', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'), 
            ('tether', 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB')
    ) as t(coingecko_id, solana_mint_address)
),
latest_price as (
    select coingecko_id, date as date, shifted_token_price_usd as price
    from {{ ref("fact_coingecko_token_date_adjusted_gold") }}
    where 
        coingecko_id in ('solana', 'usd-coin', 'tether')
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select token_id as coingecko_id, dateadd('day', -1, to_date(sysdate())) as date, token_current_price as price
    from {{ ref("fact_coingecko_token_realtime_data") }}
    where token_id in ('solana', 'usd-coin', 'tether')
    union
    select token_id as coingecko_id, to_date(sysdate()) as date, token_current_price as price
    from {{ ref("fact_coingecko_token_realtime_data") }}
    where token_id in ('solana', 'usd-coin', 'tether')
)
select 
    date
    , cg.coingecko_id
    , price
    , solana_mint_address
from latest_price as cg
left join solana_tokens on cg.coingecko_id = solana_tokens.coingecko_id
