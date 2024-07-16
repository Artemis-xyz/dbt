{{
    config(
        materialized="table",
        snowflake_warehouse="TON",
        database="ton",
        schema="core",
        alias="ez_token_open_league",
    )
}}
WITH ton_coingecko_price_data as (
    SELECT 
        date,
        coingecko_id,
        shifted_token_price_usd as price
    FROM pc_dbt_db.prod.fact_coingecko_token_date_adjusted
    where coingecko_id in (select distinct coingecko_id from {{ source("SIGMA", "ton_token_coingecko_id")}})
    union 
    SELECT 
        dateadd('day', -1, date) as date,
        token_id as coingecko_id,
        token_current_price as price
    from pc_dbt_db.prod.fact_coingecko_token_realtime_data
    where token_id in (select distinct coingecko_id from {{ source("SIGMA", "ton_token_coingecko_id")}})
), collapsed_prices as (
    select date, coingecko_id, max(price) as price from ton_coingecko_price_data group by date, coingecko_id
), daily_price_changes as (
    select 
        date
        , coingecko_id
        , price
        , CASE 
                WHEN LAG(price) OVER (PARTITION BY coingecko_id ORDER BY date) = 0 THEN NULL
                ELSE (price - LAG(price) OVER (PARTITION BY coingecko_id ORDER BY date)) / LAG(price) OVER (PARTITION BY coingecko_id ORDER BY date) * 100
        END AS price_chg
    from collapsed_prices
), ton_token_data as (
    SELECT 
        date
        , unique_id
        , season
        , has_boost
        , boost_link
        , icon
        , is_meme
        , name
        , friendly_name
        , new_users_min_amount
        , price_change_normed
        , price_change_simple
        , score
        , token_address
        , token_last_tvl
        , token_price_after
        , token_price_before
        , token_start_tvl
        , token_tvl_change
        , url
        , coingecko_id
    from {{ ref("fact_ton_token_open_league") }}
    left join {{ source("SIGMA", "ton_token_coingecko_id")}} on name = ton_token
)
SELECT 
    ton_token_data.date
    , unique_id
    , season
    , has_boost
    , boost_link
    , icon
    , is_meme
    , name
    , friendly_name
    , new_users_min_amount
    , price_change_normed
    , coalesce(price_chg, price_change_simple) as price_change_simple
    , score
    , token_address
    , token_last_tvl
    , coalesce(price, token_price_after) as token_price_after
    , token_price_before
    , token_start_tvl
    , token_tvl_change
    , url
    , ton_token_data.coingecko_id
from ton_token_data
left join daily_price_changes on ton_token_data.date = daily_price_changes.date and ton_token_data.coingecko_id = daily_price_changes.coingecko_id
