{{
    config(
        materialized="table",
        snowflake_warehouse="TON",
        database="ton",
        schema="core",
        alias="ez_token_open_league",
    )
}}
SELECT 
    date
    , concat(
        coalesce(cast(date as string), '_this_is_null_'),
        '|',
        coalesce(cast(name as string), '_this_is_null_')
    ) as unique_id
    , season
    , has_boost
    , boost_link
    , icon
    , is_meme
    , name
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
from {{ ref("fact_ton_token_open_league") }}
