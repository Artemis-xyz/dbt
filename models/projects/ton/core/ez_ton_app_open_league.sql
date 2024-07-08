{{
    config(
        materialized="table",
        snowflake_warehouse="TON",
        database="ton",
        schema="core",
        alias="ez_app_open_league",
    )
}}
SELECT 
    date
    , unique_id
    , season
    , icon
    , name
    , offchain_avg_dau
    , offchain_non_premium_users
    , offchain_premium_users
    , offchain_stickiness
    , offchain_total_unique_users
    , onchain_median_tx
    , onchain_total_tx
    , onchain_uaw
    , score
    , url
    , end_date
from {{ ref("fact_ton_app_open_league") }}
