{{
    config(
        materialized="table",
        snowflake_warehouse="DIMO",
        database="dimo",
        schema="core",
        alias="ez_metrics",
    )
}}


with token_incentives as (
    select
        date,
        token_incentives_native,
        token_incentives
    from {{ ref("fact_dimo_token_incentives") }}
)
, airdrop as (
    select
        date,
        daily_airdrop_amount
    from {{ ref("fact_dimo_airdrop") }}
)
, market_metrics as (
    {{ get_coingecko_metrics("dimo") }}
)
select
    market_metrics.date,
    token_incentives.token_incentives_native,
    token_incentives.token_incentives,
    airdrop.daily_airdrop_amount as airdrop,
    airdrop.daily_airdrop_amount_native as airdrop_native,
    market_metrics.price,
    market_metrics.market_cap,
    market_metrics.fdmc,
    market_metrics.token_turnover_circulating,
    market_metrics.token_turnover_fdv,
    market_metrics.token_volume
from token_incentives
left join market_metrics using(date)
left join airdrop using(date)
