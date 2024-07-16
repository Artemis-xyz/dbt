{{
    config(
        materialized="table",
    )
}}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_avalanche_staked") }}
    ),
    prices as ({{ get_coingecko_price_with_latest("avalanche-2") }}),
    data as (
        select
            date(value:date) as date,
            value:"validator_stake"::float as validator_stake_native,
            value:"delegator_stake"::float as delegator_stake_native
        from
            {{ source("PROD_LANDING", "raw_avalanche_staked") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    ),
    map as (
        select
            date,
            validator_stake_native,
            delegator_stake_native,
            validator_stake_native + delegator_stake_native as total_staked_native
        from data
    )
select
    map.date,
    'avalanche' as chain,
    validator_stake_native,
    validator_stake_native * coalesce(price, 0) as validator_stake_usd,
    delegator_stake_native,
    delegator_stake_native * coalesce(price, 0) as delegator_stake_usd,
    total_staked_native,
    total_staked_native * coalesce(price, 0) as total_staked_usd
from map
left join prices on map.date = prices.date
where map.date < to_date(sysdate()) and map.date >= '2015-01-01' --sometimes the dashboard is borked
