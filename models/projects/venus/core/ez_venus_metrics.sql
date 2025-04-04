{{
    config(
        materialized="table",
        snowflake_warehouse="venus",
        database="venus",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    venus_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_venus_v4_lending_bsc_gold"),
                ],
            )
        }}
    )
    , venus_metrics as (
        select
            date
            , sum(daily_borrows_usd) as daily_borrows_usd
            , sum(daily_supply_usd) as daily_supply_usd
        from venus_by_chain
        group by 1
    )
    , price_data as ({{ get_coingecko_metrics("venus") }})

select
    venus_metrics.date
    , 'venus' as app
    , 'DeFi' as category
    , venus_metrics.daily_borrows_usd
    , venus_metrics.daily_supply_usd
    -- Standardized metrics
    , venus_metrics.daily_borrows_usd as lending_loans
    , venus_metrics.daily_supply_usd as lending_deposits
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
from venus_metrics
left join price_data
    on venus_metrics.date = price_data.date
where venus_metrics.date < to_date(sysdate())