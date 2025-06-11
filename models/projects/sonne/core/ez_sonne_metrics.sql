{{
    config(
        materialized="table",
        snowflake_warehouse="SONNE_FINANCE",
        database="sonne_finance",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    sonne_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_sonne_base_borrows_deposits_gold"),
                    ref("fact_sonne_optimism_borrows_deposits_gold"),
                ],
            )
        }}
    )
    , sonne_metrics as (
        select
            date
            , sum(daily_borrows_usd) as daily_borrows_usd
            , sum(daily_supply_usd) as daily_supply_usd
        from sonne_by_chain
        group by 1
    )
    , price_data as ({{ get_coingecko_metrics("sonne-finance") }})
select
    sonne_metrics.date
    , 'sonne' as app
    , 'DeFi' as category
    , sonne_metrics.daily_borrows_usd
    , sonne_metrics.daily_supply_usd
    -- Standardized metrics
    , sonne_metrics.daily_borrows_usd as lending_loans
    , sonne_metrics.daily_supply_usd as lending_deposits
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
from sonne_metrics
left join price_data
    on sonne_metrics.date = price_data.date
where sonne_metrics.date < to_date(sysdate())