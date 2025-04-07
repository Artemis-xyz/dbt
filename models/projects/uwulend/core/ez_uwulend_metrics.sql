{{
    config(
        materialized="table",
        snowflake_warehouse="UWULEND",
        database="uwulend",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    uwulend_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_uwu_lend_ethereum_borrows_deposits_gold"),
                ],
            )
        }}
    )
    , uwulend_metrics as (
        select
            date
            , sum(daily_borrows_usd) as daily_borrows_usd
            , sum(daily_supply_usd) as daily_supply_usd
        from uwulend_by_chain
        group by 1
    )
    , price_data as ({{ get_coingecko_metrics("uwu-lend") }})

select
    uwulend_metrics.date
    , 'uwulend' as app
    , 'DeFi' as category
    , uwulend_metrics.daily_borrows_usd
    , uwulend_metrics.daily_supply_usd
    -- Standardized metrics
    , uwulend_metrics.daily_borrows_usd as lending_loans
    , uwulend_metrics.daily_supply_usd as lending_deposits
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
from uwulend_metrics
left join price_data
    on uwulend_metrics.date = price_data.date
where uwulend_metrics.date < to_date(sysdate())