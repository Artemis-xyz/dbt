{{
    config(
        materialized="table",
        snowflake_warehouse="COMPOUND",
        database="compound",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    compound_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_compound_lending_arbitrum_gold"),
                    ref("fact_compound_lending_base_gold"),
                    ref("fact_compound_lending_ethereum_gold"),
                    ref("fact_compound_lending_polygon_gold"),
                ],
            )
        }}
    )
    , compound_metrics as (
        select
            date
            , sum(daily_borrows_usd) as daily_borrows_usd
            , sum(daily_supply_usd) as daily_supply_usd
        from compound_by_chain
        group by 1
    )
    , price_data as ({{ get_coingecko_metrics("compound-governance-token") }})

select
    compound_metrics.date
    , 'compound' as app
    , 'DeFi' as category
    -- Standardized metrics
    , compound_metrics.daily_borrows_usd as lending_loans
    , compound_metrics.daily_supply_usd as lending_deposits
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
from compound_metrics
left join price_data
    on compound_metrics.date = price_data.date
where compound_metrics.date < to_date(sysdate())