{{
    config(
        materialized="table",
        snowflake_warehouse="SEAMLESSPROTOCOL",
        database="seamlessprotocol",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    seamless_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_seamless_protocol_base_borrows_deposits_gold"),
                ],
            )
        }}
    )
    , seamless_metrics as (
        select
            date
            , sum(daily_borrows_usd) as daily_borrows_usd
            , sum(daily_supply_usd) as daily_supply_usd
        from seamless_by_chain
        group by 1
    )

    , price_data as ({{ get_coingecko_metrics("seamless-protocol") }})

select
    seamless_metrics.date
    , 'seamless' as app
    , 'DeFi' as category
    , seamless_metrics.daily_borrows_usd
    , seamless_metrics.daily_supply_usd
    -- Standardized metrics
    , seamless_metrics.daily_borrows_usd as lending_loans
    , seamless_metrics.daily_supply_usd as lending_deposits
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
from seamless_metrics
left join price_data
    on seamless_metrics.date = price_data.date
where seamless_metrics.date < to_date(sysdate())