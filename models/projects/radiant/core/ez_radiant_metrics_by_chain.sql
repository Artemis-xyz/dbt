{{
    config(
        materialized="table",
        snowflake_warehouse="RADIANT",
        database="radiant",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    radiant_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_radiant_arbitrum_borrows_deposits_gold"),
                    ref("fact_radiant_bsc_borrows_deposits_gold"),
                    ref("fact_radiant_ethereum_borrows_deposits_gold"),
                ],
            )
        }}
    )
    , token_incentives as (
        select date, chain, amount_native, amount_usd from {{ ref("fact_radiant_token_incentives") }}
    )
select
    token_incentives.date
    , 'radiant' as app
    , 'DeFi' as category
    , token_incentives.chain
    , radiant_by_chain.daily_borrows_usd
    , radiant_by_chain.daily_supply_usd
    -- Standardized metrics
    , radiant_by_chain.daily_borrows_usd as lending_loans
    , radiant_by_chain.daily_supply_usd as lending_deposits

    -- Supply data
    , token_incentives.amount_native as gross_emissions_native
    , token_incentives.amount_usd as gross_emissions
from token_incentives
left join radiant_by_chain on token_incentives.date = radiant_by_chain.date
    and token_incentives.chain = radiant_by_chain.chain
where token_incentives.date < to_date(sysdate())