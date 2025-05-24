{{
    config(
        materialized="table",
        snowflake_warehouse="venus",
        database="venus",
        schema="core",
        alias="ez_metrics_by_chain",
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
    , token_incentives as (
        select
            date,
            'bsc' as chain,
            token_incentives as token_incentives
        from {{ref('fact_venus_token_incentives')}}
    )
select
    venus_by_chain.date
    , 'venus' as app
    , 'DeFi' as category
    , venus_by_chain.chain
    , venus_by_chain.daily_borrows_usd
    , venus_by_chain.daily_supply_usd
    -- Standardized metrics
    , venus_by_chain.daily_borrows_usd as lending_loans
    , venus_by_chain.daily_supply_usd as lending_deposits
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
from venus_by_chain
left join token_incentives
    on venus_by_chain.date = token_incentives.date
   and venus_by_chain.chain = token_incentives.chain
where venus_by_chain.date < to_date(sysdate())