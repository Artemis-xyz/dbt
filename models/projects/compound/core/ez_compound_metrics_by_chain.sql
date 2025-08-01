{{
    config(
        materialized="table",
        snowflake_warehouse="COMPOUND",
        database="compound",
        schema="core",
        alias="ez_metrics_by_chain",
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
    ),
    token_incentives as (
        select
            day as date,
            'ethereum' as chain,
            sum(total_usd_value) as token_incentives
        from {{ ref('fact_compound_token_incentives') }}
        group by 1, 2
    )
select
    compound_by_chain.date
    , 'compound' as artemis_id
    , compound_by_chain.chain

    -- Standardized metrics
    -- Usage Metrics
    , compound_by_chain.daily_borrows_usd as lending_loans
    , compound_by_chain.daily_supply_usd as lending_deposits

    -- Financial Metrics
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
from compound_by_chain
left join token_incentives
    on compound_by_chain.date = token_incentives.date
   and compound_by_chain.chain = token_incentives.chain
where compound_by_chain.date < to_date(sysdate())