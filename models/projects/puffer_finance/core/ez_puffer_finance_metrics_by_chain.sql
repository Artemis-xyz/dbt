{{
    config(
        materialized="table",
        snowflake_warehouse="PUFFER_FINANCE",
        database="puffer_finance",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    restaked_eth_metrics as (
        select
            date,
            chain,
            num_restaked_eth,
            amount_restaked_usd,
            num_restaked_eth_net_change,
            amount_restaked_usd_net_change
        from {{ ref('fact_puffer_finance_restaked_eth_count_with_usd_and_change') }}
    )
select
    restaked_eth_metrics.date,
    'puffer_finance' as app,
    'DeFi' as category,
    restaked_eth_metrics.chain,
    restaked_eth_metrics.num_restaked_eth,
    restaked_eth_metrics.amount_restaked_usd,
    restaked_eth_metrics.num_restaked_eth_net_change,
    restaked_eth_metrics.amount_restaked_usd_net_change
from restaked_eth_metrics
where restaked_eth_metrics.date < to_date(sysdate())
