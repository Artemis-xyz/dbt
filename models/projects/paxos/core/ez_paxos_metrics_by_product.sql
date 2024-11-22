{{
    config(
        materialized = 'table',
        database = 'paxos',
        schema = 'core',
        snowflake_warehouse = 'PAXOS',
        alias = 'ez_metrics_by_product'
    )
}}

with
tvl as (
    select * from {{ ref('fact_paxos_tvl_by_product') }}
)
select
    tvl.date,
    tvl.issuer,
    tvl.symbol,
    tvl.tokenized_supply_change,
    tvl.tokenized_mcap_change,
    tvl.tokenized_supply,
    tvl.tokenized_mcap,
from tvl
where date < to_date(sysdate())
