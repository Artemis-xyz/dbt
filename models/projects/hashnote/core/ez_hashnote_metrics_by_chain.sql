{{
    config(
        materialized = 'table',
        database = 'hashnote',
        schema = 'core',
        snowflake_warehouse = 'HASHNOTE',
        alias = 'ez_metrics_by_chain'
    )
}}

with
tvl as (
    select * from {{ ref('fact_hashnote_tvl_by_chain') }}
)
select
    tvl.date,
    tvl.issuer,
    tvl.chain,
    tvl.symbol,
    tvl.net_rwa_supply_native_change,
    tvl.net_rwa_supply_usd_change,
    tvl.rwa_supply_native,
    tvl.rwa_supply_usd,
from tvl
where date < to_date(sysdate())
