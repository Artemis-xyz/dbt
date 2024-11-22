{{
    config(
        materialized = 'table',
        database = 'ondo',
        schema = 'core',
        snowflake_warehouse = 'ONDO',
        alias = 'ez_metrics_by_product'
    )
}}

with
tvl as (
    select * from {{ ref('fact_ondo_tvl_by_product') }}
)
select
    tvl.date,
    tvl.issuer,
    tvl.symbol,
    tvl.net_rwa_supply_native_change,
    tvl.net_rwa_supply_usd_change,
    tvl.rwa_supply_native,
    tvl.rwa_supply_usd,
from tvl
where date < to_date(sysdate())
