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
    tvl.tokenized_supply_change,
    tvl.tokenized_mcap_change,
    tvl.tokenized_supply,
    tvl.tokenized_mcap,
from tvl
where date < to_date(sysdate())
