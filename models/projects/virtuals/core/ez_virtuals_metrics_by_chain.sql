{{
    config(
        materialized="table",
        snowflake_warehouse = 'VIRTUALS',
        database = 'VIRTUALS',
        schema = 'core',
        alias = 'ez_metrics_by_chain'
    )
}}

select
    date,
    'base' as chain,
    daily_agents,
    dau,
    volume_native,
    volume_usd as trading_volume,
    fee_fun_native,
    fee_fun_usd,
    tax_usd,
    fees
from {{ ref('ez_virtuals_metrics') }}