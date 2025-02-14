{{
    config(
        materialized="table",
        snowflake_warehouse="HIVEMAPPER",
        database="hivemapper",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

select
    date,
    'solana' as chain,
    fees,
    primary_supply_side_revenue,
    revenue,
    mints_native,
    burns_native,
    dau
from {{ ref('ez_hivemapper_metrics') }}