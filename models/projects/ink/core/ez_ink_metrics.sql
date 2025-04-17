{{
    config(
        materialized="table",
        snowflake_warehouse="INK",
        database="ink",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    ink_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_ink_daily_dex_volumes") }}
    )
select
    date
    , txns
    , daa as dau
    , fees_native
    , fees
    , ink_dex_volumes.dex_volumes
    -- Standardized Metrics
    -- Chain Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    , ink_dex_volumes.dex_volumes AS chain_dex_volume
    -- Cashflow Metrics
    , fees AS gross_protocol_revenue
    , fees_native AS gross_protocol_revenue_native
from {{ ref("fact_ink_fundamental_metrics") }}
left join ink_dex_volumes using (date)
where date < to_date(sysdate())
