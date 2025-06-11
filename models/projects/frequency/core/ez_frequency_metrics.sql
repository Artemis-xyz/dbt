{{
    config(
        materialized="table",
        snowflake_warehouse="FREQUENCY",
        database="frequency",
        schema="core",
        alias="ez_metrics",
    )
}}
with
    fundamental_data as (
        select
            date, 
            txns,
            daa, 
            fees_native
        from {{ ref("fact_frequency_fundamental_metrics") }}
    )
select
    date
    , txns
    , daa as dau
    , coalesce(fees_native, 0) as fees_native
    -- Standardized Metrics
    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    -- Cash Flow Metrics
    , fees_native as ecosystem_revenue_native
from fundamental_data
where fundamental_data.date < to_date(sysdate())
