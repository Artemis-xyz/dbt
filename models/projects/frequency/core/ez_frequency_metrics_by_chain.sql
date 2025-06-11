{{
    config(
        materialized="table",
        snowflake_warehouse="FREQUENCY",
        database="frequency",
        schema="core",
        alias="ez_metrics_by_chain",
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
    , 'frequency' as chain
    -- Standardized Metrics
    -- Chain Metrics
    , txns as chain_txns
    , daa as chain_dau
    -- Cash Flow Metrics
    , coalesce(fees_native, 0) as ecosystem_revenue_native
from fundamental_data
where fundamental_data.date < to_date(sysdate())
