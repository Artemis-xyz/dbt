{{
    config(
        materialized="table",
        snowflake_warehouse="ABSTRACT",
        database="abstract",
        schema="core",
        alias="ez_metrics",
    )
}}

select
    f.date
    , txns
    , daa as dau
    , fees_native
    , fees
    , cost
    , cost_native
    , revenue
    , revenue_native

    -- Standardized Metrics

    -- Usage Metrics
    , txns as chain_txns
    , daa as chain_dau

    -- Cash Flow Metrics
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    , cost as cogs
    , cost_native as cogs_native
    , revenue as foundation_revenue
    , revenue_native as foundation_revenue_native
from {{ ref("fact_abstract_fundamental_metrics") }} as f
where f.date  < to_date(sysdate())
