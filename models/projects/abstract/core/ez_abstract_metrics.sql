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

    -- Chain Usage Metrics
    , txns as chain_txns
    , daa as chain_dau

    -- Cash Flow Metrics
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    , cost as l1_cash_flow
    , cost_native as l1_cash_flow_native
    , revenue as foundation_cash_flow
    , revenue_native as foundation_cash_flow_native
from {{ ref("fact_abstract_fundamental_metrics") }} as f
where f.date  < to_date(sysdate())
