{{
    config(
        materialized="table",
        snowflake_warehouse="SONEIUM",
        database="soneium",
        schema="core",
        alias="ez_metrics",
    )
}}

select
    date
    , txns
    , daa as dau
    , fees_native
    , fees
    -- Standardized metrics
    -- Chain Usage Metrics
    , dau as chain_dau
    , txns as chain_txns
    -- Cashflow metrics
    , fees_native AS ecosystem_revenue_native
    , fees AS ecosystem_revenue
from {{ ref("fact_soneium_fundamental_metrics") }}
where date < to_date(sysdate())
