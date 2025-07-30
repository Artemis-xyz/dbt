{{
    config(
        materialized="table",
        database = 'RESERVE',
        schema = 'core',
        snowflake_warehouse = 'RESERVE',
        alias = 'ez_metrics_by_chain'
    )
}}

select
    reserve_metrics.date
    , 'reserve' as artemis_id
    , 'ethereum' as chain
    
    -- Standardized Metrics

    -- Usage Data
    , reserve_metrics.dau
    , reserve_metrics.tvl

    -- Financial Statements
    , reserve_metrics.revenue

    -- Bespoke Metrics
    , reserve_metrics.rtokens_mc

from {{ ref("ez_reserve_metrics") }} reserve_metrics