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
    date
    , 'ethereum' as chain
    , dau
    -- Standardized Metrics

    -- Stablecoin Metrics
    , stablecoin_dau

    -- Crypto Metrics
    , tvl 
from {{ ref("ez_reserve_metrics") }}