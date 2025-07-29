{{
    config(
        materialized = 'view',
        snowflake_warehouse = 'REF_FINANCE',
        unique_key = ['date'],
        alias = 'ez_metrics_by_chain',
        schema = 'core',
        database = 'REF_FINANCE'
    )
}}

select
    date
    , 'ref_finance' as artemis_id
    , 'near' as chain
    -- Standardized Metrics

    -- Market Metrics
    , price
    , market_cap
    , fdmc
    , token_volume

    -- Usage Metrics
    , spot_dau
    , spot_txns
    , spot_volume

    -- Other Metrics
    , token_turnover_circulating
    , token_turnover_fdv
from {{ref('ez_ref_finance_metrics')}}
where date < to_date(sysdate())
