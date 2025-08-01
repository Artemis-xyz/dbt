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
    date
    , 'solana' as chain
    , 'hivemapper' as artemis_id

    -- Standardized Metrics
    -- Market Metrics
    , price
    , token_volume
    , market_cap
    , fdmc

    -- Usage Metrics
    , dau

    -- Fees Metrics
    , fees
    , service_fee_allocation
    , burned_fee_allocation

    -- Financial Metrics
    , revenue

    -- Timestamp Columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from {{ ref('ez_hivemapper_metrics') }}