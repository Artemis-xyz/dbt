{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'BLUR',
        database = 'blur',
        schema = 'core',
        alias = 'ez_metrics_by_chain'
    )
}}

with
    blur_fees as (
        select *
        from {{ ref("fact_blur_fees") }}
    )
    , blur_daus as (
        select *
        from {{ ref("fact_blur_daus") }}
    )
    , blur_daily_txns as (
        select *
        from {{ ref("fact_blur_daily_txns") }}
    )

select
    blur_daus.date
    , blur_daus.chain
    , blur_daus.dau
    , blur_daily_txns.daily_txns as txns

    -- Standardized Metrics

    -- NFT Metrics
    , blur_daus.dau as nft_dau
    , blur_daily_txns.daily_txns as nft_txns
    , blur_fees.fees as nft_fees

    -- Cash Flow Metrics
    , blur_fees.fees as fees
    , blur_fees.fees as service_fee_allocation

from blur_fees
left join blur_daus using (date, chain)
left join blur_daily_txns using (date, chain)