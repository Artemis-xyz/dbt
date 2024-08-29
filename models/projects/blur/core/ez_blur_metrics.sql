{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'BLUR',
        database = 'blur',
        schema = 'core',
        alias = 'ez_metrics'
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
    blur_daus.date,
    blur_daus.dau,
    blur_daily_txns.daily_txns,
    blur_fees.fees
from blur_daus
left join blur_daily_txns
    on blur_daus.date = blur_daily_txns.date
left join blur_fees
    on blur_daus.date = blur_fees.date