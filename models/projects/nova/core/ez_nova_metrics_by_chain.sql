{{
    config(
        materialized="table",
        snowflake_warehouse="NOVA",
        database="nova",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with nova_dex_volumes as (
    select date, daily_volume as dex_volumes
    from {{ ref("fact_nova_daily_dex_volumes") }}
)

select
    date
    , 'solana' as chain
    , dex_volumes

    -- Standardized Metrics
    , dex_volumes as spot_volume


from nova_dex_volumes   
where date < to_date(sysdate())
