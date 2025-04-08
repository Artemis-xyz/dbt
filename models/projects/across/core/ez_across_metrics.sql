{{
    config(
        materialized="table",
        snowflake_warehouse="ACROSS",
        database="across",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    bridge_volume as (
        select date, bridge_volume
        from {{ ref("fact_across_bridge_volume") }}
        where chain is null
    ),
    bridge_daa as (
        select date, bridge_daa
        from {{ ref("fact_across_bridge_daa") }}
    )
    , price_data as ({{ get_coingecko_metrics("across") }})
select
    bridge_volume.date as date
    , 'across' as app
    , 'Bridge' as category
    , bridge_daa.bridge_daa
    -- Standardized Metrics
    , bridge_volume.bridge_volume as bridge_volume
    , bridge_daa.bridge_daa as bridge_dau
    , price_data.price as price
    , price_data.market_cap as market_cap
    , price_data.fdmc as fdmc
    , price_data.token_turnover_circulating as token_turnover_circulating
    , price_data.token_turnover_fdv as token_turnover_fdv
    , price_data.token_volume as token_volume

from bridge_volume
left join bridge_daa on bridge_volume.date = bridge_daa.date
left join price_data on bridge_volume.date = price_data.date
where bridge_volume.date < to_date(sysdate())
