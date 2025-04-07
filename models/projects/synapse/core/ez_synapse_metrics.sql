{{
    config(
        materialized="table",
        snowflake_warehouse="SYNAPSE",
        database="synapse",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    bridge_volume_metrics as (
        select date, bridge_volume
        from {{ ref("fact_synapse_bridge_volume") }}
        where chain is null
    ),
    bridge_daa_metrics as (
        select date, bridge_daa
        from {{ ref("fact_synapse_bridge_daa") }}
    )
    , price_data as ({{ get_coingecko_metrics("synapse-2") }})
select
    bridge_daa_metrics.date as date,
    'synapse' as app,
    'Bridge' as category,
    bridge_daa_metrics.bridge_daa
    -- Standardized Metrics
    , bridge_volume_metrics.bridge_volume
    , bridge_daa_metrics.bridge_daa as bridge_dau
    , price_data.price as price
    , price_data.market_cap as market_cap
    , price_data.fdmc as fdmc
    , price_data.token_turnover_circulating as token_turnover_circulating
    , price_data.token_turnover_fdv as token_turnover_fdv
    , price_data.token_volume as token_volume
from bridge_volume_metrics
left join bridge_daa_metrics on bridge_volume_metrics.date = bridge_daa_metrics.date
left join price_data on bridge_volume_metrics.date = price_data.date
where bridge_daa_metrics.date < to_date(sysdate())
