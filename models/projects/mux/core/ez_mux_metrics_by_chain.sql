{{
    config(
        materialized="table",
        snowflake_warehouse="MUX",
        database="mux",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with 
    mux_data as (
        select date, trading_volume, unique_traders, chain
        from {{ ref("fact_mux_trading_volume_unique_traders") }}
        where chain is not null
    )

select
    date
    , 'mux' as app
    , 'DeFi' as category
    , chain
    , trading_volume
    , unique_traders
    -- standardize metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
from mux_data
where date < to_date(sysdate())
