{{
    config(
        materialized="table",
        snowflake_warehouse="HOLDSTATION",
        database="holdstation",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


with
    zk_sync_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_holdstation_trading_volume") }}
    ),
    bera_trading_volume_data as (
        select date, perp_volume, chain
        from {{ ref("fact_holdstation_bera_perp_volume") }}
    ),
    agg_volume_data as (
        select date, chain, sum(trading_volume) as perp_volume
        from zk_sync_volume_data
        group by date, chain
        union all
        select date, chain, sum(perp_volume) as perp_volume
        from bera_trading_volume_data
        group by date, chain
    ),
    unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_holdstation_unique_traders") }}
    )
select
    date
    , 'holdstation' as artemis_id
    , chain
    -- standardize metrics
    , perp_volume
    , unique_traders as perp_dau
from agg_volume_data
left join unique_traders_data using(date, chain)
where date < to_date(sysdate())