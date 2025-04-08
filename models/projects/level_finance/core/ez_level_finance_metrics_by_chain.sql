{{
    config(
        materialized="table",
        snowflake_warehouse="LEVEL_FINANCE",
        database="level_finance",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_level_finance_trading_volume") }}
        where chain is not null
    ),
    unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_level_finance_unique_traders") }}
        where chain is not null
    )

select 
    date as date
    , 'level_finance' as app
    , 'DeFi' as category
    , chain
    , trading_volume
    , unique_traders
    -- standardize metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
from unique_traders_data
left join trading_volume_data using(date, chain)
where date > '2022-12-10' and date < to_date(sysdate())
