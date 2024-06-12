{{
    config(
        materialized="table",
        snowflake_warehouse="PERPETUAL_PROTOCOL",
        database="perpetual_protocol",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_perpetual_protocol_trading_volume") }}
        where chain is not null
    ),
    unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_perpetual_protocol_unique_traders") }}
        where chain is not null
    ),
    fees_data as (
        select date, fees, chain
        from {{ ref("fact_perpetual_protocol_fees") }}
        where chain is not null
    )
select
    date as date,
    'perpetual_protocol' as app,
    'DeFi' as category,
    chain,
    trading_volume,
    unique_traders,
    fees
from unique_traders_data
left join trading_volume_data using(date, chain)
left join fees_data using(date, chain)
where date > '2021-11-25' and date < to_date(sysdate())