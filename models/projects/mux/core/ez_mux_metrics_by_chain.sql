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
    , token_incentives as (
        select
            date,
            'arbitrum' as chain,
            sum(token_incentives) as token_incentives
        from {{ ref("fact_mux_token_incentives") }}
        group by date, chain
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
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
from mux_data
left join token_incentives using(date, chain)
where date < to_date(sysdate())
