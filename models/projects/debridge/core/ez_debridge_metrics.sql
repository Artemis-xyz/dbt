{{
    config(
        materialized="table",
        snowflake_warehouse="DEBRIDGE",
        database="debridge",
        schema="core",
        alias="ez_metrics",
    )
}}

with bridge_volume_fees as (
    select 
        date
        , bridge_volume
        , ecosystem_revenue
        , bridge_txns
        , bridge_txns as txns
        , ecosystem_revenue as fees
    from {{ ref("fact_debridge_fundamental_metrics") }}
)

, price_data as ({{ get_coingecko_metrics("debridge") }})

select
    bridge_volume_fees.date
    , ecosystem_revenue
    , txns
    , fees
    -- Standardized Metrics
    , bridge_volume
    , bridge_txns
    , ecosystem_revenue as gross_protocol_revenue
    , price_data.price as price
    , price_data.market_cap as market_cap
    , price_data.fdmc as fdmc
    , price_data.token_turnover_circulating as token_turnover_circulating
    , price_data.token_turnover_fdv as token_turnover_fdv
    , price_data.token_volume as token_volume
from bridge_volume_fees
left join price_data on bridge_volume_fees.date = price_data.date
where date < to_date(sysdate())
