{{
    config(
        materialized="table",
        snowflake_warehouse = 'MOVEMENT',
        database = 'MOVEMENT',
        schema = 'core',
        alias = 'ez_metrics'
    )
 }}

with movement_data as (
    select
        date
        , sum(dau) as dau
        , sum(txns) as txns
        , sum(gas_native) as gas_native
        , sum(gas) as gas
    from {{ ref("fact_movement_fundamental_data") }}
    group by 1
)
, prices as (
    {{ get_coingecko_metrics('movement') }}
)
, date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date between (select min(date) from movement_data) and (to_date(sysdate()))
)
select
    ds.date

    -- Usage Metrics
    , dau as chain_dau
    , txns as chain_txns

    -- Cash Flow Metrics
    , gas_native as chain_fees_native
    , gas as chain_fees
    , gas as ecosystem_revenue

    -- Market Metrics
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
    , token_volume
from date_spine ds
left join movement_data using (date)
left join prices using (date)
