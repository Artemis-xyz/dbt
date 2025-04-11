{{
    config(
        materialized="table",
        snowflake_warehouse="FRAX",
        database="frax",
        schema="core",
        alias="ez_metrics",
    )
}}

with dex_data as (
    SELECT
        block_timestamp::date as date,
        count(distinct sender) as spot_dau,
        count(*) as spot_txns,
        sum(trading_volume) as spot_volume,
        sum(gas_cost_native) as gas_cost_native,
        sum(trading_fees) as spot_fees
    FROM
        {{ ref("ez_frax_dex_swaps") }}
    GROUP BY
        1
)
, staked_eth_metrics as (
    select
        date,
        num_staked_eth,
        amount_staked_usd,
        num_staked_eth_net_change,
        amount_staked_usd_net_change
    from {{ ref('fact_frax_staked_eth_count_with_USD_and_change') }}
)
, tvl_data as (
    SELECT
        date,
        sum(tvl) as tvl
    FROM
        {{ ref("fact_fraxswap_ethereum_tvl_by_pool") }}
    GROUP BY
        date
)
, market_data as (
    {{ get_coingecko_metrics('frax-share')}}
)

SELECT
    market_data.date

    -- Standardized Metrics

    -- Price Metrics
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage/Sector Metrics
    , dex_data.spot_dau
    , dex_data.spot_txns
    , dex_data.spot_volume
    , dex_data.spot_fees
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.num_staked_eth_net_change as lst_tvl_native_net_change
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change
    , tvl_data.tvl as spot_tvl
    

    -- Other Metrics
    , dex_data.gas_cost_native
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv
from market_data
left join dex_data using (date)
left join staked_eth_metrics using (date)
left join tvl_data using (date)
where market_data.date < to_date(sysdate())
