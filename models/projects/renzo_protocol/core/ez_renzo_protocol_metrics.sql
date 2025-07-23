{{
    config(
        materialized="table",
        snowflake_warehouse="RENZO_PROTOCOL",
        database="renzo_protocol",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    restaked_eth_metrics_by_chain as (
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_ethereum_restaked_eth_count_with_usd_and_change') }}
        group by 1
        union all
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_arbitrum_restaked_eth_count_with_usd_and_change') }}
        group by 1
        union all
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_base_restaked_eth_count_with_usd_and_change') }}
        group by 1
        union all
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_blast_restaked_eth_count_with_usd_and_change') }}
        group by 1
        union all
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_bsc_restaked_eth_count_with_usd_and_change') }}
        group by 1
        union all
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_linea_restaked_eth_count_with_usd_and_change') }}
        group by 1
        union all
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_mode_restaked_eth_count_with_usd_and_change') }}
        group by 1
    ),
    restaked_eth_metrics as (
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from restaked_eth_metrics_by_chain
        group by 1
    ),
    market_metrics as (
        {{get_coingecko_metrics('renzo')}}
    ),
    date_spine as (
        select
            ds.date
        from {{ ref('dim_date_spine') }} ds
        where ds.date between (select min(date) from restaked_eth_metrics) and to_date(sysdate())
    )
select
    date_spine.date,
    --Standardized Metrics

    --Market Metrics
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc

    --Usage Metrics
    , restaked_eth_metrics.num_restaked_eth as tvl_native
    , restaked_eth_metrics.num_restaked_eth as lrt_tvl_native
    , restaked_eth_metrics.amount_restaked_usd as tvl
    , restaked_eth_metrics.amount_restaked_usd as lrt_tvl
    , restaked_eth_metrics.num_restaked_eth_net_change as lrt_tvl_native_net_change
    , restaked_eth_metrics.amount_restaked_usd_net_change as lrt_tvl_net_change

    --Other Metrics
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , market_metrics.token_turnover_fdv as token_turnover_fdv
    
from date_spine
--left join restaked_eth_metrics_by_chain using(date)
left join restaked_eth_metrics using(date)
left join market_metrics using(date)
where date_spine.date < to_date(sysdate())
