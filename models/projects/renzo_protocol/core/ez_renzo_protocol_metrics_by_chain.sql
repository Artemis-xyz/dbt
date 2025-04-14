{{
    config(
        materialized="table",
        snowflake_warehouse="RENZO_PROTOCOL",
        database="renzo_protocol",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    restaked_eth_metrics_by_chain as (
        select
            date,
            chain,
            num_restaked_eth,
            amount_restaked_usd,
            num_restaked_eth_net_change,
            amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_ethereum_restaked_eth_count_with_usd_and_change') }}
        union all
        select
            date,
            chain,
            num_restaked_eth,
            amount_restaked_usd,
            num_restaked_eth_net_change,
            amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_arbitrum_restaked_eth_count_with_usd_and_change') }}
        union all
        select
            date,
            chain,
            num_restaked_eth,
            amount_restaked_usd,
            num_restaked_eth_net_change,
            amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_base_restaked_eth_count_with_usd_and_change') }}
        union all
        select
            date,
            chain,
            num_restaked_eth,
            amount_restaked_usd,
            num_restaked_eth_net_change,
            amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_blast_restaked_eth_count_with_usd_and_change') }}
        union all
        select
            date,
            chain,
            num_restaked_eth,
            amount_restaked_usd,
            num_restaked_eth_net_change,
            amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_bsc_restaked_eth_count_with_usd_and_change') }}
        union all
        select
            date,
            chain,
            num_restaked_eth,
            amount_restaked_usd,
            num_restaked_eth_net_change,
            amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_linea_restaked_eth_count_with_usd_and_change') }}
        union all
        select
            date,
            chain,
            num_restaked_eth,
            amount_restaked_usd,
            num_restaked_eth_net_change,
            amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_mode_restaked_eth_count_with_usd_and_change') }}
    ),
    date_spine as (
        select
            ds.date
        from {{ ref('dim_date_spine') }} ds
        where ds.date between (select min(date) from restaked_eth_metrics_by_chain) and to_date(sysdate())
    )
select
    date_spine.date,
    restaked_eth_metrics_by_chain.chain,
    'renzo_protocol' as app,
    'DeFi' as category,

    --Old metrics needed for compatibility
    restaked_eth_metrics_by_chain.num_restaked_eth,
    restaked_eth_metrics_by_chain.amount_restaked_usd,
    restaked_eth_metrics_by_chain.num_restaked_eth_net_change,
    restaked_eth_metrics_by_chain.amount_restaked_usd_net_change 

    --Standardized Metrics

    --Usage Metrics
    , restaked_eth_metrics_by_chain.num_restaked_eth as tvl_native
    , restaked_eth_metrics_by_chain.amount_restaked_usd as tvl
    , restaked_eth_metrics_by_chain.num_restaked_eth_net_change as tvl_native_net_change
    , restaked_eth_metrics_by_chain.amount_restaked_usd_net_change as tvl_net_change
from date_spine
left join restaked_eth_metrics_by_chain using(date)
where date_spine.date < to_date(sysdate())
