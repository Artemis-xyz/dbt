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
    )
select
    restaked_eth_metrics_by_chain.date,
    'renzo_protocol' as protocol,
    'DeFi' as category,
    restaked_eth_metrics_by_chain.chain,
    restaked_eth_metrics_by_chain.num_restaked_eth,
    restaked_eth_metrics_by_chain.amount_restaked_usd,
    restaked_eth_metrics_by_chain.num_restaked_eth_net_change,
    restaked_eth_metrics_by_chain.amount_restaked_usd_net_change 
from restaked_eth_metrics_by_chain
where restaked_eth_metrics_by_chain.date < to_date(sysdate())
